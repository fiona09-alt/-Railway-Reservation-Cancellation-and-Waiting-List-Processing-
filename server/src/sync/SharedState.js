import { Mutex } from './Mutex.js';
import { ConditionVariable } from './ConditionVariable.js';
import { SIM_STATE } from '../models/enums.js';

/**
 * SharedState — the single SharedArrayBuffer that every OS thread
 * (main thread, Producer-1..N, Consumer-1..N) maps into its own address space.
 *
 * Layout (all little-endian Int32 words):
 *   [ HEADER: control words, lock words, condition sequences, statistics ]
 *   [ SEAT TABLE   : MAX_SEATS  * SEAT_INTS  words ]
 *   [ WAITING LIST : MAX_WAITLIST * WL_INTS   words ]
 *   [ BUFFER SLOTS : MAX_BUFFER * SLOT_INTS  words ]
 *
 * Two independent locks live in the header:
 *   - BUF_MUTEX  guards Producer <-> Buffer <-> Consumer (the bounded buffer)
 *   - RES_MUTEX  guards Consumer <-> Seat table <-> Waiting list
 */

export const LIMITS = { MAX_BUFFER: 64, MAX_SEATS: 48, MAX_WAITLIST: 100 };
export const NAME_BYTES = 24;
export const SLOT_INTS = 16;
export const SEAT_INTS = 16;
export const WL_INTS = 16;
export const MAGIC = 0x52574c59; // 'RWLY'

const HEADER_KEYS = [
  'MAGIC', 'VERSION',
  'BUF_CAP', 'BUF_COUNT', 'BUF_HEAD', 'BUF_TAIL',
  'BUF_MUTEX', 'NE_SEQ', 'NF_SEQ', 'NE_WAITERS', 'NF_WAITERS',
  'RES_MUTEX', 'RES_WAITERS',
  'SIM_ST', 'PAUSE_PROD', 'PAUSE_CONS', 'STOP', 'UNSAFE',
  'REQ_SEQ', 'PASSENGER_SEQ', 'SERIAL_SEQ',
  'SEAT_COUNT', 'WL_HEAD', 'WL_COUNT', 'WL_MAX',
  'ST_TOTAL', 'ST_BOOK_GEN', 'ST_CANCEL_GEN', 'ST_PROCESSED',
  'ST_CONFIRMED', 'ST_FAILED',
  'ST_BUF_WAIT_P', 'ST_BUF_WAIT_C', 'ST_LOCK', 'ST_RACE',
  'ST_PROC_MS', 'ST_PROC_N',
  'ST_PROMOTED', 'ST_CANCEL_OK', 'ST_ERRORS', 'ST_WL_FULL', 'ST_DUP',
];
export const H = {};
HEADER_KEYS.forEach((k, i) => { H[k] = i; });
export const HEADER_INTS = HEADER_KEYS.length;

export const SEATS_OFF = HEADER_INTS;
export const WL_OFF = SEATS_OFF + LIMITS.MAX_SEATS * SEAT_INTS;
export const BUF_OFF = WL_OFF + LIMITS.MAX_WAITLIST * WL_INTS;
export const TOTAL_INTS = BUF_OFF + LIMITS.MAX_BUFFER * SLOT_INTS;

// Field maps inside fixed-width records (word indices relative to record base)
export const SLOT = { REQ: 0, TYPE: 1, PASSENGER: 2, SEAT: 3, PRODUCER: 4, STATUS: 5, CREATED: 6, TRAIN: 7, AGE: 8, GENDER: 9, NAME: 10 };
export const SEATF = { STATUS: 0, PASSENGER: 1, REQUEST: 2, AGE: 3, GENDER: 4, TRAIN: 5, BOOKED_AT: 6, SERIAL: 7, NAME: 8 };
export const WLF = { PASSENGER: 0, REQUEST: 1, TIME: 2, TARGET: 3, PRODUCER: 4, SEQ: 5, NAME: 6, AGE: 12, GENDER: 13 };

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export class SharedState {
  /** Allocate and initialize a brand new shared region. Main thread only. */
  static create(config) {
    const sab = new SharedArrayBuffer(TOTAL_INTS * 4);
    const state = new SharedState(sab);
    state.initialize(config);
    return state;
  }

  /** Attach to an existing shared region. Callable from any thread. */
  static wrap(sab) {
    return new SharedState(sab);
  }

  constructor(sab) {
    this.sab = sab;
    this.i32 = new Int32Array(sab);
    this.u8 = new Uint8Array(sab);
    this.bufMutex = new Mutex(this.i32, H.BUF_MUTEX, 'buffer');
    this.notEmpty = new ConditionVariable(this.i32, H.NE_SEQ, H.NE_WAITERS, 'notEmpty');
    this.notFull = new ConditionVariable(this.i32, H.NF_SEQ, H.NF_WAITERS, 'notFull');
    this.resMutex = new Mutex(this.i32, H.RES_MUTEX, 'reservation');
  }

  initialize(config) {
    const a = this.i32;
    for (let i = 0; i < TOTAL_INTS; i++) a[i] = 0;
    Atomics.store(a, H.MAGIC, MAGIC);
    Atomics.store(a, H.VERSION, 1);
    this.setCapacity(config.capacity);
    Atomics.store(a, H.SEAT_COUNT, config.seats);
    Atomics.store(a, H.WL_MAX, config.waitlistMax);
    Atomics.store(a, H.SIM_ST, SIM_STATE.STOPPED);
  }

  // -- flags ----------------------------------------------------------------
  simState() { return Atomics.load(this.i32, H.SIM_ST); }
  isStoppedFlag() { return Atomics.load(this.i32, H.STOP) === 1; }
  isPaused() { return Atomics.load(this.i32, H.SIM_ST) === SIM_STATE.PAUSED; }
  producersPaused() { return Atomics.load(this.i32, H.PAUSE_PROD) === 1; }
  consumersPaused() { return Atomics.load(this.i32, H.PAUSE_CONS) === 1; }
  unsafeMode() { return Atomics.load(this.i32, H.UNSAFE) === 1; }

  /** Store + notify: how the main thread signals workers (start/pause/resume/stop). */
  setFlag(index, value) {
    Atomics.store(this.i32, index, value);
    Atomics.notify(this.i32, index, 4096);
  }

  setSimState(value) { this.setFlag(H.SIM_ST, value); }
  setStop(value) { this.setFlag(H.STOP, value ? 1 : 0); }
  setPauseProducers(value) { this.setFlag(H.PAUSE_PROD, value ? 1 : 0); }
  setPauseConsumers(value) { this.setFlag(H.PAUSE_CONS, value ? 1 : 0); }
  setUnsafe(value) { this.setFlag(H.UNSAFE, value ? 1 : 0); }

  /** Wake every thread blocked on buffer/control conditions (resume/stop/reset). */
  wakeAll() {
    this.notEmpty.notifyAll();
    this.notFull.notifyAll();
    Atomics.notify(this.i32, H.SIM_ST, 4096);
    Atomics.notify(this.i32, H.PAUSE_PROD, 4096);
    Atomics.notify(this.i32, H.PAUSE_CONS, 4096);
  }

  capacity() { return Atomics.load(this.i32, H.BUF_CAP); }
  setCapacity(n) { Atomics.store(this.i32, H.BUF_CAP, n); }
  count() { return Atomics.load(this.i32, H.BUF_COUNT); }
  seatCount() { return Atomics.load(this.i32, H.SEAT_COUNT); }
  waitlistMax() { return Atomics.load(this.i32, H.WL_MAX); }

  // -- statistics (thread-safe counters: Atomics.add) --------------------------
  addStat(key, n = 1) { return Atomics.add(this.i32, H[key], n); }
  stat(key) { return Atomics.load(this.i32, H[key]); }
  nextReqNum() { return Atomics.add(this.i32, H.REQ_SEQ, 1) + 1; }
  nextPassengerId() { return Atomics.add(this.i32, H.PASSENGER_SEQ, 1) + 1; }

  // -------------------------------------------------------------------------
  // BOUNDED BUFFER SLOTS (guarded by bufMutex)
  // -------------------------------------------------------------------------
  slotOff(slotIndex) { return BUF_OFF + slotIndex * SLOT_INTS; }

  /** Write a request record into a raw buffer slot (caller must hold BUF_MUTEX). */
  writeSlot(slotIndex, r) {
    const o = this.slotOff(slotIndex);
    const a = this.i32;
    Atomics.store(a, o + SLOT.REQ, r.reqNum);
    Atomics.store(a, o + SLOT.TYPE, r.type);
    Atomics.store(a, o + SLOT.PASSENGER, r.passengerId);
    Atomics.store(a, o + SLOT.SEAT, r.seat);
    Atomics.store(a, o + SLOT.PRODUCER, r.producer);
    Atomics.store(a, o + SLOT.STATUS, r.status);
    // created/train/age squeeze through a 32-bit word: keep low 31 bits.
    Atomics.store(a, o + SLOT.CREATED, (r.created | 0) & 0x7fffffff);
    Atomics.store(a, o + SLOT.TRAIN, r.trainNo);
    Atomics.store(a, o + SLOT.AGE, r.age);
    Atomics.store(a, o + SLOT.GENDER, r.gender);
    this.writeNameAt(o + SLOT.NAME, r.passengerName);
  }

  /** Read a raw buffer slot back into a plain object. */
  readSlot(slotIndex) {
    const o = this.slotOff(slotIndex);
    const a = this.i32;
    return {
      reqNum: a[o + SLOT.REQ],
      type: a[o + SLOT.TYPE],
      passengerId: a[o + SLOT.PASSENGER],
      seat: a[o + SLOT.SEAT],
      producer: a[o + SLOT.PRODUCER],
      status: a[o + SLOT.STATUS],
      created: a[o + SLOT.CREATED],
      trainNo: a[o + SLOT.TRAIN],
      age: a[o + SLOT.AGE],
      gender: a[o + SLOT.GENDER],
      passengerName: this.readNameAt(o + SLOT.NAME),
    };
  }

  clearSlot(slotIndex) {
    const o = this.slotOff(slotIndex);
    for (let i = 0; i < SLOT_INTS; i++) Atomics.store(this.i32, o + i, 0);
  }

  // -------------------------------------------------------------------------
  // SEAT TABLE (guarded by resMutex)
  // -------------------------------------------------------------------------
  seatOff(i) { return SEATS_OFF + i * SEAT_INTS; }

  readSeat(i) {
    const o = this.seatOff(i);
    const a = this.i32;
    return {
      number: i + 1,
      status: a[o + SEATF.STATUS],
      passengerId: a[o + SEATF.PASSENGER],
      requestId: a[o + SEATF.REQUEST],
      age: a[o + SEATF.AGE],
      gender: a[o + SEATF.GENDER],
      trainNo: a[o + SEATF.TRAIN],
      bookedAt: a[o + SEATF.BOOKED_AT],
      serial: a[o + SEATF.SERIAL],
      passengerName: this.readNameAt(o + SEATF.NAME),
    };
  }

  /** Book a seat. MUST be called inside the reservation mutex. */
  seatBook(i, { passengerId, requestId, age, gender, trainNo, name, now }) {
    const o = this.seatOff(i);
    const a = this.i32;
    Atomics.store(a, o + SEATF.STATUS, 1); // BOOKED
    Atomics.store(a, o + SEATF.PASSENGER, passengerId);
    Atomics.store(a, o + SEATF.REQUEST, requestId);
    Atomics.store(a, o + SEATF.AGE, age);
    Atomics.store(a, o + SEATF.GENDER, gender);
    Atomics.store(a, o + SEATF.TRAIN, trainNo);
    Atomics.store(a, o + SEATF.BOOKED_AT, now);
    Atomics.store(a, o + SEATF.SERIAL, Atomics.add(a, H.SERIAL_SEQ, 1) + 1);
    this.writeNameAt(o + SEATF.NAME, name);
  }

  /** Release a seat back to AVAILABLE. MUST be called inside the reservation mutex. */
  seatRelease(i) {
    const o = this.seatOff(i);
    for (let f = 0; f < SEATF.NAME + NAME_BYTES / 4; f++) {
      Atomics.store(this.i32, o + f, 0);
    }
  }

  seatStatus(i) { return Atomics.load(this.i32, this.seatOff(i) + SEATF.STATUS); }
  seatOccupant(i) { return Atomics.load(this.i32, this.seatOff(i) + SEATF.PASSENGER); }

  /** Find 0-based seat index booked by a passenger, or -1. Linear scan — <=48 seats. */
  seatOfPassenger(passengerId) {
    const n = this.seatCount();
    for (let i = 0; i < n; i++) {
      if (this.seatStatus(i) === 1 && this.seatOccupant(i) === passengerId) return i;
    }
    return -1;
  }

  // -------------------------------------------------------------------------
  // WAITING LIST — FIFO ring buffer (guarded by resMutex)
  // -------------------------------------------------------------------------
  wlOff(physical) { return WL_OFF + physical * WL_INTS; }
  wlCount() { return Atomics.load(this.i32, H.WL_COUNT); }

  /** k is logical position: 0 = front (first come), count-1 = back. */
  wlAt(k) {
    const head = Atomics.load(this.i32, H.WL_HEAD);
    const count = Atomics.load(this.i32, H.WL_COUNT);
    if (k < 0 || k >= count) return null;
    const physical = (head + k) % LIMITS.MAX_WAITLIST;
    const o = this.wlOff(physical);
    const a = this.i32;
    return {
      position: k + 1,
      passengerId: a[o + WLF.PASSENGER],
      requestId: a[o + WLF.REQUEST],
      time: a[o + WLF.TIME],
      targetSeat: a[o + WLF.TARGET],
      producer: a[o + WLF.PRODUCER],
      seq: a[o + WLF.SEQ],
      age: a[o + WLF.AGE],
      gender: a[o + WLF.GENDER],
      passengerName: this.readNameAt(o + WLF.NAME),
    };
  }

  wlList() {
    const out = [];
    for (let k = 0; k < this.wlCount(); k++) out.push(this.wlAt(k));
    return out;
  }

  /** Append at the back. Caller holds reservation mutex. Returns false when full. */
  wlPush(entry) {
    if (this.wlCount() >= this.waitlistMax()) return false;
    const head = Atomics.load(this.i32, H.WL_HEAD);
    const count = Atomics.load(this.i32, H.WL_COUNT);
    const physical = (head + count) % LIMITS.MAX_WAITLIST;
    const o = this.wlOff(physical);
    const a = this.i32;
    Atomics.store(a, o + WLF.PASSENGER, entry.passengerId);
    Atomics.store(a, o + WLF.REQUEST, entry.requestId);
    Atomics.store(a, o + WLF.TIME, entry.time);
    Atomics.store(a, o + WLF.TARGET, entry.targetSeat);
    Atomics.store(a, o + WLF.PRODUCER, entry.producer);
    Atomics.store(a, o + WLF.SEQ, entry.seq);
    Atomics.store(a, o + WLF.AGE, entry.age ?? 0);
    Atomics.store(a, o + WLF.GENDER, entry.gender ?? 0);
    this.writeNameAt(o + WLF.NAME, entry.passengerName);
    Atomics.store(a, H.WL_COUNT, count + 1);
    return true;
  }

  /** Remove and return the FRONT entry (first come → first served). */
  wlShift() {
    const count = this.wlCount();
    if (count === 0) return null;
    const front = this.wlAt(0);
    const head = Atomics.load(this.i32, H.WL_HEAD);
    this.clearWlEntry(head);
    Atomics.store(this.i32, H.WL_HEAD, (head + 1) % LIMITS.MAX_WAITLIST);
    Atomics.store(this.i32, H.WL_COUNT, count - 1);
    return front;
  }

  /** Remove entry at logical position k by shifting the tail forward (keeps FIFO order). */
  wlRemoveAt(k) {
    const count = this.wlCount();
    if (k < 0 || k >= count) return null;
    const removed = this.wlAt(k);
    const head = Atomics.load(this.i32, H.WL_HEAD);
    for (let i = k; i < count - 1; i++) {
      const oFrom = this.wlOff((head + i + 1) % LIMITS.MAX_WAITLIST);
      const oTo = this.wlOff((head + i) % LIMITS.MAX_WAITLIST);
      const a = this.i32;
      for (const f of [WLF.PASSENGER, WLF.REQUEST, WLF.TIME, WLF.TARGET, WLF.PRODUCER, WLF.SEQ, WLF.AGE, WLF.GENDER]) {
        Atomics.store(a, oTo + f, a[oFrom + f]);
      }
      this.copyName(oFrom + WLF.NAME, oTo + WLF.NAME);
    }
    this.clearWlEntry((head + count - 1) % LIMITS.MAX_WAITLIST);
    Atomics.store(this.i32, H.WL_COUNT, count - 1);
    return removed;
  }

  wlClear() {
    Atomics.store(this.i32, H.WL_HEAD, 0);
    Atomics.store(this.i32, H.WL_COUNT, 0);
  }

  wlFind(passengerId) {
    const count = this.wlCount();
    for (let k = 0; k < count; k++) {
      if (this.wlAt(k).passengerId === passengerId) return k;
    }
    return -1;
  }

  clearWlEntry(physical) {
    const o = this.wlOff(physical);
    for (let i = 0; i < WL_INTS; i++) Atomics.store(this.i32, o + i, 0);
  }

  // -------------------------------------------------------------------------
  // Fixed-width UTF-8 name fields (24 bytes => up to 20 chars kept in the model)
  // -------------------------------------------------------------------------
  writeNameAt(baseInt, text) {
    const bytes = encoder.encode(String(text ?? '').slice(0, 20));
    const off = baseInt * 4;
    for (let i = 0; i < NAME_BYTES; i++) this.u8[off + i] = i < bytes.length ? bytes[i] : 0;
  }

  readNameAt(baseInt) {
    const off = baseInt * 4;
    return decoder.decode(this.u8.subarray(off, off + NAME_BYTES)).replace(/\0/g, '').trim();
  }

  copyName(srcBaseInt, dstBaseInt) {
    const s = srcBaseInt * 4;
    const d = dstBaseInt * 4;
    for (let i = 0; i < NAME_BYTES; i++) this.u8[d + i] = this.u8[s + i];
  }

  // -------------------------------------------------------------------------
  // Aggregated statistics for the dashboard
  // -------------------------------------------------------------------------
  stats() {
    const procN = this.stat('ST_PROC_N');
    return {
      totalRequests: this.stat('ST_TOTAL'),
      bookingsGenerated: this.stat('ST_BOOK_GEN'),
      cancellationsGenerated: this.stat('ST_CANCEL_GEN'),
      processed: this.stat('ST_PROCESSED'),
      confirmed: this.stat('ST_CONFIRMED'),
      failed: this.stat('ST_FAILED'),
      bufferWaitProducers: this.stat('ST_BUF_WAIT_P'),
      bufferWaitConsumers: this.stat('ST_BUF_WAIT_C'),
      lockContention: this.stat('ST_LOCK'),
      raceTests: this.stat('ST_RACE'),
      promotions: this.stat('ST_PROMOTED'),
      cancellationsOk: this.stat('ST_CANCEL_OK'),
      errors: this.stat('ST_ERRORS'),
      waitlistFull: this.stat('ST_WL_FULL'),
      duplicates: this.stat('ST_DUP'),
      avgProcessingMs: procN > 0 ? Math.round((this.stat('ST_PROC_MS') / procN) * 10) / 10 : 0,
      waitingPassengers: this.wlCount(),
      bufferUsed: this.count(),
      bufferCapacity: this.capacity(),
    };
  }
}
