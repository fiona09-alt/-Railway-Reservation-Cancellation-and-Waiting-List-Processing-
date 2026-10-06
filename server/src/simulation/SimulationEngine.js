import { EventEmitter } from 'node:events';
import { Worker } from 'node:worker_threads';
import { SharedState } from '../sync/SharedState.js';
import { BoundedBuffer } from '../sync/BoundedBuffer.js';
import { RequestFactory } from '../models/RequestFactory.js';
import {
  validateConfig, MAX_EVENT_HISTORY, MAX_LEDGER_ENTRIES, WORKER_EXIT_TIMEOUT_MS,
} from '../config.js';
import * as E from '../models/enums.js';
import { runDemoScenario } from './DemoScenario.js';

const PRODUCER_URL = new URL('../workers/producer.worker.js', import.meta.url);
const CONSUMER_URL = new URL('../workers/consumer.worker.js', import.meta.url);

/**
 * SimulationEngine — main-thread coordinator.
 *
 * Owns the SharedArrayBuffer, spawns the producer/consumer worker threads,
 * aggregates their messages into the event log / request ledger / thread
 * monitor / lock monitor, and exposes the commands used by the UI and tests.
 *
 * The engine itself NEVER performs reservation operations — those happen
 * inside the consumer threads under the reservation mutex. The engine only
 * orchestrates (start/stop/pause), injects requests, and reads snapshots.
 */
export class SimulationEngine extends EventEmitter {
  constructor(configInput = {}) {
    super();
    const { ok, errors, config } = validateConfig(configInput);
    if (!ok) throw new Error(`Invalid configuration: ${errors.join(' ')}`);
    this.config = config;
    this.state = SharedState.create(config);
    this.buffer = new BoundedBuffer(this.state);
    this.factory = new RequestFactory(this.state, config);

    this.workers = []; // {worker, kind, index, name, exited}
    this.threads = new Map(); // name -> {state, detail, ts}
    this.locks = {
      buffer: { owner: null, waiters: [] },
      reservation: { owner: null, waiters: [] },
    };
    this.ledger = new Map(); // reqNum -> request ledger record
    this.events = []; // ring buffer of log entries
    this.eventSeq = 0;
    this.lastRaceResult = null;
    this.activeRace = null;
    this.demoRunning = false;
    this.demoStep = 0;
    this.lastBufferView = [];
    this.lastSeatView = [];
    this.lastWaitlistView = [];
    this.rrProducer = 0;
    this.hintTimer = null;
    this.stopping = false;
  }

  // -------------------------------------------------------------------------
  // Worker lifecycle
  // -------------------------------------------------------------------------
  spawnWorkers() {
    const spawn = (kind, index, url) => {
      const name = `${kind === 'producer' ? 'Producer' : 'Consumer'}-${index}`;
      const worker = new Worker(url, {
        workerData: { sab: this.state.sab, config: this.config, index },
      });
      const entry = { worker, kind, index, name, exited: false };
      worker.on('message', (msg) => this.handleWorkerMessage(entry, msg));
      worker.on('error', (err) => {
        this.pushEvent(E.LOG_LEVEL.ERROR, `${name} crashed: ${err.message}`);
        this.threads.set(name, { state: E.THREAD_STATE.STOPPED, detail: 'ERROR', ts: Date.now() });
      });
      worker.on('exit', () => {
        entry.exited = true;
        const t = this.threads.get(name);
        if (t) this.threads.set(name, { ...t, state: E.THREAD_STATE.STOPPED, ts: Date.now() });
        this.emit('threads_changed');
      });
      this.workers.push(entry);
      this.threads.set(name, { state: E.THREAD_STATE.RUNNING, detail: null, ts: Date.now() });
    };

    for (let i = 1; i <= this.config.producers; i++) spawn('producer', i, PRODUCER_URL);
    for (let i = 1; i <= this.config.consumers; i++) spawn('consumer', i, CONSUMER_URL);
    this.startHints();
  }

  aliveWorkers() {
    return this.workers.filter((w) => !w.exited);
  }

  isRunning() {
    return this.aliveWorkers().length > 0 && this.state.simState() !== E.SIM_STATE.STOPPED;
  }

  async start() {
    if (this.stopping) return { ok: false, error: 'Stop in progress' };
    if (this.aliveWorkers().length > 0) return this.resume();
    this.state.setStop(false);
    this.state.setSimState(E.SIM_STATE.RUNNING);
    this.state.wakeAll();
    this.spawnWorkers();
    this.pushEvent(E.LOG_LEVEL.SYSTEM,
      `Simulation started — ${this.config.producers} producers, ${this.config.consumers} consumers, buffer capacity ${this.config.capacity}, seats ${this.config.seats}`);
    this.emit('state_changed');
    return { ok: true };
  }

  async pause() {
    if (!this.isRunning()) return { ok: false, error: 'Simulation is not running' };
    this.state.setSimState(E.SIM_STATE.PAUSED);
    this.state.wakeAll();
    this.pushEvent(E.LOG_LEVEL.SYSTEM, 'Simulation PAUSED — workers idle at their next checkpoint');
    this.emit('state_changed');
    return { ok: true };
  }

  async resume() {
    if (this.aliveWorkers().length === 0) return this.start();
    this.state.setSimState(E.SIM_STATE.RUNNING);
    this.state.wakeAll();
    this.pushEvent(E.LOG_LEVEL.SYSTEM, 'Simulation RESUMED — blocked threads re-check their conditions');
    this.emit('state_changed');
    return { ok: true };
  }

  setRolePause(role, value) {
    if (!this.isRunning()) return { ok: false, error: 'Simulation is not running' };
    if (role === 'consumers') this.state.setPauseConsumers(value);
    else if (role === 'producers') this.state.setPauseProducers(value);
    else return { ok: false, error: `Unknown role: ${role}` };
    this.state.wakeAll();
    const label = role === 'consumers' ? 'Consumer' : 'Producer';
    this.pushEvent(E.LOG_LEVEL.SYSTEM, `${label}s ${value ? 'paused' : 'resumed'}${value ? ' — watch the buffer react' : ''}`);
    this.emit('state_changed');
    return { ok: true };
  }

  setUnsafe(value) {
    this.state.setUnsafe(value);
    this.pushEvent(
      value ? E.LOG_LEVEL.WARNING : E.LOG_LEVEL.SUCCESS,
      value
        ? '⚠ UNSAFE MODE enabled — reservation mutex DISABLED (educational race demonstration)'
        : 'Reservation mutex ENABLED — double booking is now impossible',
    );
    this.emit('state_changed');
    return { ok: true };
  }

  /**
   * Stop: producers halt immediately; consumers drain whatever is left in the
   * buffer (graceful shutdown — no request is silently lost), then every
   * worker thread exits. Falls back to terminate() after a timeout so the UI
   * can never hang.
   */
  async stop() {
    if (this.workers.length === 0) {
      this.state.setSimState(E.SIM_STATE.STOPPED);
      this.emit('state_changed');
      return { ok: true };
    }
    this.stopping = true;
    this.stopHints();
    this.state.setStop(true);
    this.state.wakeAll();
    this.pushEvent(E.LOG_LEVEL.SYSTEM, 'Stop requested — producers halt, consumers drain remaining requests');
    await this.awaitWorkersExit();
    this.state.setSimState(E.SIM_STATE.STOPPED);
    this.stopping = false;
    this.pushEvent(E.LOG_LEVEL.SYSTEM, 'Simulation STOPPED — all worker threads exited');
    this.emit('state_changed');
    return { ok: true };
  }

  async awaitWorkersExit() {
    const alive = this.aliveWorkers();
    await Promise.race([
      Promise.all(alive.map((e) => new Promise((resolve) => e.worker.once('exit', resolve)))),
      new Promise((resolve) => setTimeout(resolve, WORKER_EXIT_TIMEOUT_MS)),
    ]);
    for (const e of this.aliveWorkers()) {
      await e.worker.terminate().catch(() => {});
      e.exited = true;
    }
    this.workers = [];
  }

  /** Full reset: stops threads, replaces the shared region, clears all UI state. */
  async reset(configInput = null) {
    if (this.aliveWorkers().length > 0 || this.stopping) await this.stop();
    this.stopHints();
    if (configInput) {
      const { ok, errors, config } = validateConfig(configInput);
      if (!ok) return { ok: false, error: errors.join(' ') };
      this.config = config;
    }
    // Brand-new SharedArrayBuffer => every counter, lock, seat and slot is zero.
    this.state = SharedState.create(this.config);
    this.buffer = new BoundedBuffer(this.state);
    this.factory = new RequestFactory(this.state, this.config);
    this.workers = [];
    this.threads.clear();
    this.locks = { buffer: { owner: null, waiters: [] }, reservation: { owner: null, waiters: [] } };
    this.ledger.clear();
    this.events = [];
    this.eventSeq = 0;
    this.lastRaceResult = null;
    this.activeRace = null;
    this.demoRunning = false;
    this.demoStep = 0;
    this.lastBufferView = [];
    this.lastSeatView = [];
    this.lastWaitlistView = [];
    this.rrProducer = 0;
    this.pushEvent(E.LOG_LEVEL.SYSTEM, 'System RESET — seats, waiting list, buffer, threads, statistics and locks restored');
    this.emit('reset');
    this.emit('state_changed');
    return { ok: true };
  }

  applyConfig(configInput) {
    if (this.aliveWorkers().length > 0) {
      return { ok: false, error: 'Stop the simulation before changing the configuration' };
    }
    const { ok, errors, config } = validateConfig(configInput);
    if (!ok) return { ok: false, error: errors.join(' ') };
    this.config = config;
    return this.reset(config);
  }

  // -------------------------------------------------------------------------
  // Worker message handling → event log, ledger, thread & lock monitors
  // -------------------------------------------------------------------------
  handleWorkerMessage(entry, msg) {
    switch (msg.t) {
      case 'log':
        this.pushEvent(msg.level, msg.msg, msg.thread ?? entry.name, msg.reqNum);
        break;
      case 'status':
        this.threads.set(msg.thread, { state: msg.state, detail: msg.detail, ts: Date.now() });
        this.emit('threads_changed');
        break;
      case 'lock':
        this.updateLock(msg);
        break;
      case 'generated':
        this.ledgerSet(msg.req.reqNum, {
          id: msg.req.id,
          reqNum: msg.req.reqNum,
          type: msg.req.type,
          typeLabel: msg.req.type === E.REQ_TYPE.BOOK ? 'BOOK' : 'CANCEL',
          passengerId: msg.req.passengerId,
          passengerName: msg.req.passengerName,
          seat: msg.req.seat,
          age: msg.req.age,
          gender: msg.req.gender,
          producer: entry.name,
          status: E.REQ_STATUS.GENERATED,
          created: msg.req.created,
          processedAt: null,
          consumer: null,
          outcome: null,
          processingMs: null,
        });
        break;
      case 'inserted':
        this.ledgerPatch(msg.req.reqNum, { status: E.REQ_STATUS.IN_BUFFER });
        break;
      case 'consumed':
        this.ledgerPatch(msg.req.reqNum, { status: E.REQ_STATUS.CONSUMED, consumer: msg.thread });
        break;
      case 'processed':
        this.onProcessed(msg);
        break;
      case 'promoted':
        this.onPromoted(msg);
        break;
      case 'error':
        this.pushEvent(E.LOG_LEVEL.ERROR, msg.message, msg.thread);
        break;
      case 'exited':
        this.emit('threads_changed');
        break;
      default:
        break;
    }
  }

  updateLock(msg) {
    const lock = this.locks[msg.mutex];
    if (!lock) return;
    if (msg.action === 'wait') {
      if (!lock.waiters.includes(msg.thread)) lock.waiters.push(msg.thread);
    } else if (msg.action === 'acquire') {
      lock.owner = msg.thread;
      lock.waiters = lock.waiters.filter((t) => t !== msg.thread);
      lock.contended = Boolean(msg.contended);
    } else if (msg.action === 'release') {
      if (lock.owner === msg.thread) lock.owner = null;
      lock.contended = false;
    }
    this.emit('locks_changed');
  }

  onProcessed(msg) {
    const now = Date.now();
    const entry = this.ledger.get(msg.reqNum);
    const status = entry && entry.type === E.REQ_TYPE.CANCEL
      && msg.outcome && msg.outcome.withdrewFromWaitlist
      ? E.REQ_STATUS.CANCELLED
      : msg.status;
    this.ledgerPatch(msg.reqNum, {
      status,
      processedAt: now,
      consumer: msg.consumer,
      processingMs: msg.processingMs,
      outcome: { outcome: msg.outcome.outcome, reason: msg.outcome.reason, seat: msg.outcome.seat, promoted: msg.outcome.promoted },
    });
    this.emit('processed', msg);

    if (this.activeRace && this.activeRace.reqNums.has(msg.reqNum)) {
      this.activeRace.pending.delete(msg.reqNum);
      if (this.activeRace.pending.size === 0) this.finishRace();
    }
  }

  onPromoted(msg) {
    this.ledgerPatch(msg.requestId, { status: E.REQ_STATUS.PROMOTED, seat: msg.seat });
    this.emit('promotion', msg);
    this.pushEvent(E.LOG_LEVEL.SUCCESS,
      `✓ ${msg.name} promoted from waiting list → seat ${msg.seat} (by ${msg.consumer})`, msg.consumer);
  }

  ledgerSet(reqNum, record) {
    this.ledger.set(reqNum, record);
    if (this.ledger.size > MAX_LEDGER_ENTRIES) {
      const oldest = this.ledger.keys().next().value;
      this.ledger.delete(oldest);
    }
    this.emit('ledger_changed');
  }

  ledgerPatch(reqNum, patch) {
    const rec = this.ledger.get(reqNum);
    if (!rec) return;
    Object.assign(rec, patch);
    this.emit('ledger_changed');
  }

  pushEvent(level, msg, thread = null, reqNum = null) {
    const ts = Date.now();
    const d = new Date(ts);
    const pad = (n, w = 2) => String(n).padStart(w, '0');
    const entry = {
      seq: ++this.eventSeq,
      ts,
      time: `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`,
      level,
      thread,
      reqNum,
      msg,
    };
    this.events.push(entry);
    if (this.events.length > MAX_EVENT_HISTORY) this.events.shift();
    this.emit('event', entry);
    return entry;
  }

  // Periodically broadcast booked-seat hints so random CANCEL traffic can aim
  // at real bookings (stale hints intentionally exercise error handling).
  startHints() {
    this.stopHints();
    this.hintTimer = setInterval(() => this.broadcastHints(), 1500);
    this.hintTimer.unref?.();
  }

  stopHints() {
    if (this.hintTimer) clearInterval(this.hintTimer);
    this.hintTimer = null;
  }

  broadcastHints() {
    const hints = [];
    const n = this.state.seatCount();
    for (let i = 0; i < n; i++) {
      if (this.state.seatStatus(i) === 1) {
        hints.push({ seat: i + 1, passengerId: this.state.seatOccupant(i), passengerName: this.state.readSeat(i).passengerName });
      }
    }
    for (const w of this.aliveWorkers()) {
      if (w.kind === 'producer') w.worker.postMessage({ cmd: 'hints', hints });
    }
  }

  // -------------------------------------------------------------------------
  // Snapshots for the UI (advisory reads — never block worker threads)
  // -------------------------------------------------------------------------
  snapshot() {
    let bufferView = null;
    try { bufferView = this.buffer.tryReadAll(); } catch { bufferView = null; }
    if (bufferView) {
      this.lastBufferView = bufferView.map((r) => ({
        reqNum: r.reqNum, id: `REQ-${String(r.reqNum).padStart(3, '0')}`,
        type: r.type, typeLabel: r.type === E.REQ_TYPE.BOOK ? 'BOOK' : 'CANCEL',
        passengerId: r.passengerId, passengerName: r.passengerName,
        seat: r.seat, producer: r.producer,
      }));
    }
    const seats = [];
    try {
      const n = this.state.seatCount();
      for (let i = 0; i < n; i++) {
        const s = this.state.readSeat(i);
        seats.push({
          number: s.number,
          status: s.status === 1 ? 'BOOKED' : 'AVAILABLE',
          passengerId: s.passengerId || null,
          passengerName: s.passengerName || null,
          requestId: s.requestId || null,
        });
      }
      this.lastSeatView = seats;
    } catch { /* keep last cached view */ }
    let waitlist = this.lastWaitlistView;
    try {
      waitlist = this.state.wlList().map((w) => ({
        position: w.position, passengerId: w.passengerId,
        passengerName: w.passengerName, requestId: w.requestId,
        targetSeat: w.targetSeat,
      }));
      this.lastWaitlistView = waitlist;
    } catch { /* keep last */ }
    const threads = [...this.threads.entries()].map(([name, t]) => ({ name, ...t }));
    for (let i = 1; i <= this.config.producers; i++) {
      const n = `Producer-${i}`;
      if (!this.threads.has(n)) threads.push({ name: n, state: 'STOPPED', detail: null });
    }
    for (let i = 1; i <= this.config.consumers; i++) {
      const n = `Consumer-${i}`;
      if (!this.threads.has(n)) threads.push({ name: n, state: 'STOPPED', detail: null });
    }
    let stats = null;
    try { stats = this.state.stats(); } catch { stats = null; }
    return {
      simState: E.SIM_STATE_LABEL[this.state.simState()] || 'STOPPED',
      unsafe: this.state.unsafeMode(),
      producersPaused: this.state.producersPaused(),
      consumersPaused: this.state.consumersPaused(),
      config: this.config,
      stats, seats: this.lastSeatView, waitlist,
      buffer: this.lastBufferView,
      bufferCapacity: this.config.capacity,
      bufferUsed: this.lastBufferView.length,
      threads, locks: this.locks,
      requests: [...this.ledger.values()].slice(-80).reverse(),
      events: this.events.slice(-120),
      lastRaceResult: this.lastRaceResult,
      demoRunning: this.demoRunning, demoStep: this.demoStep,
    };
  }

  // -------------------------------------------------------------------------
  // Request injection (UI buttons / race test / demo)
  // -------------------------------------------------------------------------
  ensureRunning() {
    if (this.aliveWorkers().length === 0) return { ok: false, error: 'Start the simulation first' };
    return { ok: true };
  }

  pickProducer() {
    const alive = this.aliveWorkers().filter((w) => w.kind === 'producer');
    if (alive.length === 0) return null;
    const w = alive[this.rrProducer % alive.length];
    this.rrProducer = (this.rrProducer + 1) % alive.length;
    return w;
  }

  inject(spec) {
    const gate = this.ensureRunning();
    if (!gate.ok) return gate;
    const worker = this.pickProducer();
    if (!worker) return { ok: false, error: 'No producer threads alive' };
    const rec = this.factory.make({ ...spec, producer: worker.index });
    worker.worker.postMessage({ cmd: 'produce', request: rec });
    return { ok: true, request: { ...rec, id: rec.id } };
  }

  generateBooking({ seat = null } = {}) {
    const target = seat ?? (1 + Math.floor(Math.random() * this.config.seats));
    return this.inject({ type: E.REQ_TYPE.BOOK, seat: target });
  }

  generateCancellation({ seat = null, passengerId = null, name = null } = {}) {
    let targetSeat = seat; let targetPid = passengerId; let targetName = name;
    if (targetSeat == null || targetPid == null) {
      try {
        const booked = [];
        for (let i = 0; i < this.state.seatCount(); i++) {
          if (this.state.seatStatus(i) === 1) {
            const s = this.state.readSeat(i);
            booked.push({ seat: i + 1, passengerId: s.passengerId, passengerName: s.passengerName });
          }
        }
        if (booked.length > 0) {
          const pick = booked[Math.floor(Math.random() * booked.length)];
          targetSeat = targetSeat ?? pick.seat;
          if (targetPid == null && targetSeat === pick.seat) {
            targetPid = pick.passengerId; targetName = targetName || pick.passengerName;
          }
        }
      } catch { /* ignore */ }
    }
    if (targetSeat == null) targetSeat = 1 + Math.floor(Math.random() * this.config.seats);
    const spec = { type: E.REQ_TYPE.CANCEL, seat: targetSeat };
    if (targetPid != null) spec.passengerId = targetPid;
    if (targetName) spec.name = targetName;
    return this.inject(spec);
  }

  // ---- Race test: N simultaneous BOOKs for the SAME seat ------------------
  runRaceTest({ seat = 10, count = 3 } = {}) {
    const gate = this.ensureRunning();
    if (!gate.ok) return gate;
    const target = Math.min(Math.max(1, seat || 1), this.config.seats);
    // Free the target seat first so the demo is deterministic: exactly ONE of
    // the simultaneous racers must book it, the rest must queue. The sweep
    // runs under the reservation mutex, atomically with any promotion.
    try {
      this.state.resMutex.lock();
      try {
        const idx = target - 1;
        if (this.state.seatStatus(idx) === 1) {
          this.state.seatRelease(idx);
          this.pushEvent(E.LOG_LEVEL.INFO, `Race setup — seat ${target} freed so ${count} racers contend for it`);
        }
      } finally { this.state.resMutex.unlock(); }
    } catch { /* advisory setup only */ }
    this.state.addStat('ST_RACE');
    const reqNums = new Set(); const produced = [];
    const wasPaused = this.state.consumersPaused();
    this.state.setPauseConsumers(true); // stage all requests, then release
    for (let i = 0; i < count; i++) {
      const r = this.inject({ type: E.REQ_TYPE.BOOK, seat: target });
      if (r.ok) { reqNums.add(r.request.reqNum); produced.push(r.request); }
    }
    this.activeRace = { seat: target, reqNums, pending: new Set(reqNums), startedAt: Date.now() };
    this.pushEvent(E.LOG_LEVEL.WARNING,
      `RACE TEST started — ${count} passengers simultaneously booking seat ${target}: ` +
      produced.map((p) => `${p.passengerName}(P-${p.passengerId})`).join(', '));
    setTimeout(() => {
      this.state.setPauseConsumers(wasPaused);
      this.state.wakeAll();
      setTimeout(() => { if (this.activeRace && this.activeRace.pending.size > 0) this.finishRace(); }, 8000);
    }, 600);
    return { ok: true, seat: target, requests: produced };
  }

  finishRace() {
    const race = this.activeRace;
    if (!race) return;
    this.activeRace = null;
    const recs = [...race.reqNums].map((n) => this.ledger.get(n)).filter(Boolean);
    const booked = recs.filter((r) => r.status === E.REQ_STATUS.CONFIRMED);
    const waiting = recs.filter((r) => r.status === E.REQ_STATUS.WAITING_LIST);
    let holders = [];
    try {
      const s = this.state.readSeat(race.seat - 1);
      if (s.status === 1) holders = [{ passengerId: s.passengerId, passengerName: s.passengerName }];
    } catch { /* ignore */ }
    const prevented = booked.length <= 1 && holders.length <= 1;
    this.lastRaceResult = {
      seat: race.seat, at: Date.now(),
      booked: booked.map((r) => ({ passengerName: r.passengerName, passengerId: r.passengerId, id: r.id })),
      waiting: waiting.map((r) => ({ passengerName: r.passengerName, passengerId: r.passengerId, id: r.id })),
      doubleBookingPrevented: prevented,
      mutexProtection: prevented ? 'SUCCESS' : 'FAILED',
    };
    this.pushEvent(prevented ? E.LOG_LEVEL.SUCCESS : E.LOG_LEVEL.ERROR,
      prevented
        ? `RACE TEST COMPLETE — seat ${race.seat}: ${booked[0] ? `${booked[0].passengerName} BOOKED` : 'pending'}; ` +
          `${waiting.length} on waiting list. Double booking PREVENTED`
        : `RACE TEST FAILED — seat ${race.seat} has ${booked.length} confirmed bookings!`);
    this.emit('race_finished', this.lastRaceResult);
    this.emit('state_changed');
  }

  // ---- Demo scenario -------------------------------------------------------
  async loadDemo() {
    const gate = this.ensureRunning();
    if (!gate.ok) return gate;
    if (this.demoRunning) return { ok: false, error: 'Demo already running' };
    this.demoRunning = true; this.demoStep = 0;
    this.emit('state_changed');
    runDemoScenario(this).finally(() => {
      this.demoRunning = false;
      this.emit('state_changed');
    });
    return { ok: true };
  }

  demoSay(step, msg) {
    this.demoStep = step;
    this.pushEvent(E.LOG_LEVEL.DEMO, `DEMO Step ${step}/10 — ${msg}`);
    this.emit('state_changed');
  }

  sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

  async waitForIdle(timeoutMs = 15000) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const stats = this.state.stats();
      if (stats.bufferUsed === 0 && stats.totalRequests === stats.processed) return true;
      await this.sleep(150);
    }
    return false;
  }

}
