import { parentPort, workerData } from 'node:worker_threads';
import { SharedState } from '../sync/SharedState.js';
import { BoundedBuffer } from '../sync/BoundedBuffer.js';
import { applyReservation, OUTCOME } from '../simulation/ReservationCore.js';
import { REQ_TYPE, REQ_STATUS } from '../models/enums.js';
import { stopped, paused, controlWait, rolePaused, simPaused, raceWindow } from './threadUtils.js';

/**
 * CONSUMER WORKER — a real OS thread (node:worker_threads).
 *
 * 1. Acquires the buffer lock.
 * 2. Waits (condition variable) while the buffer is empty.
 * 3. Removes a request, signals notFull (a blocked producer can resume).
 * 4. Releases the buffer lock.
 * 5. Processes the reservation under the SEPARATE reservation mutex —
 *    the critical section that prevents two threads from confirming the
 *    same seat (race-condition prevention).
 */

const state = SharedState.wrap(workerData.sab);
const buffer = new BoundedBuffer(state);
const config = workerData.config;
const thread = `Consumer-${workerData.index}`;

const post = (m) => parentPort.postMessage(m);
const status = (s, detail = null) => post({ t: 'status', thread, state: s, detail });
const log = (level, msg, extra = {}) => post({ t: 'log', level, msg, thread, ...extra });
const lockEvent = (mutex, action, extra = {}) => post({ t: 'lock', mutex, action, thread, ...extra });

function onBufferLock({ phase, contended }) {
  if (phase === 'contended-acquire') {
    state.addStat('ST_LOCK');
    log('WAITING', `${thread} waiting for buffer mutex...`);
    lockEvent('buffer', 'wait');
  } else if (phase === 'acquire') {
    lockEvent('buffer', 'acquire', { contended });
  } else if (phase === 'release') {
    lockEvent('buffer', 'release');
  }
}

function reqId(rec) {
  return `REQ-${String(rec.reqNum).padStart(3, '0')}`;
}

function run() {
  status('RUNNING');
  try {
    while (!stopped(state)) {
      if (paused(state, 'cons')) {
        status('IDLE', 'PAUSED');
        log('INFO', `${thread} paused`);
        controlWait(state, 'cons'); // blocks the OS thread until resume/stop
        if (stopped(state)) break;
        status('RUNNING');
        log('INFO', `${thread} resumed`);
        continue;
      }

      let waitingEmpty = false;
      const rec = buffer.take({
        // While paused we keep waiting instead of draining — lets demos create
        // real BUFFER FULL conditions with blocked producers.
        shouldAbort: () => !stopped(state) && (simPaused(state) || rolePaused(state, 'cons')),
        onLock: onBufferLock,
        onWait: ({ paused: waitBecausePaused }) => {
          if (waitingEmpty) return;
          waitingEmpty = true;
          if (waitBecausePaused) {
            status('IDLE', 'PAUSED');
            log('WAITING', `${thread} paused — not consuming (buffer ${buffer.count}/${buffer.capacity})`);
          } else {
            state.addStat('ST_BUF_WAIT_C');
            status('WAITING', 'BUFFER EMPTY');
            log('WAITING', `BUFFER EMPTY — ${thread} waiting...`);
            lockEvent('buffer', 'wait', { reason: 'BUFFER EMPTY' });
          }
        },
      });

      if (!rec) break; // stop flag + empty buffer → graceful exit

      if (waitingEmpty) {
        status('RUNNING');
        log('INFO', `${thread} resumed — new request inserted (buffer ${buffer.count}/${buffer.capacity})`);
      }

      const id = reqId(rec);
      log('INFO', `${thread} consumed ${id}`, { reqNum: rec.reqNum });
      post({ t: 'consumed', req: { ...rec, id }, thread });
      status('PROCESSING', id);
      processRequest({ ...rec, id });
    }
  } catch (err) {
    log('ERROR', `${thread} worker error: ${err.message}`);
    post({ t: 'error', thread, message: String(err.stack || err) });
  }
  status('STOPPED');
  post({ t: 'exited', thread });
}

// ---------------------------------------------------------------------------
// Reservation processing — BUFFER LOCK IS RELEASED, RESERVATION LOCK TAKEN.
// The two critical sections never overlap, which is what lets producers keep
// filling the buffer while a consumer is booking/cancelling seats.
// ---------------------------------------------------------------------------
function processRequest(rec) {
  const unsafe = state.unsafeMode();
  const start = Date.now();
  const typeLabel = rec.type === REQ_TYPE.BOOK ? 'BOOK' : 'CANCEL';
  let locked = false;

  if (unsafe) {
    log('WARNING', `⚠ UNSYNCED MODE — ${thread} processes ${rec.id} WITHOUT the reservation mutex (race window open)`);
  } else {
    // Instrumented acquire: tryLock first so contention is reported BEFORE blocking.
    const got = state.resMutex.tryLock();
    if (!got) {
      state.addStat('ST_LOCK');
      status('BLOCKED', 'RESERVATION LOCK');
      log('WAITING', `Thread ${thread} waiting for reservation lock...`);
      lockEvent('reservation', 'wait');
      state.resMutex.lock(); // real OS-thread block via Atomics.wait
    }
    locked = true;
    log('LOCK', `Thread ${thread} acquired reservation mutex`);
    lockEvent('reservation', 'acquire', { contended: !got });
  }

  // ---- CRITICAL SECTION (or unsynchronised window in unsafe mode) ----
  const observe = unsafe
    ? (phase) => {
      if (phase === 'write-gap') {
        log('WARNING', `${thread}: check-then-act gap on seat ${rec.seat} with NO lock — another thread may interleave here`);
        raceWindow(state, 3);
      }
    }
    : null;

  let result;
  try {
    result = applyReservation(state, rec, { observe });
  } catch (err) {
    state.addStat('ST_ERRORS');
    result = { outcome: OUTCOME.REJECTED, reason: 'INTERNAL', seat: rec.seat, message: `Internal error: ${err.message}` };
    post({ t: 'error', thread, message: String(err.stack || err) });
  }
  // ---- END CRITICAL SECTION ----

  emitOutcomeLogs(rec, result);

  if (locked) {
    log('UNLOCK', `Thread ${thread} released reservation mutex`);
    lockEvent('reservation', 'release');
    state.resMutex.unlock();
  }

  const processingMs = Date.now() - start;
  state.addStat('ST_PROCESSED');
  state.addStat('ST_PROC_MS', processingMs);
  state.addStat('ST_PROC_N', 1);

  post({
    t: 'processed',
    reqNum: rec.reqNum,
    id: rec.id,
    status: mapOutcome(result),
    outcome: result,
    processingMs,
    consumer: thread,
    unsafe,
    type: rec.type,
    typeLabel,
  });
  if (result.promoted) post({ t: 'promoted', ...result.promoted, consumer: thread });
  status('RUNNING');
}

function mapOutcome(result) {
  if (result.outcome === OUTCOME.CONFIRMED) return REQ_STATUS.CONFIRMED;
  if (result.outcome === OUTCOME.WAITING) return REQ_STATUS.WAITING_LIST;
  if (result.outcome === OUTCOME.CANCELLED) return REQ_STATUS.CANCELLED;
  return result.reason === 'DUPLICATE_BOOKING' ? REQ_STATUS.REJECTED : REQ_STATUS.FAILED;
}

function emitOutcomeLogs(rec, result) {
  const typeLabel = rec.type === REQ_TYPE.BOOK ? 'BOOK' : 'CANCEL';
  if (result.outcome === OUTCOME.CONFIRMED) {
    log('SUCCESS', `Seat ${result.seat} booked for P-${rec.passengerId} (${rec.passengerName}) — CONFIRMED`, { reqNum: rec.reqNum });
  } else if (result.outcome === OUTCOME.WAITING) {
    log('INFO', `Consumer checked Seat ${result.seat} — ${result.seatStateAtCheck}`, { reqNum: rec.reqNum });
    log('WAITING', `P-${rec.passengerId} (${rec.passengerName}) → WAITING LIST position ${result.position}`, { reqNum: rec.reqNum });
  } else if (result.outcome === OUTCOME.CANCELLED) {
    if (result.withdrewFromWaitlist) {
      log('INFO', result.message, { reqNum: rec.reqNum });
    } else {
      log('SUCCESS', `Seat ${result.seat} cancelled by P-${rec.passengerId} (${result.previousOccupant?.name || ''})`, { reqNum: rec.reqNum });
      if (result.promoted) {
        log('SUCCESS', `✓ ${result.promoted.name} promoted from waiting list`, { reqNum: rec.reqNum });
        log('SUCCESS', `✓ Seat ${result.promoted.seat} automatically assigned`, { reqNum: rec.reqNum });
      } else {
        log('INFO', `No waiting passengers — seat ${result.seat} is now AVAILABLE`, { reqNum: rec.reqNum });
      }
    }
  } else {
    log('ERROR', `${typeLabel} ${rec.id} rejected: ${result.message}`, { reqNum: rec.reqNum });
  }
}

parentPort.on('message', (m) => {
  if (m.cmd === 'exit') state.setStop(true);
});

run();

