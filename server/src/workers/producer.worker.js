import { parentPort, workerData } from 'node:worker_threads';
import { SharedState } from '../sync/SharedState.js';
import { BoundedBuffer } from '../sync/BoundedBuffer.js';
import { RequestFactory } from '../models/RequestFactory.js';
import { REQ_TYPE } from '../models/enums.js';
import { stopped, paused, controlWait } from './threadUtils.js';

/**
 * PRODUCER WORKER — a real OS thread (node:worker_threads).
 *
 * It generates BOOK/CANCEL requests and inserts them into the bounded buffer.
 * When the buffer is full it BLOCKS inside the condition variable
 * (Atomics.wait → OS-level sleep) until a consumer frees a slot — this is the
 * classic producer side of the Producer–Consumer problem.
 *
 * Commands from the engine arrive via parentPort messages:
 *   {cmd:'produce', request} — inject a specific request (buttons / race / demo)
 *   {cmd:'hints', hints}     — booked-seat hints so random CANCELs aim at real bookings
 *   {cmd:'random', enabled}  — toggle background traffic
 */

const state = SharedState.wrap(workerData.sab);
const buffer = new BoundedBuffer(state);
const config = workerData.config;
const producerIndex = workerData.index;
const thread = `Producer-${producerIndex}`;
const factory = new RequestFactory(state, config);

const pending = []; // engine-injected requests (processed with priority)
let hints = [];
let randomEnabled = config.randomGeneration !== false;

const post = (m) => parentPort.postMessage(m);
const status = (s, detail = null) => post({ t: 'status', thread, state: s, detail });
const log = (level, msg, extra = {}) => post({ t: 'log', level, msg, thread, ...extra });

/** Buffer-mutex instrumentation (contention on the BUFFER critical section). */
function onBufferLock({ phase, contended }) {
  if (phase === 'contended-acquire') {
    state.addStat('ST_LOCK');
    log('WAITING', `${thread} waiting for buffer mutex...`);
    post({ t: 'lock', mutex: 'buffer', action: 'wait', thread });
  } else if (phase === 'acquire') {
    post({ t: 'lock', mutex: 'buffer', action: 'acquire', thread, contended });
  } else if (phase === 'release') {
    post({ t: 'lock', mutex: 'buffer', action: 'release', thread });
  }
}

parentPort.on('message', (m) => {
  if (m.cmd === 'produce') pending.push(m.request);
  else if (m.cmd === 'hints') hints = m.hints || [];
  else if (m.cmd === 'random') randomEnabled = Boolean(m.enabled);
});

/**
 * Async pacing between background requests (models the request arrival rate).
 * This is NEVER used to emulate synchronization — real blocking happens in
 * BoundedBuffer.put() via Atomics.wait. Async slices keep this thread able to
 * receive injected commands from the engine.
 */
async function pace(ms) {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    if (stopped(state) || pending.length > 0) return;
    if (paused(state, 'prod')) return;
    await new Promise((r) => setTimeout(r, Math.min(50, Math.max(1, deadline - Date.now()))));
  }
}

async function nextRequest() {
  if (pending.length > 0) return pending.shift();
  if (!randomEnabled || stopped(state)) {
    // Idle yield (NOT synchronization): keeps this thread responsive to
    // injected 'produce' commands without hot-spinning the event loop.
    await new Promise((r) => setTimeout(r, 25));
    if (pending.length > 0) return pending.shift();
    return null;
  }
  await pace(400 + Math.random() * 700);
  if (stopped(state) || paused(state, 'prod') || pending.length > 0) return null;
  return factory.randomRequest(producerIndex, hints);
}

async function run() {
  status('RUNNING');
  try {
    while (!stopped(state)) {
      if (paused(state, 'prod')) {
        status('IDLE', 'PAUSED');
        log('INFO', `${thread} paused`);
        controlWait(state, 'prod'); // blocks the OS thread until resume/stop
        if (stopped(state)) break;
        status('RUNNING');
        log('INFO', `${thread} resumed`);
        continue;
      }

      const rec = await nextRequest();
      if (!rec) continue;

      // Thread-safe arrival statistics (Atomics.add on the shared region).
      state.addStat('ST_TOTAL');
      state.addStat(rec.type === REQ_TYPE.BOOK ? 'ST_BOOK_GEN' : 'ST_CANCEL_GEN');
      const typeLabel = rec.type === REQ_TYPE.BOOK ? 'BOOK' : 'CANCEL';
      log('INFO', `${thread} generated ${typeLabel} request ${rec.id} → P-${rec.passengerId} (${rec.passengerName}) seat ${rec.seat || '-'}`, { reqNum: rec.reqNum });
      post({ t: 'generated', req: rec, thread });

      let waitingFull = false;
      const result = buffer.put(rec, {
        onLock: onBufferLock,
        // Called the first time this producer blocks because the buffer is full.
        onWait: () => {
          if (waitingFull) return;
          waitingFull = true;
          state.addStat('ST_BUF_WAIT_P');
          status('WAITING', 'BUFFER FULL');
          log('WAITING', `BUFFER FULL — ${thread} waiting... (${buffer.count}/${buffer.capacity})`);
          post({ t: 'lock', mutex: 'buffer', action: 'wait', thread, reason: 'BUFFER FULL' });
        },
      });

      if (result.ok) {
        if (waitingFull) {
          status('RUNNING');
          log('INFO', `${thread} resumed — slot released by a consumer (buffer ${buffer.count}/${buffer.capacity})`);
          post({ t: 'lock', mutex: 'buffer', action: 'acquire', thread, contended: true, resumed: true });
        }
        log('INFO', `${rec.id} inserted into buffer (${buffer.count}/${buffer.capacity})`, { reqNum: rec.reqNum });
        post({ t: 'inserted', req: rec, thread });
      } else if (result.aborted) {
        log('WARNING', `${thread}: simulation stopping — ${rec.id} discarded`);
      }
    }
  } catch (err) {
    log('ERROR', `${thread} worker error: ${err.message}`);
    post({ t: 'error', thread, message: String(err.stack || err) });
  }
  status('STOPPED');
  post({ t: 'exited', thread });
}

run();
