/**
 * Minimal harness: runs one blocking buffer/reservation op inside a real
 * worker thread so tests can prove producers/consumers genuinely block.
 */
import { parentPort, workerData } from 'node:worker_threads';
import { SharedState } from '../src/sync/SharedState.js';
import { BoundedBuffer } from '../src/sync/BoundedBuffer.js';
import { applyReservation } from '../src/simulation/ReservationCore.js';

const state = SharedState.wrap(workerData.sab);
const buffer = new BoundedBuffer(state);
const { fnName, payload } = workerData;

if (fnName === 'put') {
  const r = buffer.put(payload, { onWait: () => state.addStat('ST_BUF_WAIT_P') });
  parentPort.postMessage({ ok: r.ok === true });
} else if (fnName === 'take') {
  const rec = buffer.take({ onWait: () => state.addStat('ST_BUF_WAIT_C') });
  parentPort.postMessage({ rec });
} else if (fnName === 'book') {
  // Contended reservation critical section, exactly like a consumer thread.
  state.resMutex.lock();
  let out;
  try { out = applyReservation(state, payload); }
  finally { state.resMutex.unlock(); }
  parentPort.postMessage({ out });
}
