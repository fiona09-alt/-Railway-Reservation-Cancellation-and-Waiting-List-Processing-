import { Worker } from 'node:worker_threads';

/** Run one reservation op inside a REAL worker thread under the mutex. */
export function runInWorkerDoc(sab, fnName, payload) {
  return new Promise((resolve, reject) => {
    const w = new Worker(new URL('./worker-harness.mjs', import.meta.url), { workerData: { sab, fnName, payload } });
    w.on('message', (m) => { resolve(m); w.terminate(); });
    w.on('error', reject);
  });
}

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
