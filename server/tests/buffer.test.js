import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SharedState } from '../src/sync/SharedState.js';
import { BoundedBuffer } from '../src/sync/BoundedBuffer.js';
import { RequestFactory } from '../src/models/RequestFactory.js';
import { REQ_TYPE } from '../src/models/enums.js';
import { sleep, runInWorkerDoc } from './helpers.mjs';

const runInWorker = (sab, fnName, payload) => runInWorkerDoc(sab, fnName, payload);

const cfg = (over = {}) => ({ capacity: 5, seats: 12, producers: 1, consumers: 1, waitlistMax: 50, trainNo: 1001, trainName: 'T', ...over });

describe('bounded buffer', () => {
  it('Test 1 — producer adds request; FIFO order preserved', () => {
    const s = SharedState.create(cfg());
    const b = new BoundedBuffer(s);
    const f = new RequestFactory(s, cfg());
    const a = f.make({ type: REQ_TYPE.BOOK, seat: 1, producer: 1 });
    const c = f.make({ type: REQ_TYPE.CANCEL, seat: 2, producer: 1 });
    assert.equal(b.put(a).ok, true);
    assert.equal(b.put(c).ok, true);
    assert.equal(s.count(), 2);
    assert.equal(b.take().reqNum, a.reqNum);
    assert.equal(b.take().reqNum, c.reqNum);
  });

  it('Test 2 — producer blocks when buffer is full, resumes after take', async () => {
    const s = SharedState.create(cfg({ capacity: 2 }));
    const b = new BoundedBuffer(s);
    const f = new RequestFactory(s, cfg({ capacity: 2 }));
    b.put(f.make({ type: REQ_TYPE.BOOK, seat: 1, producer: 1 }));
    b.put(f.make({ type: REQ_TYPE.BOOK, seat: 2, producer: 1 }));
    assert.equal(s.count(), 2);
    const rec = f.make({ type: REQ_TYPE.BOOK, seat: 3, producer: 2 });
    let done = false;
    const p = runInWorker(s.sab, 'put', rec).then((m) => { done = true; return m; });
    await sleep(400); // worker should still be blocked inside Atomics.wait
    assert.equal(done, false, 'producer must BLOCK while buffer is full (real Atomics.wait)');
    assert.equal(s.stat('ST_BUF_WAIT_P') >= 0, true);
    b.take(); // free one slot → producer must resume
    const m = await p;
    assert.equal(m.ok, true);
    assert.equal(s.count(), 2);
  });

  it('Test 3 — consumer blocks when buffer is empty, resumes after put', async () => {
    const s = SharedState.create(cfg({ capacity: 2 }));
    const b = new BoundedBuffer(s);
    let done = false;
    const p = runInWorker(s.sab, 'take', {}).then((m) => { done = true; return m; });
    await sleep(400);
    assert.equal(done, false, 'consumer must BLOCK while buffer is empty');
    const f = new RequestFactory(s, cfg());
    b.put(f.make({ type: REQ_TYPE.BOOK, seat: 5, producer: 1 }));
    const m = await p;
    assert.equal(m.rec.seat, 5);
  });
});
