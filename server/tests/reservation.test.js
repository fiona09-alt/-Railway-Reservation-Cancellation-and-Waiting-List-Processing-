import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SharedState } from '../src/sync/SharedState.js';
import { applyReservation, OUTCOME } from '../src/simulation/ReservationCore.js';
import { RequestFactory } from '../src/models/RequestFactory.js';
import { REQ_TYPE } from '../src/models/enums.js';
import { runInWorkerDoc } from './helpers.mjs';

const cfg = (over = {}) => ({ capacity: 5, seats: 8, producers: 1, consumers: 1, waitlistMax: 50, trainNo: 1001, trainName: 'T', ...over });

// Every mutation wrapped in the reservation mutex — mirrors consumer.worker.js.
function guarded(state, rec) {
  state.resMutex.lock();
  try { return applyReservation(state, rec); }
  finally { state.resMutex.unlock(); }
}

describe('reservation core', () => {
  it('Test 4 — two threads cannot book the same seat (mutex serialises)', async () => {
    const s = SharedState.create(cfg());
    const f = new RequestFactory(s, cfg());
    const seat = 5;
    const a = f.make({ type: REQ_TYPE.BOOK, seat, name: 'A' });
    const b = f.make({ type: REQ_TYPE.BOOK, seat, name: 'B' });
    const [ra, rb] = await Promise.all([
      runInWorkerDoc(s.sab, 'book', a),
      runInWorkerDoc(s.sab, 'book', b),
    ]);
    const outcomes = [ra.out.outcome, rb.out.outcome].sort();
    assert.deepEqual(outcomes, [OUTCOME.CONFIRMED, OUTCOME.WAITING].sort());
    assert.equal(s.seatStatus(seat - 1), 1);
    assert.equal(s.wlCount(), 1, 'loser must be queued FIFO');
  });

  it('Test 5 — occupied seat sends passenger to waiting list', () => {
    const s = SharedState.create(cfg());
    const f = new RequestFactory(s, cfg());
    assert.equal(guarded(s, f.make({ type: REQ_TYPE.BOOK, seat: 2, name: 'Rahul' })).outcome, OUTCOME.CONFIRMED);
    const r = guarded(s, f.make({ type: REQ_TYPE.BOOK, seat: 2, name: 'Atharva' }));
    assert.equal(r.outcome, OUTCOME.WAITING);
    assert.equal(r.position, 1);
    assert.equal(s.wlCount(), 1);
  });

  it('Test 6+7 — cancellation releases the seat AND promotes FIFO head atomically', () => {
    const s = SharedState.create(cfg());
    const f = new RequestFactory(s, cfg());
    const first = f.make({ type: REQ_TYPE.BOOK, seat: 2, name: 'Rahul' });
    guarded(s, first);
    guarded(s, f.make({ type: REQ_TYPE.BOOK, seat: 2, name: 'Atharva' }));
    guarded(s, f.make({ type: REQ_TYPE.BOOK, seat: 2, name: 'Rohan' }));
    assert.equal(s.wlCount(), 2);
    const cancel = { type: REQ_TYPE.CANCEL, seat: 2, passengerId: first.passengerId, passengerName: 'Rahul', reqNum: 999, age: 30, gender: 1, trainNo: 1001, producer: 1 };
    const out = guarded(s, cancel);
    assert.equal(out.outcome, OUTCOME.CANCELLED);
    assert.equal(out.promoted.name, 'Atharva', 'FIRST waiting passenger receives the seat');
    assert.equal(s.wlCount(), 1);
    assert.equal(s.readSeat(1).passengerName, 'Atharva');
    assert.equal(s.wlAt(0).passengerName, 'Rohan');
  });

  it('Test 8 — waiting list maintains FIFO order', () => {
    const s = SharedState.create(cfg());
    const f = new RequestFactory(s, cfg());
    guarded(s, f.make({ type: REQ_TYPE.BOOK, seat: 1, name: 'Holder' }));
    for (const n of ['One', 'Two', 'Three']) guarded(s, f.make({ type: REQ_TYPE.BOOK, seat: 1, name: n }));
    assert.deepEqual(s.wlList().map((w) => w.passengerName), ['One', 'Two', 'Three']);
    assert.equal(s.wlShift().passengerName, 'One');
    assert.deepEqual(s.wlList().map((w) => w.passengerName), ['Two', 'Three']);
  });

  it('guards — invalid seat, duplicate booking, duplicate cancel are rejected safely', () => {
    const s = SharedState.create(cfg());
    const f = new RequestFactory(s, cfg());
    assert.equal(guarded(s, f.make({ type: REQ_TYPE.BOOK, seat: 99, name: 'X' })).outcome, OUTCOME.REJECTED);
    const p = f.make({ type: REQ_TYPE.BOOK, seat: 3, name: 'Dup' });
    assert.equal(guarded(s, p).outcome, OUTCOME.CONFIRMED);
    const dup = { ...f.make({ type: REQ_TYPE.BOOK, seat: 4, name: 'Dup' }), passengerId: p.passengerId };
    assert.equal(guarded(s, dup).reason, 'DUPLICATE_BOOKING');
    const cancelFree = { type: REQ_TYPE.CANCEL, seat: 6, passengerId: 4242, passengerName: 'Ghost', reqNum: 1000, age: 30, gender: 1, trainNo: 1001, producer: 1 };
    assert.equal(guarded(s, cancelFree).reason, 'SEAT_NOT_BOOKED');
  });
});
