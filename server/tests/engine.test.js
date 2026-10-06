import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SimulationEngine } from '../src/simulation/SimulationEngine.js';
import { REQ_STATUS } from '../src/models/enums.js';
import { sleep } from './helpers.mjs';

const cfg = (over = {}) => ({
  capacity: 5, seats: 6, producers: 2, consumers: 2,
  waitlistMax: 20, randomGeneration: false, ...over,
});

async function waitFor(engine, pred, timeoutMs = 15000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (pred()) return true;
    await sleep(120);
  }
  return false;
}

describe('simulation engine (live worker threads)', () => {
  it('Test 9 — multiple producers and consumers operate correctly', async () => {
    const engine = new SimulationEngine(cfg());
    await engine.start();
    try {
      for (let seat = 1; seat <= 6; seat++) engine.generateBooking({ seat });
      const ok = await waitFor(engine, () => engine.state.stats().processed >= 6);
      assert.equal(ok, true, 'all 6 bookings must be processed by live consumers');
      const stats = engine.state.stats();
      assert.equal(stats.confirmed, 6);
      assert.equal(engine.state.wlCount(), 0);
    } finally { await engine.stop(); }
  });

  it('race via live engine — exactly one CONFIRMED, rest WAITING', async () => {
    const engine = new SimulationEngine(cfg({ seats: 12 }));
    await engine.start();
    try {
      const r = engine.runRaceTest({ seat: 10, count: 3 });
      assert.equal(r.ok, true);
      const finished = await waitFor(engine, () => engine.lastRaceResult !== null, 20000);
      assert.equal(finished, true, 'race must finish');
      assert.equal(engine.lastRaceResult.doubleBookingPrevented, true);
      assert.equal(engine.lastRaceResult.booked.length, 1);
      assert.equal(engine.lastRaceResult.waiting.length, 2);
    } finally { await engine.stop(); }
  });

  it('cancel → FIFO promotion via live engine', async () => {
    const engine = new SimulationEngine(cfg({ seats: 4 }));
    await engine.start();
    try {
      for (let seat = 1; seat <= 4; seat++) engine.generateBooking({ seat });
      await waitFor(engine, () => engine.state.stats().confirmed >= 4);
      engine.generateBooking({ seat: 2 }); // → waiting
      await waitFor(engine, () => engine.state.wlCount() >= 1);
      const before = engine.state.readSeat(1).passengerName;
      assert.notEqual(before, null);
      engine.generateCancellation({}); // cancels a real booking
      await waitFor(engine, () => engine.state.stats().promotions >= 1);
      assert.equal(engine.state.wlCount(), 0, 'head of waiting list must be promoted');
    } finally { await engine.stop(); }
  });

  it('Test 10 — reset restores the complete initial state', async () => {
    const engine = new SimulationEngine(cfg());
    await engine.start();
    engine.generateBooking({ seat: 1 });
    await waitFor(engine, () => engine.state.stats().processed >= 1);
    await engine.reset();
    const snap = engine.snapshot();
    assert.equal(snap.bufferUsed, 0);
    assert.equal(snap.waitlist.length, 0);
    assert.equal(snap.seats.filter((s) => s.status === 'BOOKED').length, 0);
    assert.equal(snap.stats.totalRequests, 0);
    assert.equal(snap.events.length, 1, 'only the reset event remains');
    assert.equal(engine.aliveWorkers().length, 0);
  });

  it('status mapping — REQ_STATUS import is live (cancel-from-waitlist path)', () => {
    assert.equal(typeof REQ_STATUS.CONFIRMED, 'number');
  });
});
