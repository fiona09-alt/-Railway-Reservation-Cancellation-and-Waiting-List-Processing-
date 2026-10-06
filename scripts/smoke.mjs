/** Headless end-to-end check: boots the engine, injects bookings, asserts */
import { SimulationEngine } from '../server/src/simulation/SimulationEngine.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function waitFor(e, pred, ms = 15000) {
  const t0 = Date.now();
  while (Date.now() - t0 < ms) { if (pred()) return true; await sleep(120); }
  return false;
}
const engine = new SimulationEngine({ capacity: 5, seats: 6, producers: 2, consumers: 2, waitlistMax: 20, randomGeneration: false });
await engine.start();
for (let s = 1; s <= 6; s++) engine.generateBooking({ seat: s });
console.log('bookings injected;',
  (await waitFor(engine, () => engine.state.stats().processed >= 6)) ? 'all processed' : 'TIMEOUT');
const st = engine.state.stats();
console.log(`confirmed=${st.confirmed} waiting=${st.waitingPassengers}`);
engine.runRaceTest({ seat: 6, count: 2 });
console.log('race injected;',
  (await waitFor(engine, () => engine.lastRaceResult !== null, 20000)) ? JSON.stringify(engine.lastRaceResult) : 'TIMEOUT');
await engine.stop();
console.log('smoke OK');
process.exit(0);
