/**
 * DemoScenario — the 10-step guided practical demonstration (§25).
 * Drives the LIVE engine (real threads, real buffer, real locks).
 */
import { REQ_TYPE } from '../models/enums.js';

export async function runDemoScenario(engine) {
  const say = (s, m) => engine.demoSay(s, m);
  try {
    say(1, 'Filling seats with confirmed bookings...');
    const n = engine.config.seats;
    for (let seat = 1; seat <= n; seat++) {
      engine.generateBooking({ seat });
      await engine.sleep(120);
    }
    await engine.waitForIdle();

    say(2, 'Generating extra bookings — train is now full...');
    for (let i = 0; i < 3; i++) {
      engine.generateBooking({ seat: 1 + (i % n) });
      await engine.sleep(150);
    }
    await engine.waitForIdle();

    say(3, 'Occupied seats push passengers to the FIFO waiting list...');
    await engine.sleep(800);

    say(4, 'Pausing consumers to create a BUFFER FULL condition...');
    engine.setRolePause('consumers', true);
    for (let i = 0; i < engine.config.capacity + 3; i++) {
      engine.generateBooking({ seat: 1 + (i % n) });
      await engine.sleep(100);
    }
    await engine.sleep(1200);

    say(5, 'Producers are now WAITING — buffer full, backpressure works...');
    await engine.sleep(1200);

    say(6, 'Resuming consumers — watch blocked producers resume...');
    engine.setRolePause('consumers', false);
    await engine.waitForIdle();

    say(7, 'Simultaneous booking attempt for the SAME seat (race)...');
    await engine.sleep(500);

    say(8, 'Reservation mutex serialises the race — one books, rest wait...');
    engine.runRaceTest({ seat: Math.min(10, n), count: 3 });
    await engine.sleep(4500);

    say(9, 'Cancelling an occupied seat...');
    engine.generateCancellation({});
    await engine.waitForIdle();

    say(10, 'First waiting passenger auto-promoted — FIFO + atomicity hold.');
    await engine.sleep(800);
    say(10, 'Demo complete — inspect seats, waiting list, log and stats.');
  } catch (err) {
    engine.pushEvent('ERROR', `Demo error: ${err.message}`);
  }
}
