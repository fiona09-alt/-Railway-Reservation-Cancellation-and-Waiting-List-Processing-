## Architecture

- `server/src/sync/` — `SharedState` (SharedArrayBuffer layout: header, buffer
  ring, seat table, waitlist ring, stats), `Mutex` (Atomics CAS + wait queue),
  `ConditionVariable` (sequence-counter wait/notify), `BoundedBuffer`
  (put/take with `while(full) wait(notFull)` / `while(empty) wait(notEmpty)`).
- `server/src/workers/` — `producer.worker.js` (generates BOOK/CANCEL, blocks on
  `notFull`), `consumer.worker.js` (takes from buffer, then books/cancels under
  the reservation mutex), `threadUtils.js` (role-pause/stop helpers via shared flags).
- `server/src/simulation/` — `ReservationCore.applyReservation` (pure atomic
  check-and-act: book / waitlist / cancel+promote), `SimulationEngine`
  (spawns/joins workers, ledger, event fan-out, race test, 10-step demo),
  `DemoScenario.js`.
- `server/src/models/` — `enums.js` (shared codes), `RequestFactory.js`
  (monotonic `Atomics.add` request/passenger IDs — no two threads reuse an ID).
- `server/src/index.js` — Express REST + WebSocket push (250 ms snapshots).
- `client/src/` — `hooks/useSimulation.js` (WS + REST), `components/` (Header,
  Controls, StateViews, Monitors, Extras), `models/constants.js` (presentation copy).

## Synchronization algorithms

```text
Producer:  loop { rec = next()
             lock(bufMutex); while (count==cap) wait(notFull, bufMutex)
             writeSlot(tail%cap); count++; tail++; signal(notEmpty); unlock(bufMutex) }

Consumer:  loop { lock(bufMutex); while (count==0 && !stop) wait(notEmpty, bufMutex)
             rec = readSlot(head%cap); clear; head++; count--; signal(notFull); unlock(bufMutex)
             lock(resMutex); applyReservation(rec); unlock(resMutex) }   // locks never nest

applyReservation(BOOK s): if invalid → REJECT; if already holds a seat → REJECT
             if free → BOOK it (CONFIRMED) else append FIFO (WAITING)      // atomic under resMutex
applyReservation(CANCEL s): verify holder; free seat; if waitlist non-empty
             shift head → assign same seat (PROMOTED)                      // atomic under resMutex
```

## Data structures (all inside one SharedArrayBuffer)

Header counters/flags · buffer ring (`MAX_BUFFER` slots with inlined request
fields + fixed-width names) · seat table (`MAX_SEATS`: status, occupant,
request, timestamps + names) · waitlist ring (`MAX_WAITLIST`) · stat counters.
Fixed sizes (buffer 64, seats 48, waitlist 100) are pre-flight limits; the UI
config selects the active subset.

## Testing

```bash
npm test   # node --test server/tests/*.test.js
```

| # | Test | Proves |
|---|---|---|
| 1 | producer adds request, FIFO order | ring buffer correctness |
| 2 | producer blocks when full, resumes after take | real `Atomics.wait` on `notFull` |
| 3 | consumer blocks when empty, resumes after put | real `Atomics.wait` on `notEmpty` |
| 4 | two threads cannot book the same seat | mutex serialises check-and-act |
| 5 | occupied seat → waiting list | waitlist enqueue + position |
| 6 | cancellation releases the seat | seat freed |
| 7 | first waiter receives the seat | atomic FIFO promotion |
| 8 | waiting list keeps FIFO order | ring shift correctness |
| 9 | multiple producers + consumers live | end-to-end throughput |
| 10 | reset restores initial state | full state teardown |
| + | live race test, live cancel→promote, guard rejections | engine integration |

## Limitations & honest notes

- Worker threads are OS threads, but the "CPUs" they run on are the Node thread
  pool — scheduling is real but not a bare-metal RTOS.
- Snapshots read shared memory with `tryLock`; a contended moment falls back to
  the last cached view (advisory, never blocks workers).
- Names are fixed-width UTF-8 (24 B) in shared memory; the ledger keeps full strings.
- The UNSAFE (mutex-off) mode is intentionally racy for teaching — it can double
  book, which is the point of the demo.
