# Railway Reservation, Cancellation & Waiting-List Processing
## using the Producer–Consumer Synchronization Model

A working OS-practical simulation with **real OS threads** (`worker_threads`),
a real **bounded buffer** in `SharedArrayBuffer`, **Atomics-based mutexes**,
**condition variables** (`Atomics.wait`/`notify`), a genuine **race-condition
demonstration**, a **FIFO waiting list**, and **atomic cancellation → promotion**.

```
                 ┌───────────────────────────┐
  Producer-1 ──► │                           │ ──► Consumer-1 ──┐
  Producer-2 ──► │      BOUNDED BUFFER       │ ──► Consumer-2 ──┼──► RESERVATION MUTEX ──► seats + FIFO waitlist
  Producer-3 ──► │  (bufMutex + notFull /    │ ──► Consumer-3 ──┘         (check-and-book
                 │   notEmpty conditions)    │                            is atomic)
                 └───────────────────────────┘
```

## Quick start

```bash
npm install
npm test            # 13 automated tests (must all pass)
npm run smoke       # headless end-to-end check of the live engine
npm run build       # rebuild client bundle into server/public
npm start           # http://localhost:4000 (serves dashboard + API + WS)
npm run dev         # backend :4000 + vite dev :5173 (proxied /api + /ws)
```

## OS concepts demonstrated (all observable in the UI)

| Concept | Where to see it |
|---|---|
| Threads / multithreading | Thread Monitor: Producer-1..N, Consumer-1..M with live states |
| Bounded buffer | Request Buffer panel: `used/capacity`, FULL / EMPTY status |
| Blocking (producer waits when full) | `BUFFER FULL — Producer-N waiting...`, `ST_BUF_WAIT_P` stat, pause consumers to force it |
| Blocking (consumer waits when empty) | `BUFFER EMPTY — Consumer-N waiting...`, press Start with no traffic |
| Mutex / critical section | Sync Monitor: BUFFER MUTEX vs RESERVATION MUTEX, owner + waiters |
| Condition variables | `notFull` / `notEmpty` wake-ups logged as "resumed" events |
| Race condition | ⚡ Race Test: 3 passengers, 1 seat — log shows check→book serialised |
| Mutual exclusion proof | Double booking PREVENTED ✓ panel; disable mutex to see the danger |
| FIFO queue | Waiting List panel; cancellation promotes position #1 |
| Atomic promotion | Cancel log: seat freed + head promoted inside one mutex hold |

## Demo script (≈2 min viva)

1. `Start` → consumers log `BUFFER EMPTY … waiting`.
2. `+ Booking` × 7 with capacity 5 → watch buffer fill, then `BUFFER FULL`.
3. `Pause consumers` → producers block (`ST_BUF_WAIT_P` rises); `Resume consumers` → producers resume.
4. `Race Test` → result card: one BOOKED, two WAITING, double booking PREVENTED.
5. `- Cancellation` on a full train → head of waiting list auto-promoted.
6. `Demo Scenario` runs all ten steps automatically.
7. Point at Sync Monitor: two independent mutexes; buffer sync never blocks seat processing.
