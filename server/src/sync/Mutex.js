/**
 * Mutex — a REAL mutual-exclusion lock shared between OS threads.
 *
 * Built on SharedArrayBuffer + Atomics.compareExchange (atomic test-and-set)
 * with Atomics.wait/notify as the futex-style blocking mechanism. Threads that
 * lose the race for the lock are genuinely descheduled by the OS inside
 * Atomics.wait — they do not spin the CPU and they do not fake waiting with
 * timers.
 *
 * Lock word values: 0 = free, 1 = held.
 */
export class Mutex {
  /**
   * @param {Int32Array} i32 shared array that holds the lock word
   * @param {number} index index of the lock word inside the array
   * @param {string} [name] label used in instrumentation messages
   */
  constructor(i32, index, name = 'mutex') {
    this.i32 = i32;
    this.index = index;
    this.name = name;
  }

  /** Non-blocking attempt. Used to detect lock contention for instrumentation. */
  tryLock() {
    return Atomics.compareExchange(this.i32, this.index, 0, 1) === 0;
  }

  /**
   * Blocking acquire.
   * @returns {boolean} true when the lock was contended (another thread held it)
   */
  lock() {
    if (this.tryLock()) return false; // fast path: uncontended
    let contended = true;
    // Slow path: block on the futex word while it still reads 1 (held).
    // The bounded timeout is a safety net so a thread can always re-check the
    // lock word; correctness comes from the compareExchange retry loop.
    while (!this.tryLock()) {
      Atomics.wait(this.i32, this.index, 1, 10);
    }
    return contended;
  }

  /** Release the lock and wake one thread blocked in lock(). */
  unlock() {
    Atomics.store(this.i32, this.index, 0);
    Atomics.notify(this.i32, this.index, 1);
  }

  /** Snapshot helper for the synchronization monitor. */
  isLocked() {
    return Atomics.load(this.i32, this.index) === 1;
  }
}
