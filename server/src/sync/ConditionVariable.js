import { Mutex } from './Mutex.js';

/**
 * ConditionVariable — Mesa-style condition variable for the producer-consumer
 * protocol, built on a sequence counter inside the SharedArrayBuffer.
 *
 * wait(mutex) atomically releases the mutex and blocks the calling OS thread
 * via Atomics.wait. notifyOne()/notifyAll() bump the sequence counter and call
 * Atomics.notify, which wakes blocked threads.
 *
 * Because the sequence is captured BEFORE the mutex is released, a notify that
 * arrives in that window changes the sequence and the subsequent wait()
 * returns immediately — no wakeup can be lost. Callers must always re-check
 * their predicate in a `while` loop (Mesa semantics), because wakeups may be
 * spurious or stolen by other threads.
 */
export class ConditionVariable {
  /**
   * @param {Int32Array} i32 shared array
   * @param {number} seqIndex index of the sequence counter
   * @param {number} waitersIndex index of the waiter-count counter (instrumentation)
   * @param {string} [name]
   */
  constructor(i32, seqIndex, waitersIndex, name = 'condvar') {
    this.i32 = i32;
    this.seqIndex = seqIndex;
    this.waitersIndex = waitersIndex;
    this.name = name;
  }

  /**
   * Block until notified. MUST be called while holding `mutex`; the mutex is
   * released before blocking and re-acquired before returning.
   * @param {Mutex} mutex
   * @param {number} [timeoutMs] upper bound for one wait slice (re-check safety)
   */
  wait(mutex, timeoutMs = 500) {
    const seq = Atomics.load(this.i32, this.seqIndex);
    Atomics.add(this.i32, this.waitersIndex, 1);
    // CRITICAL SECTION BOUNDARY: release the buffer mutex so producers/consumers
    // can interleave while this thread sleeps.
    mutex.unlock();
    try {
      // Blocks the OS thread while the sequence still equals `seq`.
      Atomics.wait(this.i32, this.seqIndex, seq, timeoutMs);
    } finally {
      Atomics.add(this.i32, this.waitersIndex, -1);
      // Re-enter the critical section before returning (Mesa-style).
      mutex.lock();
    }
  }

  /** Wake the thread at the head of the wait queue, if any. */
  notifyOne() {
    Atomics.add(this.i32, this.seqIndex, 1);
    Atomics.notify(this.i32, this.seqIndex, 1);
  }

  /** Wake every thread blocked on this condition (used on stop/reset/resume). */
  notifyAll() {
    Atomics.add(this.i32, this.seqIndex, 1);
    Atomics.notify(this.i32, this.seqIndex, 4096);
  }

  /** Instrumentation: how many threads are currently blocked on this condition. */
  waiters() {
    return Atomics.load(this.i32, this.waitersIndex);
  }
}

export { Mutex };
