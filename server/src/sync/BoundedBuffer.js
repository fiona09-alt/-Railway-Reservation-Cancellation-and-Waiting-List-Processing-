import { H } from './SharedState.js';
import { LIMITS } from './SharedState.js';

/**
 * BoundedBuffer — the classic bounded request queue of the Producer–Consumer
 * problem, implemented over the SharedArrayBuffer with a REAL mutex and two
 * REAL condition variables.
 *
 * Invariants (all guarded by state.bufMutex):
 *   - 0 <= BUF_COUNT <= BUF_CAP
 *   - requests are removed in exactly the order they were inserted (FIFO)
 *
 * Blocking behaviour:
 *   - put()  blocks (Atomics.wait inside condvar.wait) while BUF_COUNT == BUF_CAP.
 *            The mutex is released while blocked so a consumer can drain.
 *   - take() blocks while BUF_COUNT == 0. The mutex is released while blocked
 *            so a producer can fill.
 *   - every successful put() signals notEmpty; every successful take() signals notFull.
 */
export class BoundedBuffer {
  constructor(state) {
    this.state = state;
  }

  get capacity() { return this.state.capacity(); }
  get count() { return this.state.count(); }
  get isEmpty() { return this.count === 0; }
  get isFull() { return this.count >= this.capacity; }

  /**
   * PRODUCER side. Blocks while the buffer is full.
   * @param {object} rec request record
   * @param {{onWait?: Function, onInserted?: Function}} hooks instrumentation callbacks
   * @returns {{ok?: boolean, aborted?: boolean}}
   */
  put(rec, hooks = {}) {
    const s = this.state;
    // CRITICAL SECTION START — buffer mutex protects BUF_COUNT/HEAD/TAIL.
    const contended = s.bufMutex.lock();
    hooks.onLock?.({ phase: contended ? 'contended-acquire' : 'acquire', contended });
    try {
      while (s.count() >= s.capacity()) {
        // Stop must never deadlock a blocked producer.
        if (s.isStoppedFlag()) return { aborted: true };
        // Tell the outside world we are genuinely blocked (once per blocking bout).
        hooks.onWait?.({ count: s.count(), capacity: s.capacity });
        // Releases bufMutex, blocks on notFull, re-acquires bufMutex on wake.
        s.notFull.wait(s.bufMutex);
      }
      const tail = Atomics.load(s.i32, H.BUF_TAIL);
      const slot = tail % s.capacity();
      s.writeSlot(slot, rec);
      Atomics.store(s.i32, H.BUF_COUNT, s.count() + 1);
      Atomics.store(s.i32, H.BUF_TAIL, tail + 1);
      hooks.onInserted?.({ slot, count: s.count(), index: tail });
      // Wake one consumer sleeping in take().
      s.notEmpty.notifyOne();
      return { ok: true, slot };
    } finally {
      // CRITICAL SECTION END
      s.bufMutex.unlock();
      hooks.onLock?.({ phase: 'release', contended: false });
    }
  }

  /**
   * CONSUMER side. Blocks while the buffer is empty.
   * @param {{onWait?: Function, onRemoved?: Function}} hooks
   * @returns {object|null} the request, or null when the simulation stopped with an empty buffer
   */
  take(hooks = {}) {
    const s = this.state;
    // CRITICAL SECTION START — same buffer mutex, opposite direction.
    const contended = s.bufMutex.lock();
    hooks.onLock?.({ phase: contended ? 'contended-acquire' : 'acquire', contended });
    try {
      // Block while EMPTY, or (unless stopping) while the caller must not
      // consume right now — e.g. the simulation is paused for a demo step.
      while (s.count() === 0 || (hooks.shouldAbort?.() ?? false)) {
        if (s.isStoppedFlag()) break;
        hooks.onWait?.({ capacity: s.capacity(), paused: hooks.shouldAbort?.() ?? false });
        // Releases bufMutex, blocks on notEmpty, re-acquires bufMutex on wake.
        s.notEmpty.wait(s.bufMutex);
      }
      if (s.count() === 0) return null; // stopped with an empty buffer
      const head = Atomics.load(s.i32, H.BUF_HEAD);
      const slot = head % s.capacity();
      const rec = s.readSlot(slot);
      s.clearSlot(slot);
      Atomics.store(s.i32, H.BUF_COUNT, s.count() - 1);
      Atomics.store(s.i32, H.BUF_HEAD, head + 1);
      hooks.onRemoved?.({ slot, count: s.count(), index: head });
      // Wake one producer sleeping in put().
      s.notFull.notifyOne();
      return rec;
    } finally {
      // CRITICAL SECTION END
      s.bufMutex.unlock();
      hooks.onLock?.({ phase: 'release', contended: false });
    }
  }

  /**
   * Non-blocking copy of the buffered requests, used by the UI snapshot.
   * If the mutex is currently held we simply report "locked" instead of
   * blocking the main thread — snapshots are advisory, not correctness-critical.
   */
  tryReadAll() {
    const s = this.state;
    if (!s.bufMutex.tryLock()) return null; // caller falls back to cached view
    try {
      const out = [];
      const cap = s.capacity();
      const count = s.count();
      const head = Atomics.load(s.i32, H.BUF_HEAD);
      for (let i = 0; i < count; i++) {
        out.push(s.readSlot((head + i) % cap));
      }
      return out;
    } finally {
      s.bufMutex.unlock();
    }
  }
}

export { LIMITS };
