import { REQ_TYPE, SEAT_STATUS } from '../models/enums.js';

/**
 * applyReservation — THE RESERVATION MUTEX CRITICAL SECTION.
 *
 * This module is the only place that mutates the seat table or the waiting
 * list. The consumer worker holds `state.resMutex` around every call, which
 * guarantees:
 *
 *   1. Two consumers can never confirm the same seat (no double booking).
 *   2. "Seat occupied?" + "write booking" is one atomic check-and-act —
 *      the classic race condition is impossible while the mutex is held.
 *   3. Cancel → release seat → promote first waiting passenger happens
 *      atomically, so no other thread can steal the freed seat in between.
 *
 * UNSAFE/EDUCATIONAL MODE: when `opts.observe` is provided, the caller runs
 * WITHOUT the mutex and the observe callback opens a deliberate observation
 * window between the status check and the write, exposing the race that the
 * mutex normally prevents. Unsafe mode is opt-in from the UI.
 */

export const OUTCOME = {
  CONFIRMED: 'CONFIRMED',
  WAITING: 'WAITING_LIST',
  CANCELLED: 'CANCELLED',
  REJECTED: 'REJECTED',
};

export function applyReservation(state, rec, opts = {}) {
  const observe = opts.observe || null;
  if (rec.type === REQ_TYPE.BOOK) return bookSeat(state, rec, observe);
  return cancelSeat(state, rec, observe);
}

// ---------------------------------------------------------------------------
// BOOK
// ---------------------------------------------------------------------------
function bookSeat(state, rec, observe) {
  const now = Date.now();
  const seatIdx = rec.seat - 1;

  if (!Number.isInteger(rec.seat) || rec.seat < 1 || rec.seat > state.seatCount()) {
    state.addStat('ST_FAILED');
    state.addStat('ST_ERRORS');
    return {
      outcome: OUTCOME.REJECTED,
      reason: 'INVALID_SEAT',
      seat: rec.seat,
      message: `Invalid seat number ${rec.seat} (valid range 1..${state.seatCount()})`,
    };
  }

  // Duplicate passenger booking guard (inside the critical section).
  const existing = state.seatOfPassenger(rec.passengerId);
  if (existing >= 0) {
    state.addStat('ST_DUP');
    state.addStat('ST_FAILED');
    state.addStat('ST_ERRORS');
    return {
      outcome: OUTCOME.REJECTED,
      reason: 'DUPLICATE_BOOKING',
      seat: rec.seat,
      message: `Duplicate booking rejected — P-${rec.passengerId} already holds seat ${existing + 1}`,
    };
  }

  // CHECK (inside the mutex in safe mode).
  observe?.('before-check');
  const free = state.seatStatus(seatIdx) === SEAT_STATUS.AVAILABLE;
  // UNSAFE MODE ONLY: observation window between check and act — the
  // interleaving that makes double booking possible without a mutex.
  observe?.('write-gap');

  if (!free) {
    const pushed = state.wlPush({
      passengerId: rec.passengerId,
      requestId: rec.reqNum,
      time: now,
      targetSeat: rec.seat,
      producer: rec.producer,
      seq: now,
      age: rec.age,
      gender: rec.gender,
      passengerName: rec.passengerName,
    });
    if (!pushed) {
      state.addStat('ST_WL_FULL');
      state.addStat('ST_FAILED');
      state.addStat('ST_ERRORS');
      return {
        outcome: OUTCOME.REJECTED,
        reason: 'WAITLIST_FULL',
        seat: rec.seat,
        message: `Waiting list is full (max ${state.waitlistMax()}) — booking rejected`,
      };
    }
    return {
      outcome: OUTCOME.WAITING,
      seat: rec.seat,
      position: state.wlCount(),
      seatStateAtCheck: 'OCCUPIED',
      message: `Seat ${rec.seat} already occupied — P-${rec.passengerId} added to waiting list (position ${state.wlCount()})`,
    };
  }

  // ACT — atomic with the check while the mutex is held.
  state.seatBook(seatIdx, {
    passengerId: rec.passengerId,
    requestId: rec.reqNum,
    age: rec.age,
    gender: rec.gender,
    trainNo: rec.trainNo,
    name: rec.passengerName,
    now,
  });
  state.addStat('ST_CONFIRMED');
  return {
    outcome: OUTCOME.CONFIRMED,
    seat: rec.seat,
    seatStateAtCheck: 'AVAILABLE',
    message: `Seat ${rec.seat} booked for P-${rec.passengerId} (${rec.passengerName})`,
  };
}

// ---------------------------------------------------------------------------
// CANCEL (+ atomic waiting-list promotion)
// ---------------------------------------------------------------------------
function cancelSeat(state, rec, observe) {
  const seatIdx = rec.seat - 1;

  if (!Number.isInteger(rec.seat) || rec.seat < 1 || rec.seat > state.seatCount()) {
    state.addStat('ST_FAILED');
    state.addStat('ST_ERRORS');
    return {
      outcome: OUTCOME.REJECTED,
      reason: 'INVALID_SEAT',
      seat: rec.seat,
      message: `Invalid seat number ${rec.seat} (valid range 1..${state.seatCount()})`,
    };
  }

  const holdsSeat = state.seatStatus(seatIdx) === SEAT_STATUS.BOOKED
    && state.seatOccupant(seatIdx) === rec.passengerId;

  // Passenger is on the waiting list (and does not hold the seat) → withdraw.
  const wlIdx = state.wlFind(rec.passengerId);
  if (wlIdx >= 0 && !holdsSeat) {
    const removed = state.wlRemoveAt(wlIdx);
    return {
      outcome: OUTCOME.CANCELLED,
      seat: rec.seat,
      withdrewFromWaitlist: true,
      position: removed.position,
      message: `P-${rec.passengerId} (${removed.passengerName}) withdrawn from waiting list (was position ${removed.position})`,
    };
  }

  observe?.('before-check');
  const seatFree = state.seatStatus(seatIdx) === SEAT_STATUS.AVAILABLE;
  observe?.('write-gap');

  if (seatFree) {
    state.addStat('ST_FAILED');
    state.addStat('ST_ERRORS');
    return {
      outcome: OUTCOME.REJECTED,
      reason: 'SEAT_NOT_BOOKED',
      seat: rec.seat,
      message: `Cancellation rejected — seat ${rec.seat} is not booked (duplicate or invalid cancellation)`,
    };
  }
  if (state.seatOccupant(seatIdx) !== rec.passengerId) {
    state.addStat('ST_FAILED');
    state.addStat('ST_ERRORS');
    const owner = state.readSeat(seatIdx);
    return {
      outcome: OUTCOME.REJECTED,
      reason: 'NOT_OWNER',
      seat: rec.seat,
      message: `Cancellation rejected — seat ${rec.seat} belongs to ${owner.passengerName}, not P-${rec.passengerId}`,
    };
  }

  // RELEASE + PROMOTE — atomically inside the same critical section, so no
  // other consumer can grab the freed seat before the waiting passenger gets it.
  const previous = state.readSeat(seatIdx);
  state.seatRelease(seatIdx);
  state.addStat('ST_CANCEL_OK');

  let promoted = null;
  if (state.wlCount() > 0) {
    const first = state.wlShift(); // FIFO: first come → first served
    state.seatBook(seatIdx, {
      passengerId: first.passengerId,
      requestId: first.requestId,
      age: first.age,
      gender: first.gender,
      trainNo: rec.trainNo,
      name: first.passengerName,
      now: Date.now(),
    });
    state.addStat('ST_PROMOTED');
    state.addStat('ST_CONFIRMED');
    promoted = {
      passengerId: first.passengerId,
      requestId: first.requestId,
      name: first.passengerName,
      seat: seatIdx + 1,
      previousPosition: 1,
    };
  }

  return {
    outcome: OUTCOME.CANCELLED,
    seat: seatIdx + 1,
    previousOccupant: { passengerId: previous.passengerId, name: previous.passengerName },
    promoted,
    message: `Seat ${seatIdx + 1} cancelled by P-${rec.passengerId} (${previous.passengerName})`,
  };
}

