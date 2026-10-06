import { H } from '../sync/SharedState.js';
import { SIM_STATE } from '../models/enums.js';

/**
 * Helpers shared by producer/consumer worker threads.
 * All functions here block the calling OS thread with Atomics.wait — they are
 * real thread-blocking primitives, not event-loop timers.
 */

export function stopped(state) {
  return Atomics.load(state.i32, H.STOP) === 1;
}

export function simPaused(state) {
  return Atomics.load(state.i32, H.SIM_ST) === SIM_STATE.PAUSED;
}

export function rolePaused(state, role) {
  const idx = role === 'prod' ? H.PAUSE_PROD : H.PAUSE_CONS;
  return Atomics.load(state.i32, idx) === 1;
}

export function paused(state, role) {
  return simPaused(state) || rolePaused(state, role);
}

/**
 * Block the worker thread while the simulation (or its role) is paused.
 * Waits on the actual control words so resume/stop notifications wake it up;
 * a bounded timeout guarantees a re-check even if a notify was missed.
 */
export function controlWait(state, role) {
  const roleIdx = role === 'prod' ? H.PAUSE_PROD : H.PAUSE_CONS;
  while (!stopped(state)) {
    if (simPaused(state)) {
      Atomics.wait(state.i32, H.SIM_ST, SIM_STATE.PAUSED, 100);
    } else if (rolePaused(state, role)) {
      Atomics.wait(state.i32, roleIdx, 1, 100);
    } else {
      return; // resumed
    }
  }
}

/**
 * Pacing sleep between generated requests (arrival rate modelling ONLY —
 * it is never used to emulate synchronization). Wakes early on pause/stop.
 */
export function sleepPaced(state, ms) {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    if (stopped(state) || simPaused(state)) return;
    const current = Atomics.load(state.i32, H.SIM_ST);
    const remain = Math.min(30, deadline - Date.now());
    if (remain <= 0) break;
    Atomics.wait(state.i32, H.SIM_ST, current, remain);
  }
}

/** Observation window used ONLY in the educational unsafe mode to expose the race. */
export function raceWindow(state, ms = 2) {
  if (stopped(state)) return;
  Atomics.wait(state.i32, H.SIM_ST, Atomics.load(state.i32, H.SIM_ST), ms);
}
