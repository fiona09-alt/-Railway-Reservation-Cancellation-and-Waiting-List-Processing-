/**
 * Shared enumerations used across the simulation engine, worker threads and
 * tests. Mirrored client-side in client/src/models/constants.js (documented).
 */

/** Request types produced by producer threads. */
export const REQ_TYPE = { BOOK: 0, CANCEL: 1 };
export const REQ_TYPE_LABEL = ['BOOK', 'CANCEL'];

/** Lifecycle status of a request in the ledger. */
export const REQ_STATUS = {
  GENERATED: 0,
  IN_BUFFER: 1,
  CONSUMED: 2,
  PROCESSING: 3,
  CONFIRMED: 4,
  WAITING_LIST: 5,
  CANCELLED: 6,
  PROMOTED: 7,
  FAILED: 8,
  REJECTED: 9,
};

export const REQ_STATUS_LABEL = [
  'GENERATED',
  'IN_BUFFER',
  'CONSUMED',
  'PROCESSING',
  'CONFIRMED',
  'WAITING LIST',
  'CANCELLED',
  'PROMOTED',
  'FAILED',
  'REJECTED',
];

/** Statuses that will never change again. */
export const TERMINAL_STATUSES = new Set([
  REQ_STATUS.CONFIRMED,
  REQ_STATUS.WAITING_LIST,
  REQ_STATUS.CANCELLED,
  REQ_STATUS.FAILED,
  REQ_STATUS.REJECTED,
]);

/** Seat states in the shared seat table. */
export const SEAT_STATUS = { AVAILABLE: 0, BOOKED: 1 };
export const SEAT_STATUS_LABEL = ['AVAILABLE', 'BOOKED'];

/** Simulation run states (stored in SharedArrayBuffer). */
export const SIM_STATE = { STOPPED: 0, RUNNING: 1, PAUSED: 2 };
export const SIM_STATE_LABEL = ['STOPPED', 'RUNNING', 'PAUSED'];

/** Observable thread states for the thread monitor. */
export const THREAD_STATE = {
  RUNNING: 'RUNNING',
  WAITING: 'WAITING',
  BLOCKED: 'BLOCKED',
  PROCESSING: 'PROCESSING',
  IDLE: 'IDLE',
  STOPPED: 'STOPPED',
};

/** Log levels rendered with distinct visual indicators in the event log. */
export const LOG_LEVEL = {
  INFO: 'INFO',
  SUCCESS: 'SUCCESS',
  WAITING: 'WAITING',
  LOCK: 'LOCK',
  UNLOCK: 'UNLOCK',
  WARNING: 'WARNING',
  ERROR: 'ERROR',
  DEMO: 'DEMO',
  SYSTEM: 'SYSTEM',
  TEST: 'TEST',
};

export const GENDER = { UNKNOWN: 0, MALE: 1, FEMALE: 2 };
export const GENDER_LABEL = ['-', 'M', 'F'];

/** The two independent locks in the system (buffer sync vs reservation sync). */
export const LOCK = { BUFFER: 'buffer', RESERVATION: 'reservation' };
export const LOCK_LABEL = { buffer: 'BUFFER MUTEX', reservation: 'RESERVATION MUTEX' };
