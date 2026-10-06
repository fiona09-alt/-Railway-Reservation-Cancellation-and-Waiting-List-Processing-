/** Mirrors server/src/models/enums.js for rendering. Keep in sync. */
export const REQ_STATUS_LABEL = ['GENERATED','IN_BUFFER','CONSUMED','PROCESSING','CONFIRMED','WAITING LIST','CANCELLED','PROMOTED','FAILED','REJECTED'];
export const LOG_COLORS = {
  INFO: 'text-sky-300', SUCCESS: 'text-emerald-300', WAITING: 'text-amber-300',
  LOCK: 'text-violet-300', UNLOCK: 'text-teal-300', WARNING: 'text-orange-300',
  ERROR: 'text-red-300', DEMO: 'text-fuchsia-300', SYSTEM: 'text-slate-300', TEST: 'text-cyan-300',
};
export const LOG_DOT = {
  INFO: 'bg-sky-400', SUCCESS: 'bg-emerald-400', WAITING: 'bg-amber-400',
  LOCK: 'bg-violet-400', UNLOCK: 'bg-teal-400', WARNING: 'bg-orange-400',
  ERROR: 'bg-red-400', DEMO: 'bg-fuchsia-400', SYSTEM: 'bg-slate-400', TEST: 'bg-cyan-400',
};
