/**
 * Simulation configuration defaults, hard limits and validation.
 * Limits must stay in sync with the SharedArrayBuffer layout (sync/SharedState.js).
 */

export const DEFAULT_CONFIG = {
  capacity: 5, // bounded buffer capacity (slots)
  seats: 12, // number of seats in the train
  producers: 3, // producer worker threads
  consumers: 3, // consumer worker threads
  waitlistMax: 50, // maximum waiting-list length
  randomGeneration: true, // producers keep generating background traffic
  trainNo: 1001,
  trainName: 'EXPRESS-101',
};

export const CONFIG_LIMITS = {
  capacity: [1, 64],
  seats: [4, 48],
  producers: [1, 8],
  consumers: [1, 8],
  waitlistMax: [1, 100],
};

export const MAX_EVENT_HISTORY = 400;
export const MAX_LEDGER_ENTRIES = 300;
export const SNAPSHOT_INTERVAL_MS = 250;
export const WORKER_EXIT_TIMEOUT_MS = 5000;
export const DEFAULT_PORT = 4000;

/**
 * Validate a partial configuration object.
 * @returns {{ok: boolean, errors: string[], config: object}}
 */
export function validateConfig(input = {}) {
  const errors = [];
  const config = { ...DEFAULT_CONFIG, ...input };

  for (const [key, [min, max]] of Object.entries(CONFIG_LIMITS)) {
    const value = Number(config[key]);
    if (!Number.isInteger(value) || value < min || value > max) {
      errors.push(`${key} must be an integer between ${min} and ${max} (received: ${input[key]}).`);
    } else {
      config[key] = value;
    }
  }

  config.randomGeneration = Boolean(config.randomGeneration);
  config.trainNo = Number.isFinite(Number(config.trainNo)) ? Number(config.trainNo) : DEFAULT_CONFIG.trainNo;
  config.trainName = String(config.trainName || DEFAULT_CONFIG.trainName).slice(0, 24);

  return { ok: errors.length === 0, errors, config };
}
