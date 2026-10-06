import { REQ_TYPE, REQ_STATUS, GENDER } from './enums.js';

/**
 * RequestFactory — builds request records for BOOK / CANCEL traffic.
 * Request numbers and passenger ids come from atomic sequences inside the
 * SharedArrayBuffer, so ids stay unique even when three producer threads
 * create requests at the same instant.
 */

const NAMES = [
  'Atharva', 'Rohan', 'Sneha', 'Rahul', 'Priya', 'Aman', 'Vikram', 'Diya',
  'Ishaan', 'Meera', 'Karan', 'Ananya', 'Zoya', 'Dev', 'Neha', 'Arjun',
  'Tara', 'Kabir', 'Sana', 'Ravi',
];

export class RequestFactory {
  constructor(state, config) {
    this.state = state;
    this.config = config;
    this.cursor = Math.floor(Math.random() * NAMES.length);
    this.seq = 0; // local round-robin over the name pool
  }

  nextName() {
    this.cursor = (this.cursor + 1) % NAMES.length;
    return NAMES[this.cursor];
  }

  /**
   * Build a complete request record.
   * @param {{type:number, seat?:number, name?:string, passengerId?:number,
   *          age?:number, gender?:number, producer?:number}} spec
   */
  make(spec) {
    const reqNum = this.state.nextReqNum();
    const passengerId = spec.passengerId || this.state.nextPassengerId();
    return {
      id: `REQ-${String(reqNum).padStart(3, '0')}`,
      reqNum,
      type: spec.type,
      passengerId,
      passengerName: (spec.name || this.nextName()).slice(0, 20),
      seat: spec.seat ?? 0,
      producer: spec.producer ?? 0,
      status: REQ_STATUS.GENERATED,
      created: Date.now(),
      trainNo: this.config.trainNo,
      age: spec.age ?? 18 + Math.floor(Math.random() * 45),
      gender: spec.gender ?? (Math.random() < 0.5 ? GENDER.MALE : GENDER.FEMALE),
    };
  }

  /**
   * Background traffic for a producer thread (~75% BOOK, 25% CANCEL).
   * CANCEL targets are taken from the engine's "booked seat hints" so random
   * cancellations usually point at a real booking (stale hints deliberately
   * exercise the error-handling paths in the consumer).
   */
  randomRequest(producer, hints = []) {
    const seats = this.config.seats;
    const tryCancel = hints.length > 0 && Math.random() < 0.25;
    if (tryCancel) {
      const hint = hints[Math.floor(Math.random() * hints.length)];
      return this.make({
        type: REQ_TYPE.CANCEL,
        seat: hint.seat,
        name: hint.passengerName,
        passengerId: hint.passengerId,
        producer,
      });
    }
    return this.make({
      type: REQ_TYPE.BOOK,
      seat: 1 + Math.floor(Math.random() * seats),
      producer,
    });
  }
}
