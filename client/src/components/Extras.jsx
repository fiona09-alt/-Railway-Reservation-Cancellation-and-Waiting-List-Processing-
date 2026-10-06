export function RaceResult({ snap }) {
  const r = snap.lastRaceResult;
  if (!r) return null;
  return (
    <div className="card !border-fuchsia-700">
      <h2 className="!text-fuchsia-300">Race Condition Test — Seat {r.seat}</h2>
      <div className="text-xs font-mono space-y-1">
        {(r.booked || []).map((b) => (
          <div key={b.passengerId} className="text-emerald-300">✓ {b.passengerName} (P-{b.passengerId}) — BOOKED</div>
        ))}
        {(r.waiting || []).map((w, i) => (
          <div key={w.passengerId} className="text-amber-300">{i + 1}. {w.passengerName} (P-{w.passengerId}) — WAITING</div>
        ))}
        <div className={r.doubleBookingPrevented ? 'text-emerald-300 font-bold' : 'text-red-300 font-bold'}>
          Double booking: {r.doubleBookingPrevented ? 'PREVENTED ✓' : 'DETECTED ✗'} · Mutex: {r.mutexProtection}
        </div>
      </div>
    </div>
  );
}

export function EduPanel() {
  return (
    <div className="card">
      <h2>How Producer–Consumer Works</h2>
      <pre className="font-mono text-[11px] text-slate-300 bg-slate-950/70 rounded-lg p-3 overflow-auto">{`PRODUCERS → generate BOOK/CANCEL
        ↓
  ┌─────────────────┐  full → producers WAIT (notFull)
  │  BOUNDED BUFFER │  empty → consumers WAIT (notEmpty)
  └─────────────────┘
        ↓
CONSUMERS → take request → RESERVATION MUTEX → seats/waitlist`}</pre>
      <ul className="text-[11px] text-slate-400 mt-2 space-y-1 list-disc pl-4">
        <li>Buffer full → producers block · buffer empty → consumers block.</li>
        <li>Reservation mutex makes check-and-book atomic — no double booking.</li>
        <li>Waiting list is FIFO; cancellation atomically promotes the head.</li>
        <li>Buffer mutex and reservation mutex are independent locks.</li>
      </ul>
    </div>
  );
}

export function RequestTable({ snap }) {
  const reqs = snap.requests || [];
  return (
    <div className="card">
      <h2>Requests (latest {reqs.length})</h2>
      <div className="overflow-auto max-h-56 font-mono text-[11px]">
        <table className="w-full">
          <thead className="text-slate-400 text-left">
            <tr><th className="pr-2">ID</th><th className="pr-2">Type</th><th className="pr-2">Passenger</th><th className="pr-2">Seat</th><th className="pr-2">Producer</th><th>Consumer / Status</th></tr>
          </thead>
          <tbody>
            {reqs.map((r) => (
              <tr key={r.reqNum} className="border-t border-slate-800">
                <td className="pr-2 text-slate-300">{r.id}</td>
                <td className={`pr-2 ${r.typeLabel === 'BOOK' ? 'text-sky-300' : 'text-amber-300'}`}>{r.typeLabel}</td>
                <td className="pr-2 text-slate-300">{r.passengerName} (P-{r.passengerId})</td>
                <td className="pr-2 text-slate-300">{r.seat}</td>
                <td className="pr-2 text-slate-400">{r.producer || '-'}</td>
                <td className="text-slate-400">{['CONFIRMED','WAITING LIST','CANCELLED','PROMOTED','FAILED','REJECTED'][r.status - 4] || (r.consumer ? `${r.consumer}` : '…')}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {reqs.length === 0 && <div className="text-slate-500 py-2">No requests yet.</div>}
      </div>
    </div>
  );
}
