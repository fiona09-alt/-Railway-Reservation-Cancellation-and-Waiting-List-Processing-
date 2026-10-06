export function BufferView({ snap }) {
  const cap = snap.bufferCapacity || 0;
  const used = snap.bufferUsed || 0;
  const full = used >= cap && cap > 0;
  const empty = used === 0;
  return (
    <div className="card">
      <h2>Request Buffer — Bounded ({used}/{cap})</h2>
      <div className={`text-xs font-bold mb-2 ${full ? 'text-red-300' : empty ? 'text-amber-300' : 'text-emerald-300'}`}>
        STATUS: {full ? 'BUFFER FULL — producers WAIT' : empty ? 'BUFFER EMPTY — consumers WAIT' : 'ACTIVE'}
      </div>
      <div className="border border-slate-700 rounded-lg overflow-hidden font-mono text-xs">
        {(snap.buffer || []).map((r, i) => (
          <div key={r.reqNum} className={`px-3 py-1.5 flex justify-between ${i % 2 ? 'bg-slate-900/60' : 'bg-slate-800/60'}`}>
            <span className={r.typeLabel === 'BOOK' ? 'text-sky-300' : 'text-amber-300'}>{r.typeLabel} · {r.id}</span>
            <span className="text-slate-400">P-{r.passengerId} {r.passengerName} → seat {r.seat}</span>
          </div>
        ))}
        {empty && <div className="px-3 py-3 text-slate-500 text-center">— empty — consumers waiting on notEmpty —</div>}
      </div>
    </div>
  );
}

export function SeatMap({ snap }) {
  const seats = snap.seats || [];
  return (
    <div className="card">
      <h2>Train Seat Map</h2>
      <div className="grid grid-cols-4 sm:grid-cols-6 gap-2">
        {seats.map((s) => (
          <div key={s.number} title={s.status === 'BOOKED' ? `Seat ${s.number} — ${s.passengerName} (P-${s.passengerId})` : `Seat ${s.number} — AVAILABLE`}
            className={`rounded-lg border p-2 text-center font-mono ${s.status === 'BOOKED' ? 'border-red-500/60 bg-red-950/40' : 'border-emerald-600/50 bg-emerald-950/30'}`}>
            <div className="text-sm font-bold">{String(s.number).padStart(2, '0')}</div>
            <div className="text-lg">{s.status === 'BOOKED' ? '🔴' : '🟢'}</div>
            <div className="text-[10px] truncate text-slate-300">{s.status === 'BOOKED' ? s.passengerName : 'free'}</div>
          </div>
        ))}
      </div>
      <div className="text-[11px] text-slate-400 mt-2">🟢 AVAILABLE &nbsp; 🔴 BOOKED &nbsp; (hover a seat for occupant)</div>
    </div>
  );
}

export function WaitlistView({ snap }) {
  const wl = snap.waitlist || [];
  return (
    <div className="card">
      <h2>Waiting List — FIFO ({wl.length})</h2>
      {wl.length === 0 && <div className="text-xs text-slate-500">Empty — new bookings confirm immediately.</div>}
      <div className="font-mono text-xs space-y-1">
        {wl.map((w) => (
          <div key={`${w.requestId}-${w.position}`} className="flex justify-between bg-slate-900/70 border border-slate-800 rounded px-2 py-1">
            <span className="text-amber-300">#{w.position} {w.passengerName}</span>
            <span className="text-slate-400">P-{w.passengerId} · REQ-{String(w.requestId).padStart(3, '0')} → seat {w.targetSeat}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
