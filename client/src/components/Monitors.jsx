import { useEffect, useRef } from 'react';
import { LOG_COLORS, LOG_DOT } from '../models/constants.js';

const stateColor = (s) =>
  s === 'RUNNING' ? 'text-emerald-300' : s === 'WAITING' ? 'text-amber-300' :
  s === 'BLOCKED' ? 'text-red-300' : s === 'PROCESSING' ? 'text-sky-300' :
  s === 'IDLE' ? 'text-slate-400' : 'text-slate-500';

export function ThreadMonitor({ snap }) {
  return (
    <div className="card">
      <h2>Thread Monitor</h2>
      <div className="font-mono text-xs space-y-1 max-h-56 overflow-auto">
        {(snap.threads || []).map((t) => (
          <div key={t.name} className="flex justify-between bg-slate-900/60 rounded px-2 py-1">
            <span className="text-slate-200">{t.name}</span>
            <span className={stateColor(t.state)}>{t.state}{t.detail ? ` — ${t.detail}` : ''}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

export function LockMonitor({ snap }) {
  const locks = snap.locks || {};
  const row = (label, key) => {
    const l = locks[key] || {};
    return (
      <div className="bg-slate-900/60 rounded px-2 py-1.5 font-mono text-xs">
        <div className="flex justify-between">
          <span className="text-slate-300 font-bold">{label}</span>
          <span className={l.owner ? 'text-red-300' : 'text-emerald-300'}>{l.owner ? `LOCKED by ${l.owner}` : 'FREE'}</span>
        </div>
        {(l.waiters || []).length > 0 && (
          <div className="text-amber-300 mt-1">waiting: {(l.waiters || []).join(', ')}</div>
        )}
      </div>
    );
  };
  return (
    <div className="card">
      <h2>Synchronization Monitor</h2>
      <div className="space-y-2">
        {row('BUFFER MUTEX (Producer↔Buffer↔Consumer)', 'buffer')}
        {row('RESERVATION MUTEX (Seats + Waiting list)', 'reservation')}
      </div>
      <p className="text-[11px] text-slate-500 mt-2">Two independent critical sections — buffer sync never blocks seat processing.</p>
    </div>
  );
}

export function EventLog({ snap }) {
  const ref = useRef(null);
  const events = snap.events || [];
  useEffect(() => {
    const el = ref.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [events.length]);
  return (
    <div className="card">
      <h2>Event / Processing Log ({events.length})</h2>
      <div ref={ref} className="log-scroll font-mono text-[11px] leading-relaxed h-72 overflow-auto bg-slate-950/70 border border-slate-800 rounded-lg p-2">
        {events.map((e) => (
          <div key={e.seq} className="flex gap-2 py-px">
            <span className="text-slate-500 shrink-0">[{e.time}]</span>
            <span className={`inline-block w-2 h-2 rounded-full mt-1 shrink-0 ${LOG_DOT[e.level] || 'bg-slate-500'}`} />
            <span className={LOG_COLORS[e.level] || 'text-slate-300'}>
              {e.thread && <b>{e.thread} </b>}{e.msg}
            </span>
          </div>
        ))}
        {events.length === 0 && <div className="text-slate-500">Press Start — worker threads will log here.</div>}
      </div>
    </div>
  );
}

export function StatsView({ snap }) {
  const s = snap.stats;
  if (!s) return <div className="card"><h2>Statistics</h2><div className="text-xs text-slate-500">No data yet.</div></div>;
  const cell = (label, value, accent) => (
    <div className="bg-slate-900/70 border border-slate-800 rounded-lg px-2 py-1.5 text-center">
      <div className={`text-base font-bold font-mono ${accent || 'text-slate-100'}`}>{value}</div>
      <div className="text-[9px] tracking-wider text-slate-400 uppercase">{label}</div>
    </div>
  );
  return (
    <div className="card">
      <h2>Statistics</h2>
      <div className="grid grid-cols-3 sm:grid-cols-5 gap-1.5">
        {cell('Total reqs', s.totalRequests)}{cell('Processed', s.processed, 'text-sky-300')}
        {cell('Confirmed', s.confirmed, 'text-emerald-300')}{cell('Waiting', s.waitingPassengers, 'text-amber-300')}
        {cell('Promoted', s.promotions, 'text-teal-300')}{cell('Cancels OK', s.cancellationsOk)}
        {cell('Failed', s.failed, 'text-red-300')}{cell('Prod waits', s.bufferWaitProducers, 'text-orange-300')}
        {cell('Cons waits', s.bufferWaitConsumers, 'text-orange-300')}{cell('Lock contention', s.lockContention, 'text-violet-300')}
        {cell('Race tests', s.raceTests, 'text-fuchsia-300')}{cell('Avg ms', s.avgProcessingMs)}
      </div>
    </div>
  );
}
