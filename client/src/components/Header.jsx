export function Header({ snap, connected }) {
  const c = snap.config || {};
  const dot = snap.simState === 'RUNNING' ? 'bg-emerald-400' : snap.simState === 'PAUSED' ? 'bg-amber-400' : 'bg-slate-500';
  return (
    <header className="card">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-2xl font-extrabold tracking-tight">Railway Reservation</h1>
          <p className="text-sky-300 text-sm font-semibold tracking-wide">Producer-Consumer Synchronization Simulator</p>
          <p className="text-xs text-slate-400 mt-1">Real worker_threads + SharedArrayBuffer + Atomics mutex + condition variables</p>
        </div>
        <div className="flex items-center gap-2 text-xs font-mono">
          <span className={`inline-block w-2.5 h-2.5 rounded-full ${dot}`} />
          <span className="font-bold">{snap.simState}</span>
          <span className="text-slate-500">|</span>
          <span className={connected ? 'text-emerald-300' : 'text-red-300'}>{connected ? 'LIVE' : 'OFFLINE'}</span>
        </div>
      </div>
      <div className="grid grid-cols-2 sm:grid-cols-5 gap-2 mt-4 text-center text-xs font-mono">
        {[['Seats', c.seats], ['Buffer', c.capacity], ['Producers', c.producers], ['Consumers', c.consumers], ['Waitlist max', c.waitlistMax]].map(([k, v]) => (
          <div key={k} className="bg-slate-900/70 border border-slate-800 rounded-lg py-1.5">
            <span className="text-slate-400">{k}: </span><span className="font-bold text-sky-300">{v}</span>
          </div>
        ))}
      </div>
      {snap.unsafe && <div className="mt-2 text-xs font-bold text-orange-300">UNSAFE MODE — reservation mutex disabled (educational race demo)</div>}
    </header>
  );
}
