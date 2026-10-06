export function Controls({ snap, cmd }) {
  const running = snap.simState === 'RUNNING';
  const paused = snap.simState === 'PAUSED';
  const stopped = snap.simState === 'STOPPED';
  const btn = (path, body) => cmd(path, body);
  return (
    <div className="card">
      <h2>Simulation Controls</h2>
      <div className="flex flex-wrap gap-2">
        <button className="btn btn-primary" disabled={!stopped && running} onClick={() => btn('/api/start')}>Start</button>
        <button className="btn" disabled={!running} onClick={() => btn('/api/pause')}>Pause</button>
        <button className="btn" disabled={!paused} onClick={() => btn('/api/resume')}>Resume</button>
        <button className="btn btn-danger" disabled={stopped} onClick={() => btn('/api/stop')}>Stop</button>
        <button className="btn" onClick={() => btn('/api/reset', {})}>Reset</button>
      </div>
      <div className="flex flex-wrap gap-2 mt-2">
        <button className="btn btn-green" disabled={stopped} onClick={() => btn('/api/book', {})}>+ Booking</button>
        <button className="btn btn-warn" disabled={stopped} onClick={() => btn('/api/cancel', {})}>- Cancellation</button>
        <button className="btn !border-fuchsia-500 !bg-fuchsia-700 text-white" disabled={stopped} onClick={() => btn('/api/race', { seat: 10, count: 3 })}>Race Test</button>
        <button className="btn !border-cyan-500 !bg-cyan-700 text-white" disabled={stopped} onClick={() => btn('/api/demo')}>Demo Scenario</button>
      </div>
      <div className="flex flex-wrap gap-2 mt-2 text-xs">
        <button className="btn" disabled={stopped} onClick={() => btn('/api/pause-consumers', { value: !snap.consumersPaused })}>
          {snap.consumersPaused ? 'Resume consumers' : 'Pause consumers'}
        </button>
        <button className="btn" disabled={stopped} onClick={() => btn('/api/pause-producers', { value: !snap.producersPaused })}>
          {snap.producersPaused ? 'Resume producers' : 'Pause producers'}
        </button>
        <button className="btn !border-orange-500" disabled={stopped} onClick={() => btn('/api/unsafe', { value: !snap.unsafe })}>
          {snap.unsafe ? 'Enable mutex' : 'Disable mutex'}
        </button>
      </div>
      {snap.demoRunning && <div className="mt-2 text-xs text-fuchsia-300 font-bold">Demo running… step {snap.demoStep}/10</div>}
      <ConfigForm snap={snap} cmd={cmd} />
    </div>
  );
}

function ConfigForm({ snap, cmd }) {
  const c = snap.config || {};
  const submit = (e) => {
    e.preventDefault();
    const f = new FormData(e.target);
    cmd('/api/config', {
      capacity: Number(f.get('capacity')), seats: Number(f.get('seats')),
      producers: Number(f.get('producers')), consumers: Number(f.get('consumers')),
      waitlistMax: Number(f.get('waitlistMax')),
    });
  };
  const dis = snap.simState !== 'STOPPED';
  const field = (label, name, v, min, max) => (
    <label key={name} className="flex flex-col text-[11px] text-slate-400">{label}
      <input name={name} type="number" min={min} max={max} defaultValue={v} disabled={dis}
        className="mt-1 w-20 px-2 py-1 rounded bg-slate-900 border border-slate-700 text-slate-100 font-mono" />
    </label>
  );
  return (
    <form onSubmit={submit} className="flex flex-wrap items-end gap-2 mt-3 pt-3 border-t border-slate-800">
      {field('Buffer', 'capacity', c.capacity, 1, 64)}
      {field('Seats', 'seats', c.seats, 4, 48)}
      {field('Producers', 'producers', c.producers, 1, 8)}
      {field('Consumers', 'consumers', c.consumers, 1, 8)}
      {field('WaitlistMax', 'waitlistMax', c.waitlistMax, 1, 100)}
      <button className="btn" disabled={dis} type="submit">Apply (when stopped)</button>
    </form>
  );
}
