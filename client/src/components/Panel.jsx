export function Panel({ title, children, right = null, className = '' }) {
  return (
    <section className={`card ${className}`}>
      <div className="flex items-center justify-between mb-3">
        <h2>{title}</h2>
        {right}
      </div>
      {children}
    </section>
  );
}

export function Stat({ label, value, accent = 'text-slate-100' }) {
  return (
    <div className="bg-slate-900/70 border border-slate-800 rounded-lg px-3 py-2 text-center">
      <div className={`text-lg font-bold font-mono ${accent}`}>{value}</div>
      <div className="text-[10px] tracking-wider text-slate-400 uppercase">{label}</div>
    </div>
  );
}
