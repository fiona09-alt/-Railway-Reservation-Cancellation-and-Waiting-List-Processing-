import { useEffect, useState } from 'react';

export function TestResults() {
  const [data, setData] = useState(null);
  useEffect(() => {
    fetch('/api/test-results').then((r) => r.json()).then(setData).catch(() => {});
  }, []);
  if (!data) return null;
  const pass = data.pass ?? 0; const fail = data.fail ?? 0;
  return (
    <div className="card !border-emerald-700">
      <h2 className="!text-emerald-300">Automated Tests — {pass}/{pass + fail} passed</h2>
      <div className="font-mono text-[11px] space-y-0.5 max-h-40 overflow-auto">
        {(data.tests || []).map((t) => (
          <div key={t} className={t.startsWith('ok') ? 'text-emerald-300' : 'text-red-300'}>{t}</div>
        ))}
      </div>
      <div className="text-[11px] text-slate-400 mt-1">Ran {data.at} · `npm test` reproduces locally</div>
    </div>
  );
}
