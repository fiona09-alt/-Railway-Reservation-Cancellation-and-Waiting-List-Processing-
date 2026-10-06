import { useEffect, useRef, useState, useCallback } from 'react';

const initial = {
  simState: 'STOPPED', unsafe: false, producersPaused: false, consumersPaused: false,
  config: { capacity: 5, seats: 12, producers: 3, consumers: 3 },
  stats: null, seats: [], waitlist: [], buffer: [], bufferCapacity: 5, bufferUsed: 0,
  threads: [], locks: { buffer: {}, reservation: {} }, requests: [], events: [],
  lastRaceResult: null, demoRunning: false, demoStep: 0,
};

export function useSimulation() {
  const [snap, setSnap] = useState(initial);
  const [connected, setConnected] = useState(false);
  const wsRef = useRef(null);

  useEffect(() => {
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    const url = `${proto}://${location.host}/ws`;
    let ws; let retry;
    const connect = () => {
      ws = new WebSocket(url);
      wsRef.current = ws;
      ws.onopen = () => setConnected(true);
      ws.onclose = () => { setConnected(false); retry = setTimeout(connect, 1500); };
      ws.onerror = () => ws.close();
      ws.onmessage = (ev) => {
        try {
          const msg = JSON.parse(ev.data);
          if (msg.t === 'snapshot') setSnap(msg.snapshot);
        } catch { /* ignore */ }
      };
    };
    connect();
    fetch('/api/state').then((r) => r.json()).then((d) => d.snapshot && setSnap(d.snapshot)).catch(() => {});
    return () => { clearTimeout(retry); ws && ws.close(); };
  }, []);

  const cmd = useCallback(async (path, body) => {
    const r = await fetch(path, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: body ? JSON.stringify(body) : '{}',
    });
    const d = await r.json().catch(() => ({}));
    if (d.snapshot) setSnap(d.snapshot);
    return d;
  }, []);

  return { snap, connected, cmd };
}
