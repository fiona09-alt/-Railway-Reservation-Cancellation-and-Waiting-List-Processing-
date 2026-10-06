/**
 * index.js — HTTP + WebSocket server hosting the multithreaded simulation.
 * REST: state snapshot + commands. WS /ws: live snapshot push (250ms).
 */
import express from 'express';
import { createServer } from 'node:http';
import { WebSocketServer } from 'ws';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { SimulationEngine } from './simulation/SimulationEngine.js';
import { DEFAULT_PORT, SNAPSHOT_INTERVAL_MS } from './config.js';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
const run = promisify(execFile);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT || DEFAULT_PORT);

const app = express();
app.use(express.json());
const engine = new SimulationEngine({});

function result(res, r) {
  if (r && r.ok === false) return res.status(400).json(r);
  return res.json({ ok: true, ...r, snapshot: engine.snapshot() });
}

let testCache = null; let testRunning = false;
async function testResults() {
  if (testCache) return testCache;
  if (testRunning) return { pass: 0, fail: 0, tests: ['tests already running…'], at: new Date().toISOString() };
  testRunning = true;
  try {
    const { stdout } = await run(process.execPath, ['--test', 'server/tests/buffer.test.js', 'server/tests/reservation.test.js', 'server/tests/engine.test.js'], { cwd: process.cwd(), timeout: 180000 });
    const lines = String(stdout).split('\n').filter((l) => /^(ok|not ok) \d+ - /.test(l.trim()));
    const pass = lines.filter((l) => l.trim().startsWith('ok')).length;
    testCache = { pass, fail: lines.length - pass, tests: lines.slice(0, 20), at: new Date().toISOString() };
  } catch (err) {
    const out = String(err.stdout || err.message);
    const lines = out.split('\n').filter((l) => /^(ok|not ok) \d+ - /.test(l.trim()));
    testCache = { pass: 0, fail: lines.length || 1, tests: lines.slice(0, 20), at: new Date().toISOString() };
  } finally { testRunning = false; }
  return testCache;
}

app.get('/api/state', (_req, res) => res.json({ ok: true, snapshot: engine.snapshot() }));
app.post('/api/start', async (_req, res) => result(res, await engine.start()));
app.post('/api/pause', async (_req, res) => result(res, await engine.pause()));
app.post('/api/resume', async (_req, res) => result(res, await engine.resume()));
app.post('/api/stop', async (_req, res) => result(res, await engine.stop()));
app.post('/api/reset', async (req, res) => result(res, await engine.reset(req.body || null)));
app.post('/api/config', async (req, res) => result(res, await engine.applyConfig(req.body || {})));
app.post('/api/pause-producers', (req, res) => result(res, engine.setRolePause('producers', req.body?.value !== false)));
app.post('/api/pause-consumers', (req, res) => result(res, engine.setRolePause('consumers', req.body?.value !== false)));
app.post('/api/unsafe', (req, res) => result(res, engine.setUnsafe(req.body?.value === true)));
app.post('/api/book', (req, res) => result(res, engine.generateBooking({ seat: req.body?.seat ?? null })));
app.post('/api/cancel', (req, res) => result(res, engine.generateCancellation({
  seat: req.body?.seat ?? null, passengerId: req.body?.passengerId ?? null,
})));
app.post('/api/race', (req, res) => result(res, engine.runRaceTest({
  seat: req.body?.seat ?? 10, count: req.body?.count ?? 3,
})));
app.post('/api/demo', async (_req, res) => result(res, await engine.loadDemo()));
app.get('/api/test-results', async (_req, res) => res.json({ ok: true, ...(await testResults()) }));

// Serve built client if present (vite build output copied to server/public).
app.use(express.static(path.join(__dirname, '..', 'public')));
app.get('*', (req, res, next) => {
  if (req.path.startsWith('/api') || req.path.startsWith('/ws')) return next();
  res.sendFile(path.join(__dirname, '..', 'public', 'index.html'), (err) => err && next());
});

const server = createServer(app);
const wss = new WebSocketServer({ path: '/ws', server });
function broadcast() {
  const payload = JSON.stringify({ t: 'snapshot', snapshot: engine.snapshot() });
  for (const c of wss.clients) { if (c.readyState === 1) c.send(payload); }
}
setInterval(broadcast, SNAPSHOT_INTERVAL_MS);
wss.on('connection', (ws) => {
  ws.send(JSON.stringify({ t: 'snapshot', snapshot: engine.snapshot() }));
});

server.listen(PORT, () => {
  console.log(`[server] Railway Producer-Consumer simulator on http://localhost:${PORT}`);
  console.log(`[server] API: /api/state  WS: /ws  (engine uses worker_threads + SharedArrayBuffer + Atomics)`);
});
