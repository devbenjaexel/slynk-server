// Servidor de Slynk.io: sala pública única, chat, ping y clasificación top 100.
// Requiere Node 18+ y el paquete "ws" (npm install).
const http = require('http');
const fs = require('fs');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const FILE = process.env.SCORES_FILE || 'scores.json';
const ORIGIN = process.env.ALLOWED_ORIGIN || ''; // ej: https://tudominio.com (vacío = cualquiera)

let scores = {};
try { scores = JSON.parse(fs.readFileSync(FILE, 'utf8')); } catch (e) {}
let dirty = false;
setInterval(() => { if (dirty) { dirty = false; fs.writeFile(FILE, JSON.stringify(scores), () => {}); } }, 5000);

const top = () => Object.entries(scores)
  .map(([id, s]) => ({ id, ...s }))
  .sort((a, b) => b.best - a.best)
  .slice(0, 100);

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Servidor de Slynk.io funcionando');
});

const wss = new WebSocketServer({
  server,
  maxPayload: 8192,
  verifyClient: info => !ORIGIN || info.origin === ORIGIN,
});

const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
const broadcast = (o, except) => {
  const m = JSON.stringify(o);
  for (const c of wss.clients) if (c !== except && c.readyState === 1) c.send(m);
};

let nextId = 1;
wss.on('connection', ws => {
  const id = 'p' + (nextId++);
  ws.pid = id; ws.pres = {}; ws.count = 0;
  send(ws, { t: 'welcome', id });
  send(ws, { t: 'snap', list: [...wss.clients].filter(c => c !== ws && c.pid).map(c => ({ id: c.pid, p: c.pres })) });
  send(ws, { t: 'top', rows: top() });

  const reset = setInterval(() => { ws.count = 0; }, 1000);

  ws.on('message', raw => {
    if (++ws.count > 80) return; // límite de mensajes por segundo
    let m; try { m = JSON.parse(raw); } catch (e) { return; }

    if (m.t === 'pres' && m.p && typeof m.p === 'object') {
      if (JSON.stringify(m.p).length > 4096) return;
      for (const k in m.p) { if (m.p[k] === null) delete ws.pres[k]; else ws.pres[k] = m.p[k]; }
      broadcast({ t: 'p', id, p: m.p }, ws);

    } else if (m.t === 'emit' && typeof m.topic === 'string' && m.topic.length < 48) {
      const o = { t: 'emit', topic: m.topic, data: m.data, from: id };
      if (JSON.stringify(o).length > 2048) return;
      broadcast(o); // también vuelve al que lo envió (así se mide el ping)

    } else if (m.t === 'score' && typeof m.uid === 'string' && m.uid.length <= 40) {
      const best = Math.floor(+m.best);
      if (!(best > 0 && best <= 100000)) return;
      const prev = scores[m.uid];
      if (prev && prev.best >= best) return;
      scores[m.uid] = {
        name: String(m.name || '?').slice(0, 12),
        best,
        c1: /^#[0-9a-f]{6}$/i.test(m.c1) ? m.c1 : '#4ade80',
        ts: Date.now(),
      };
      dirty = true;
      broadcast({ t: 'top', rows: top() });
    }
  });

  ws.on('close', () => { clearInterval(reset); broadcast({ t: 'left', id }); });
  ws.on('error', () => {});
});

server.listen(PORT, () => console.log('Servidor de Slynk.io en el puerto ' + PORT));
