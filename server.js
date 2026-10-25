import express from 'express';
import http from 'http';
import crypto from 'crypto';
import path from 'path';
import { fileURLToPath } from 'url';
import { WebSocketServer } from 'ws';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = process.env.PORT || 3000;
const MAX_PEERS = Number(process.env.MAX_PEERS || 8);
const EMPTY_ROOM_TTL = 15 * 60 * 1000;

// Optional TURN: set ICE_SERVERS env var to a JSON array, e.g.
// [{"urls":"stun:stun.l.google.com:19302"},{"urls":"turn:host:80","username":"u","credential":"p"}]
const ICE_SERVERS = process.env.ICE_SERVERS
  ? JSON.parse(process.env.ICE_SERVERS)
  : [
      { urls: 'stun:stun.l.google.com:19302' },
      { urls: 'stun:stun1.l.google.com:19302' },
    ];

const app = express();
app.use(express.static(path.join(__dirname, 'public')));
app.get('/healthz', (_req, res) => res.send('ok'));

const rooms = new Map();

function newCode() {
  const alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let s = '';
  for (let i = 0; i < 5; i++) s += alphabet[crypto.randomInt(alphabet.length)];
  return s;
}

function scheduleCleanup(code) {
  const room = rooms.get(code);
  if (!room) return;
  clearTimeout(room.timer);
  if (room.peers.size === 0) {
    room.timer = setTimeout(() => rooms.delete(code), EMPTY_ROOM_TTL);
  }
}

app.post('/api/rooms', (_req, res) => {
  let code;
  do { code = newCode(); } while (rooms.has(code));
  rooms.set(code, {
    hostKey: crypto.randomBytes(16).toString('hex'),
    peers: new Map(),
    timer: null,
  });
  scheduleCleanup(code);
  res.json({ room: code, hostKey: rooms.get(code).hostKey });
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

const send = (ws, msg) => {
  if (ws.readyState === 1) ws.send(JSON.stringify(msg));
};

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  let me = null;
  let room = null;
  let roomCode = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'join' && !me) {
      roomCode = String(msg.room || '').toUpperCase();
      room = rooms.get(roomCode);
      if (!room) return send(ws, { type: 'error', message: 'Room not found or expired. Ask the host for a new link.' });
      if (room.peers.size >= MAX_PEERS) return send(ws, { type: 'error', message: 'Room is full.' });

      me = {
        id: crypto.randomUUID().slice(0, 8),
        name: String(msg.name || 'Guest').slice(0, 24),
        isHost: !!msg.hostKey && msg.hostKey === room.hostKey,
        ws,
      };
      clearTimeout(room.timer);

      const others = [...room.peers.values()].map(({ id, name, isHost }) => ({ id, name, isHost }));
      room.peers.set(me.id, me);

      send(ws, { type: 'joined', id: me.id, isHost: me.isHost, peers: others, iceServers: ICE_SERVERS });
      for (const p of room.peers.values()) {
        if (p.id !== me.id) send(p.ws, { type: 'peer-joined', peer: { id: me.id, name: me.name, isHost: me.isHost } });
      }
      return;
    }

    if (msg.type === 'signal' && me && room) {
      const target = room.peers.get(msg.to);
      if (target) send(target.ws, { type: 'signal', from: me.id, data: msg.data });
    }
  });

  ws.on('close', () => {
    if (!me || !room) return;
    room.peers.delete(me.id);
    for (const p of room.peers.values()) send(p.ws, { type: 'peer-left', id: me.id });
    scheduleCleanup(roomCode);
  });
});

// Keep connections alive through proxies and drop dead sockets
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 25000);

server.listen(PORT, () => console.log(`Floodlight running on :${PORT}`));
