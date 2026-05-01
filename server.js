'use strict';

const http = require('http');
const crypto = require('crypto');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 3000;
const HEARTBEAT_MS = 30000;
const MAX_MESSAGE_BYTES = 16000;
const PLAYER_LIMIT = 2;

let nextGameNumber = 1;
const lobbies = new Map();

const server = http.createServer((req, res) => {
  res.writeHead(200, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store'
  });
  res.end(JSON.stringify({
    ok: true,
    service: 'Mr Francis Ball WebSocket relay',
    lobbies: lobbies.size
  }));
});

const wss = new WebSocketServer({ server });

function randomId(bytes = 8) {
  return crypto.randomBytes(bytes).toString('hex');
}

function randomPin() {
  const length = 4 + Math.floor(Math.random() * 3);
  const min = 10 ** (length - 1);
  const max = (10 ** length) - 1;
  return String(min + Math.floor(Math.random() * (max - min + 1)));
}

function send(ws, payload) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  ws.send(JSON.stringify(payload));
}

function sendError(ws, message) {
  send(ws, { type: 'error', message });
}

function lobbyStatus(lobby) {
  if (!lobby) return 'Closed';
  if (lobby.players.length >= PLAYER_LIMIT) return 'Full';
  return 'Waiting';
}

function publicLobby(lobby) {
  return {
    lobbyId: lobby.lobbyId,
    gameNumber: lobby.gameNumber,
    status: lobbyStatus(lobby),
    playerCount: lobby.players.length,
    hostColour: lobby.hostColour || null,
    createdAt: lobby.createdAt,
    updatedAt: lobby.updatedAt,
    state: lobby.state || null,
    players: lobby.players.map((player) => ({
      role: player.role,
      colour: player.colour || null,
      connected: player.ws && player.ws.readyState === WebSocket.OPEN,
      paddle: player.paddle
    }))
  };
}

function lobbyList() {
  return [...lobbies.values()]
    .filter((lobby) => lobby.players.length > 0)
    .sort((a, b) => a.gameNumber - b.gameNumber)
    .map((lobby) => ({
      lobbyId: lobby.lobbyId,
      gameNumber: lobby.gameNumber,
      status: lobbyStatus(lobby),
      playerCount: lobby.players.length,
      createdAt: lobby.createdAt,
      updatedAt: lobby.updatedAt
    }));
}

function broadcastLobbyList() {
  const payload = { type: 'lobbyList', lobbies: lobbyList() };
  for (const client of wss.clients) {
    send(client, payload);
  }
}

function broadcastLobby(lobby, extra = {}) {
  const payload = Object.assign({ type: 'lobbyUpdate', lobby: publicLobby(lobby) }, extra);
  for (const player of lobby.players) {
    send(player.ws, payload);
  }
}

function sendToOpponent(ws, payload) {
  const lobby = ws.lobbyId ? lobbies.get(ws.lobbyId) : null;
  if (!lobby) return;
  for (const player of lobby.players) {
    if (player.ws !== ws) send(player.ws, payload);
  }
}

function currentPlayer(ws) {
  const lobby = ws.lobbyId ? lobbies.get(ws.lobbyId) : null;
  if (!lobby) return { lobby: null, player: null };
  return {
    lobby,
    player: lobby.players.find((p) => p.ws === ws) || null
  };
}

function maybeStartLobby(lobby) {
  if (!lobby || lobby.players.length !== PLAYER_LIMIT || !lobby.hostColour) return;
  const host = lobby.players.find((p) => p.role === 'host');
  const guest = lobby.players.find((p) => p.role === 'guest');
  if (!host || !guest) return;
  host.colour = lobby.hostColour;
  guest.colour = lobby.hostColour === 'red' ? 'blue' : 'red';
  lobby.status = 'playing';
  lobby.updatedAt = Date.now();
  const publicState = publicLobby(lobby);
  for (const player of lobby.players) {
    send(player.ws, {
      type: 'startGame',
      lobby: publicState,
      role: player.role,
      colour: player.colour
    });
  }
  broadcastLobbyList();
}

function leaveCurrentLobby(ws, reason = 'left') {
  const lobby = ws.lobbyId ? lobbies.get(ws.lobbyId) : null;
  if (!lobby) return;
  const index = lobby.players.findIndex((player) => player.ws === ws);
  if (index === -1) return;

  const [removed] = lobby.players.splice(index, 1);
  ws.lobbyId = null;
  ws.role = null;

  for (const player of lobby.players) {
    send(player.ws, {
      type: 'opponentDisconnected',
      role: removed.role,
      reason
    });
  }

  if (lobby.players.length === 0 || removed.role === 'host') {
    for (const player of lobby.players) {
      player.ws.lobbyId = null;
      player.ws.role = null;
      send(player.ws, { type: 'lobbyClosed', reason: removed.role === 'host' ? 'host disconnected' : reason });
    }
    lobbies.delete(lobby.lobbyId);
  } else {
    lobby.status = 'waiting';
    lobby.state = null;
    lobby.updatedAt = Date.now();
    broadcastLobby(lobby);
  }
  broadcastLobbyList();
}

function handleCreateLobby(ws) {
  leaveCurrentLobby(ws, 'new lobby');
  const lobby = {
    lobbyId: randomId(8),
    gameNumber: nextGameNumber++,
    pin: randomPin(),
    players: [],
    hostColour: null,
    status: 'waiting',
    state: null,
    createdAt: Date.now(),
    updatedAt: Date.now()
  };
  const host = {
    ws,
    role: 'host',
    token: randomId(16),
    colour: null,
    paddle: { x: 0, y: 0, vx: 0, vy: 0 },
    lastSeen: Date.now()
  };
  lobby.players.push(host);
  lobbies.set(lobby.lobbyId, lobby);
  ws.lobbyId = lobby.lobbyId;
  ws.role = 'host';

  send(ws, {
    type: 'lobbyCreated',
    lobby: publicLobby(lobby),
    lobbyId: lobby.lobbyId,
    gameNumber: lobby.gameNumber,
    pin: lobby.pin,
    role: 'host',
    token: host.token
  });
  broadcastLobbyList();
}

function handleJoinLobby(ws, data) {
  const gameNumber = Number(data.gameNumber);
  const pin = String(data.pin || '').trim();
  const lobby = [...lobbies.values()].find((item) => item.gameNumber === gameNumber);
  if (!lobby) return sendError(ws, 'Game not found');
  if (lobby.players.length >= PLAYER_LIMIT) return sendError(ws, 'Lobby is full');
  if (pin !== lobby.pin) return sendError(ws, 'Wrong PIN');

  leaveCurrentLobby(ws, 'joining lobby');
  const guest = {
    ws,
    role: 'guest',
    token: randomId(16),
    colour: lobby.hostColour ? (lobby.hostColour === 'red' ? 'blue' : 'red') : null,
    paddle: { x: 0, y: 0, vx: 0, vy: 0 },
    lastSeen: Date.now()
  };
  lobby.players.push(guest);
  lobby.updatedAt = Date.now();
  ws.lobbyId = lobby.lobbyId;
  ws.role = 'guest';

  send(ws, {
    type: 'joinedLobby',
    lobby: publicLobby(lobby),
    lobbyId: lobby.lobbyId,
    gameNumber: lobby.gameNumber,
    role: 'guest',
    token: guest.token
  });
  broadcastLobby(lobby);
  broadcastLobbyList();
  maybeStartLobby(lobby);
}

function handleSetColour(ws, data) {
  const { lobby, player } = currentPlayer(ws);
  if (!lobby || !player) return sendError(ws, 'Not in a lobby');
  if (player.role !== 'host') return sendError(ws, 'Only the host can choose colour');
  const colour = String(data.colour || '').toLowerCase();
  if (colour !== 'red' && colour !== 'blue') return sendError(ws, 'Invalid colour');

  lobby.hostColour = colour;
  for (const p of lobby.players) {
    p.colour = p.role === 'host' ? colour : (colour === 'red' ? 'blue' : 'red');
  }
  lobby.updatedAt = Date.now();
  broadcastLobby(lobby);
  maybeStartLobby(lobby);
}

function cleanNumber(value, fallback = 0, min = -5000, max = 5000) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, n));
}

function handlePaddle(ws, data) {
  const { lobby, player } = currentPlayer(ws);
  if (!lobby || !player) return;
  player.paddle = {
    x: cleanNumber(data.x),
    y: cleanNumber(data.y),
    vx: cleanNumber(data.vx),
    vy: cleanNumber(data.vy)
  };
  player.lastSeen = Date.now();
  lobby.updatedAt = player.lastSeen;
  sendToOpponent(ws, {
    type: 'paddle',
    role: player.role,
    x: player.paddle.x,
    y: player.paddle.y,
    vx: player.paddle.vx,
    vy: player.paddle.vy,
    serve: !!data.serve,
    boost: !!data.boost,
    seq: Number(data.seq) || 0
  });
}

function handleGameState(ws, data) {
  const { lobby, player } = currentPlayer(ws);
  if (!lobby || !player || player.role !== 'host') return;
  const state = data.state && typeof data.state === 'object' ? data.state : null;
  if (!state) return;
  const incomingSeq = Number(state.seq) || 0;
  const currentSeq = Number(lobby.state && lobby.state.seq) || 0;
  if (incomingSeq < currentSeq) return;
  lobby.state = state;
  lobby.updatedAt = Date.now();
  sendToOpponent(ws, { type: 'gameState', state });
}

function handleReplay(ws) {
  const { lobby, player } = currentPlayer(ws);
  if (!lobby || !player) return;
  lobby.state = null;
  lobby.updatedAt = Date.now();
  for (const p of lobby.players) {
    send(p.ws, { type: 'replay', requestedBy: player.role, lobby: publicLobby(lobby) });
  }
}

function handleMessage(ws, raw) {
  if (raw.length > MAX_MESSAGE_BYTES) {
    sendError(ws, 'Message too large');
    return;
  }
  let data;
  try {
    data = JSON.parse(raw.toString());
  } catch (err) {
    sendError(ws, 'Invalid JSON');
    return;
  }
  switch (data.type) {
    case 'listLobbies':
      send(ws, { type: 'lobbyList', lobbies: lobbyList() });
      break;
    case 'createLobby':
      handleCreateLobby(ws);
      break;
    case 'joinLobby':
      handleJoinLobby(ws, data);
      break;
    case 'setColour':
      handleSetColour(ws, data);
      break;
    case 'paddle':
      handlePaddle(ws, data);
      break;
    case 'gameState':
      handleGameState(ws, data);
      break;
    case 'replay':
      handleReplay(ws);
      break;
    case 'leave':
      leaveCurrentLobby(ws, 'left');
      send(ws, { type: 'leftLobby' });
      break;
    case 'ping':
      send(ws, { type: 'pong', t: Date.now() });
      break;
    default:
      sendError(ws, 'Unknown message type');
  }
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.lobbyId = null;
  ws.role = null;
  ws.on('pong', () => {
    ws.isAlive = true;
  });
  ws.on('message', (raw) => handleMessage(ws, raw));
  ws.on('close', () => leaveCurrentLobby(ws, 'disconnected'));
  ws.on('error', () => leaveCurrentLobby(ws, 'connection error'));
  send(ws, { type: 'connected', lobbies: lobbyList() });
});

setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      leaveCurrentLobby(ws, 'heartbeat timeout');
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, HEARTBEAT_MS);

server.listen(PORT, () => {
  console.log(`Mr Francis Ball WebSocket relay listening on ${PORT}`);
});
