'use strict';

const http = require('http');
const crypto = require('crypto');
const { performance } = require('perf_hooks');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 3000;
const HEARTBEAT_MS = 30000;
const CLEANUP_MS = 60000;
const LOBBY_TTL_MS = 15 * 60 * 1000;
const FIXED_STEP_MS = 1000 / 60;
const LOOP_MS = 4;
const MAX_ACCUMULATOR_MS = 140;
const MAX_STEPS_PER_LOOP = 10;
const BROADCAST_MS = 1000 / 60;
const MAX_MESSAGE_BYTES = 16000;
const PLAYER_LIMIT = 2;

const CONFIG = {
  court: { width: 800, height: 560, depth: 1800 },
  ball: { radius: 26, spinDecay: 0.985, maxSpeed: 1900, initialZSpeed: 700, serveZ: 60 },
  paddle: { width: 170, height: 130 },
  physics: { substepThreshold: 18 },
  timing: { playerBoostWindowMs: 140, playerBoostMultiplier: 1.065 },
  score: { holdMs: 3000, fadeMs: 450 },
  shot: {
    hitGain: 1.04,
    serveEdgeKick: 310,
    hitEdgeKick: 320,
    hitEdgeBonus: 160,
    serveMoveKick: 0.40,
    hitMoveKick: 0.42,
    incomingDeflect: -0.045,
    glancingControlLoss: 0.16,
    serveSpinFromEdge: 390,
    hitSpinFromEdge: 520,
    serveSpinFromSwipe: 0.82,
    hitSpinFromSwipe: 1.05,
    maxSpin: 2600
  },
  network: {
    serveContactSlack: 0,
    sampledContactSlack: 10,
    inputHistoryLimit: 8
  }
};

let nextGameNumber = 1;
const lobbies = new Map();

const clamp = (value, min, max) => Math.max(min, Math.min(max, value));
const lerp = (a, b, t) => a + (b - a) * t;

function randomId(bytes = 8) {
  return crypto.randomBytes(bytes).toString('hex');
}

function randomPin() {
  const length = 4 + Math.floor(Math.random() * 3);
  const min = 10 ** (length - 1);
  const max = (10 ** length) - 1;
  return String(min + Math.floor(Math.random() * (max - min + 1)));
}

function nowWall() {
  return Date.now();
}

function cleanNumber(value, fallback = 0, min = -5000, max = 5000) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return clamp(n, min, max);
}

function cleanWinScore(value) {
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n)) return 10;
  return clamp(n, 1, 99);
}

function cleanColour(value) {
  const colour = String(value || '').toLowerCase();
  return colour === 'red' || colour === 'blue' ? colour : null;
}

function send(ws, payload) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  ws.send(JSON.stringify(payload));
}

function sendError(ws, message) {
  send(ws, { type: 'error', message });
}

function ballVisualWorldRadius(z) {
  const depthT = clamp(1 - (z / CONFIG.court.depth), 0, 1);
  return CONFIG.ball.radius * (0.95 + Math.pow(depthT, 1.25) * 2.1);
}

function humanPaddleHitRadius() {
  return Math.max(CONFIG.ball.radius, ballVisualWorldRadius(0));
}

function makeBall(role = 'host') {
  return {
    x: 0,
    y: 0,
    z: role === 'guest' ? CONFIG.court.depth - CONFIG.ball.serveZ : CONFIG.ball.serveZ,
    vx: 0,
    vy: 0,
    vz: 0,
    spinX: 0,
    spinY: 0
  };
}

function cloneBall(ball) {
  return {
    x: cleanNumber(ball && ball.x),
    y: cleanNumber(ball && ball.y),
    z: cleanNumber(ball && ball.z, CONFIG.ball.serveZ, -500, CONFIG.court.depth + 500),
    vx: cleanNumber(ball && ball.vx),
    vy: cleanNumber(ball && ball.vy),
    vz: cleanNumber(ball && ball.vz),
    spinX: cleanNumber(ball && ball.spinX),
    spinY: cleanNumber(ball && ball.spinY)
  };
}

function ballSpeed(ball) {
  return Math.sqrt(ball.vx * ball.vx + ball.vy * ball.vy + ball.vz * ball.vz);
}

function clampBallSpeed(ball) {
  const speed = ballSpeed(ball);
  if (speed > CONFIG.ball.maxSpeed) {
    const k = CONFIG.ball.maxSpeed / speed;
    ball.vx *= k;
    ball.vy *= k;
    ball.vz *= k;
  }
}

function makePaddle() {
  return { x: 0, y: 0, vx: 0, vy: 0 };
}

function clonePaddle(paddle) {
  return {
    x: cleanNumber(paddle && paddle.x),
    y: cleanNumber(paddle && paddle.y),
    vx: cleanNumber(paddle && paddle.vx, 0, -3200, 3200),
    vy: cleanNumber(paddle && paddle.vy, 0, -3200, 3200)
  };
}

function paddleBounds() {
  const hw = CONFIG.paddle.width / 2;
  const hh = CONFIG.paddle.height / 2;
  return {
    minX: -CONFIG.court.width / 2 + hw,
    maxX: CONFIG.court.width / 2 - hw,
    minY: -CONFIG.court.height / 2 + hh,
    maxY: CONFIG.court.height / 2 - hh
  };
}

function clampPaddle(paddle) {
  const b = paddleBounds();
  paddle.x = clamp(cleanNumber(paddle.x), b.minX, b.maxX);
  paddle.y = clamp(cleanNumber(paddle.y), b.minY, b.maxY);
  paddle.vx = cleanNumber(paddle.vx, 0, -3200, 3200);
  paddle.vy = cleanNumber(paddle.vy, 0, -3200, 3200);
  return paddle;
}

function cleanSamples(samples) {
  if (!Array.isArray(samples)) return [];
  return samples.slice(-CONFIG.network.inputHistoryLimit).map((sample) => ({
    t: cleanNumber(sample && sample.t, nowWall(), 0, 9999999999999),
    x: cleanNumber(sample && sample.x),
    y: cleanNumber(sample && sample.y),
    vx: cleanNumber(sample && sample.vx, 0, -3200, 3200),
    vy: cleanNumber(sample && sample.vy, 0, -3200, 3200)
  }));
}

function strongestVelocity(data, fallback) {
  const result = {
    vx: cleanNumber(fallback && fallback.vx, 0, -3200, 3200),
    vy: cleanNumber(fallback && fallback.vy, 0, -3200, 3200)
  };
  const peakVX = cleanNumber(data && data.peakVX, 0, -3200, 3200);
  const peakVY = cleanNumber(data && data.peakVY, 0, -3200, 3200);
  if (Math.abs(peakVX) > Math.abs(result.vx)) result.vx = peakVX;
  if (Math.abs(peakVY) > Math.abs(result.vy)) result.vy = peakVY;
  for (const sample of cleanSamples(data && data.samples)) {
    if (Math.abs(sample.vx) > Math.abs(result.vx)) result.vx = sample.vx;
    if (Math.abs(sample.vy) > Math.abs(result.vy)) result.vy = sample.vy;
  }
  result.vx = cleanNumber(result.vx, 0, -2400, 2400);
  result.vy = cleanNumber(result.vy, 0, -2400, 2400);
  return result;
}

function otherRole(role) {
  return role === 'host' ? 'guest' : 'host';
}

function playerForRole(lobby, role) {
  return lobby.players.find((player) => player.role === role) || null;
}

function colourForRole(lobby, role) {
  const player = playerForRole(lobby, role);
  return player && player.colour ? player.colour : (role === 'guest' ? 'blue' : 'red');
}

function roleForColour(lobby, colour) {
  const player = lobby.players.find((item) => item.colour === colour);
  if (player) return player.role;
  return colour === 'blue' ? 'guest' : 'host';
}

function servingRole(lobby) {
  return roleForColour(lobby, lobby.state.serveColor || 'red');
}

function lobbyStatus(lobby) {
  if (lobby.players.length >= PLAYER_LIMIT) return 'Full';
  return 'Waiting';
}

function makeEvent(lobby, type, role) {
  lobby.eventSeq += 1;
  lobby.state.event = {
    seq: lobby.eventSeq,
    type,
    role: role || null,
    serverTime: nowWall(),
    simulationTime: lobby.simulationTime
  };
  lobby.state.eventSeq = lobby.eventSeq;
  lobby.forceBroadcast = true;
}

function initialState(lobby) {
  lobby.eventSeq = 0;
  lobby.roundSeq = 1;
  return {
    seq: 0,
    serverTime: nowWall(),
    simulationTime: 0,
    tickTime: FIXED_STEP_MS / 1000,
    eventSeq: 0,
    roundSeq: lobby.roundSeq,
    roundState: 'serving',
    serveColor: 'red',
    serveRole: roleForColour(lobby, 'red'),
    nextServeColor: 'red',
    scores: { red: 0, blue: 0 },
    winScore: cleanWinScore(lobby.winScore),
    scoreEventSeq: 0,
    winner: null,
    ball: makeBall(roleForColour(lobby, 'red')),
    event: { seq: 0, type: 'none', role: null },
    paddles: { host: makePaddle(), guest: makePaddle() }
  };
}

function syncStatePaddles(lobby) {
  const host = playerForRole(lobby, 'host');
  const guest = playerForRole(lobby, 'guest');
  lobby.state.paddles = {
    host: host ? clonePaddle(host.paddle) : makePaddle(),
    guest: guest ? clonePaddle(guest.paddle) : makePaddle()
  };
}

function parkServeBall(lobby) {
  const role = servingRole(lobby);
  lobby.state.serveRole = role;
  Object.assign(lobby.state.ball, makeBall(role));
}

function publicLobby(lobby) {
  return {
    lobbyId: lobby.lobbyId,
    gameNumber: lobby.gameNumber,
    status: lobbyStatus(lobby),
    playerCount: lobby.players.length,
    isPublic: !!lobby.isPublic,
    winScore: cleanWinScore(lobby.winScore),
    hostColour: lobby.hostColour || null,
    createdAt: lobby.createdAt,
    updatedAt: lobby.updatedAt,
    players: lobby.players.map((player) => ({
      role: player.role,
      colour: player.colour || null,
      connected: player.ws && player.ws.readyState === WebSocket.OPEN
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
      isPublic: !!lobby.isPublic,
      winScore: cleanWinScore(lobby.winScore),
      createdAt: lobby.createdAt,
      updatedAt: lobby.updatedAt
    }));
}

function broadcastLobbyList() {
  const payload = { type: 'lobbyList', lobbies: lobbyList() };
  for (const client of wss.clients) send(client, payload);
}

function broadcastLobby(lobby) {
  for (const player of lobby.players) {
    send(player.ws, { type: 'lobbyUpdate', lobby: publicLobby(lobby) });
  }
}

function snapshot(lobby) {
  syncStatePaddles(lobby);
  const state = lobby.state;
  state.serverTime = nowWall();
  state.simulationTime = lobby.simulationTime;
  state.tickTime = FIXED_STEP_MS / 1000;
  state.serveRole = servingRole(lobby);
  state.winScore = cleanWinScore(lobby.winScore);
  return {
    seq: state.seq,
    serverTime: state.serverTime,
    simulationTime: state.simulationTime,
    tickTime: state.tickTime,
    eventSeq: state.eventSeq,
    roundSeq: state.roundSeq,
    roundState: state.roundState,
    serveColor: state.serveColor,
    serveRole: state.serveRole,
    nextServeColor: state.nextServeColor,
    scores: { red: state.scores.red || 0, blue: state.scores.blue || 0 },
    winScore: state.winScore,
    scoreEventSeq: state.scoreEventSeq || 0,
    winner: state.winner || null,
    ball: cloneBall(state.ball),
    paddles: {
      host: clonePaddle(state.paddles.host),
      guest: clonePaddle(state.paddles.guest)
    },
    event: state.event || { seq: 0, type: 'none', role: null }
  };
}

function broadcastState(lobby, force = false) {
  if (!lobby.state) return;
  const now = nowWall();
  let sent = false;
  for (const player of lobby.players) {
    const interval = player.broadcastIntervalMs || BROADCAST_MS;
    if (!force && now - (player.lastBroadcastAt || 0) < interval) continue;
    if (!sent) {
      lobby.state.seq += 1;
      lobby.cachedSnapshot = snapshot(lobby);
      sent = true;
    }
    player.lastBroadcastAt = now;
    send(player.ws, { type: 'gameState', state: lobby.cachedSnapshot });
  }
  if (sent) lobby.lastBroadcastAt = now;
}

function contactInfo(ball, paddle, extraSlack = 0) {
  const dx = ball.x - paddle.x;
  const dy = ball.y - paddle.y;
  const hw = CONFIG.paddle.width / 2;
  const hh = CONFIG.paddle.height / 2;
  const r = humanPaddleHitRadius();
  const closestX = clamp(dx, -hw, hw);
  const closestY = clamp(dy, -hh, hh);
  const sepX = dx - closestX;
  const sepY = dy - closestY;
  const outsideDistance = Math.sqrt(sepX * sepX + sepY * sepY);
  const hitRadius = r + extraSlack;
  return {
    paddle,
    r,
    overlaps: outsideDistance <= hitRadius,
    closestX,
    closestY,
    outsideDistance,
    offsetX: clamp(dx / Math.max(1, hw), -1.25, 1.25),
    offsetY: clamp(dy / Math.max(1, hh), -1.25, 1.25),
    edge: clamp(Math.sqrt((dx / hw) * (dx / hw) + (dy / hh) * (dy / hh)) / 1.35, 0, 1),
    glancing: clamp(outsideDistance / Math.max(1, hitRadius), 0, 1)
  };
}

function sampledPaddlesFor(player) {
  const paddles = [clonePaddle(player.paddle)];
  const samples = Array.isArray(player.samples) ? player.samples.slice(-CONFIG.network.inputHistoryLimit) : [];
  for (let i = samples.length - 1; i >= 0; i--) {
    const sample = samples[i];
    const paddle = {
      x: cleanNumber(sample.x),
      y: cleanNumber(sample.y),
      vx: cleanNumber(player.paddle.vx, 0, -3200, 3200),
      vy: cleanNumber(player.paddle.vy, 0, -3200, 3200)
    };
    clampPaddle(paddle);
    paddles.push(paddle);
  }
  return paddles;
}

function bestContactInfo(player, ball, extraSlack = 0) {
  let best = null;
  for (const paddle of sampledPaddlesFor(player)) {
    const contact = contactInfo(ball, paddle, extraSlack);
    if (!contact.overlaps) continue;
    if (!best || contact.outsideDistance < best.outsideDistance) best = contact;
  }
  return best;
}

function applyPaddleShot(ball, paddle, isServe, contact) {
  const cappedVX = clamp(paddle.vx || 0, -2400, 2400);
  const cappedVY = clamp(paddle.vy || 0, -2400, 2400);
  const incomingVX = isServe ? 0 : ball.vx;
  const incomingVY = isServe ? 0 : ball.vy;
  const edgeKick = isServe
    ? CONFIG.shot.serveEdgeKick
    : CONFIG.shot.hitEdgeKick + contact.edge * CONFIG.shot.hitEdgeBonus;
  const moveKick = isServe ? CONFIG.shot.serveMoveKick : CONFIG.shot.hitMoveKick;
  const incomingDeflect = isServe ? 0 : contact.edge * CONFIG.shot.incomingDeflect;
  const controlLoss = 1 - contact.glancing * CONFIG.shot.glancingControlLoss;

  ball.vx = (ball.vx + contact.offsetX * edgeKick + cappedVX * moveKick + incomingVX * incomingDeflect) * controlLoss;
  ball.vy = (ball.vy + contact.offsetY * edgeKick + cappedVY * moveKick + incomingVY * incomingDeflect) * controlLoss;

  const acrossX = cappedVX - incomingVX * 0.18;
  const acrossY = cappedVY - incomingVY * 0.18;
  const spinFromEdge = isServe ? CONFIG.shot.serveSpinFromEdge : CONFIG.shot.hitSpinFromEdge;
  const spinFromSwipe = isServe ? CONFIG.shot.serveSpinFromSwipe : CONFIG.shot.hitSpinFromSwipe;
  ball.spinX += contact.offsetX * spinFromEdge + acrossX * spinFromSwipe;
  ball.spinY += contact.offsetY * spinFromEdge + acrossY * spinFromSwipe;
  ball.spinX = clamp(ball.spinX, -CONFIG.shot.maxSpin, CONFIG.shot.maxSpin);
  ball.spinY = clamp(ball.spinY, -CONFIG.shot.maxSpin, CONFIG.shot.maxSpin);
  clampBallSpeed(ball);
}

function applyBoostIfReady(lobby, player) {
  if (!player.boostUntil || nowWall() > player.boostUntil) return;
  const ball = lobby.state.ball;
  ball.vx *= CONFIG.timing.playerBoostMultiplier;
  ball.vy *= CONFIG.timing.playerBoostMultiplier;
  ball.vz *= CONFIG.timing.playerBoostMultiplier;
  clampBallSpeed(ball);
  player.boostUntil = 0;
}

function applyPaddleHit(lobby, role, isServe, contact) {
  const state = lobby.state;
  const ball = state.ball;
  const player = playerForRole(lobby, role);
  const dir = role === 'guest' ? -1 : 1;
  const r = contact.r;

  ball.z = role === 'guest' ? CONFIG.court.depth - r : r;
  if (isServe) {
    ball.vx = 0;
    ball.vy = 0;
    ball.vz = CONFIG.ball.initialZSpeed * dir;
    ball.spinX = 0;
    ball.spinY = 0;
  } else {
    ball.vz = Math.abs(ball.vz) * dir;
    ball.vx *= CONFIG.shot.hitGain;
    ball.vy *= CONFIG.shot.hitGain;
    ball.vz *= CONFIG.shot.hitGain;
  }
  clampBallSpeed(ball);
  applyPaddleShot(ball, player ? player.paddle : contact.paddle, !!isServe, contact);
  if (!isServe && player) applyBoostIfReady(lobby, player);

  lobby.lastHitRole = role;
  lobby.lastHitSimulationTime = lobby.simulationTime;
  makeEvent(lobby, isServe ? 'serve' : 'hit', role);
}

function tryPaddleHit(lobby, role, isServe = false) {
  const player = playerForRole(lobby, role);
  if (!player) return false;
  const recentlySame = !isServe &&
    lobby.lastHitRole === role &&
    lobby.simulationTime - (lobby.lastHitSimulationTime || 0) < 80;
  if (recentlySame) return false;
  const slack = isServe ? CONFIG.network.serveContactSlack : CONFIG.network.sampledContactSlack;
  const contact = isServe
    ? contactInfo(lobby.state.ball, player.paddle, slack)
    : bestContactInfo(player, lobby.state.ball, slack);
  if (!contact || !contact.overlaps) return false;
  applyPaddleHit(lobby, role, isServe, contact);
  return true;
}

function launchServe(lobby, role) {
  const state = lobby.state;
  if (!state || state.roundState !== 'serving') return;
  if (servingRole(lobby) !== role) return;
  parkServeBall(lobby);
  if (!tryPaddleHit(lobby, role, true)) return;
  state.roundState = 'playing';
  lobby.forceBroadcast = true;
}

function scorePoint(lobby, scorerRole) {
  const state = lobby.state;
  const colour = colourForRole(lobby, scorerRole);
  state.scores[colour] = (state.scores[colour] || 0) + 1;
  state.scoreEventSeq = (state.scoreEventSeq || 0) + 1;
  state.roundSeq += 1;
  state.ball.vx = 0;
  state.ball.vy = 0;
  state.ball.vz = 0;
  state.ball.spinX = 0;
  state.ball.spinY = 0;
  makeEvent(lobby, 'score', scorerRole);

  if (state.scores[colour] >= cleanWinScore(lobby.winScore)) {
    state.winner = colour;
    state.roundState = 'ended';
    makeEvent(lobby, 'end', scorerRole);
    return;
  }

  state.nextServeColor = state.serveColor === 'red' ? 'blue' : 'red';
  state.roundState = 'scoreHold';
  lobby.gateUntil = nowWall() + CONFIG.score.holdMs;
}

function positionAtCrossing(ball, prev, targetZ) {
  const dz = ball.z - prev.z;
  const t = Math.abs(dz) > 0.0001 ? clamp((targetZ - prev.z) / dz, 0, 1) : 1;
  ball.x = lerp(prev.x, ball.x, t);
  ball.y = lerp(prev.y, ball.y, t);
  ball.z = targetZ;
}

function wallBounce(lobby) {
  const now = lobby.simulationTime;
  if (now - (lobby.lastWallEventAt || 0) > 30) {
    lobby.lastWallEventAt = now;
    makeEvent(lobby, 'wall', null);
  }
}

function subStepBall(lobby, dt) {
  const ball = lobby.state.ball;
  ball.vx += ball.spinX * dt;
  ball.vy += ball.spinY * dt;
  const decay = Math.pow(CONFIG.ball.spinDecay, dt * 60);
  ball.spinX *= decay;
  ball.spinY *= decay;
  clampBallSpeed(ball);

  const prev = { x: ball.x, y: ball.y, z: ball.z };
  ball.x += ball.vx * dt;
  ball.y += ball.vy * dt;
  ball.z += ball.vz * dt;

  const W = CONFIG.court.width / 2 - CONFIG.ball.radius;
  const H = CONFIG.court.height / 2 - CONFIG.ball.radius;
  let wallHit = false;
  if (ball.x < -W) { ball.x = -W; ball.vx = -ball.vx; wallHit = true; }
  if (ball.x > W) { ball.x = W; ball.vx = -ball.vx; wallHit = true; }
  if (ball.y < -H) { ball.y = -H; ball.vy = -ball.vy; wallHit = true; }
  if (ball.y > H) { ball.y = H; ball.vy = -ball.vy; wallHit = true; }
  if (wallHit) wallBounce(lobby);

  const r = humanPaddleHitRadius();
  if (ball.vz < 0 && prev.z >= r && ball.z <= r) {
    positionAtCrossing(ball, prev, r);
    if (!tryPaddleHit(lobby, 'host', false)) {
      ball.z = 0;
      scorePoint(lobby, 'guest');
      return false;
    }
  }
  if (ball.vz > 0 && prev.z <= CONFIG.court.depth - r && ball.z >= CONFIG.court.depth - r) {
    positionAtCrossing(ball, prev, CONFIG.court.depth - r);
    if (!tryPaddleHit(lobby, 'guest', false)) {
      ball.z = CONFIG.court.depth;
      scorePoint(lobby, 'host');
      return false;
    }
  }
  return true;
}

function stepBall(lobby, dt) {
  const ball = lobby.state.ball;
  const stepSize = Math.max(8, CONFIG.ball.radius * 0.6);
  const steps = Math.max(1, Math.ceil(ballSpeed(ball) * dt / stepSize));
  const sub = dt / steps;
  for (let i = 0; i < steps; i++) {
    if (!subStepBall(lobby, sub)) return;
  }
}

function simulateLobby(lobby, dt) {
  const state = lobby.state;
  if (!state || lobby.status !== 'playing' || state.winner) return;
  const wallNow = nowWall();

  if (state.roundState === 'playing') {
    stepBall(lobby, dt);
    return;
  }

  if (state.roundState === 'serving') {
    parkServeBall(lobby);
    return;
  }

  if (state.roundState === 'scoreHold' && wallNow >= lobby.gateUntil) {
    state.roundState = 'scoreFade';
    lobby.gateUntil = wallNow + CONFIG.score.fadeMs;
    lobby.forceBroadcast = true;
    return;
  }

  if (state.roundState === 'scoreFade' && wallNow >= lobby.gateUntil) {
    state.serveColor = state.nextServeColor || state.serveColor || 'red';
    state.roundState = 'serving';
    state.roundSeq += 1;
    lobby.lastHitRole = null;
    parkServeBall(lobby);
    lobby.forceBroadcast = true;
  }
}

function advanceLobby(lobby, elapsedMs) {
  if (lobby.status !== 'playing' || !lobby.state) return;
  lobby.accumulatorMs = Math.min(MAX_ACCUMULATOR_MS, lobby.accumulatorMs + Math.max(0, elapsedMs));
  let steps = 0;
  while (lobby.accumulatorMs >= FIXED_STEP_MS && steps < MAX_STEPS_PER_LOOP) {
    simulateLobby(lobby, FIXED_STEP_MS / 1000);
    lobby.simulationTime += FIXED_STEP_MS;
    lobby.accumulatorMs -= FIXED_STEP_MS;
    steps += 1;
  }
  if (steps >= MAX_STEPS_PER_LOOP) lobby.accumulatorMs = 0;
  if (steps > 0 || lobby.forceBroadcast) {
    broadcastState(lobby, lobby.forceBroadcast);
    lobby.forceBroadcast = false;
  }
}

function currentPlayer(ws) {
  const lobby = ws.lobbyId ? lobbies.get(ws.lobbyId) : null;
  if (!lobby) return { lobby: null, player: null };
  return { lobby, player: lobby.players.find((item) => item.ws === ws) || null };
}

function maybeStartLobby(lobby) {
  if (!lobby || lobby.players.length !== PLAYER_LIMIT || !lobby.hostColour) return;
  const host = playerForRole(lobby, 'host');
  const guest = playerForRole(lobby, 'guest');
  if (!host || !guest) return;

  host.colour = lobby.hostColour;
  guest.colour = lobby.hostColour === 'red' ? 'blue' : 'red';
  lobby.status = 'playing';
  lobby.state = initialState(lobby);
  lobby.simulationTime = 0;
  lobby.accumulatorMs = 0;
  lobby.gateUntil = 0;
  lobby.lastHitRole = null;
  lobby.updatedAt = nowWall();
  parkServeBall(lobby);
  syncStatePaddles(lobby);

  const shared = publicLobby(lobby);
  for (const player of lobby.players) {
    send(player.ws, {
      type: 'startGame',
      lobby: shared,
      gameNumber: lobby.gameNumber,
      role: player.role,
      colour: player.colour,
      opponentColour: player.colour === 'red' ? 'blue' : 'red',
      isPublic: !!lobby.isPublic,
      pin: lobby.isPublic ? null : lobby.pin,
      winScore: cleanWinScore(lobby.winScore)
    });
  }
  broadcastState(lobby, true);
  broadcastLobbyList();
}

function leaveCurrentLobby(ws, reason = 'left') {
  const lobby = ws.lobbyId ? lobbies.get(ws.lobbyId) : null;
  if (!lobby) return;
  const index = lobby.players.findIndex((player) => player.ws === ws);
  if (index < 0) return;
  const [removed] = lobby.players.splice(index, 1);
  ws.lobbyId = null;
  ws.role = null;

  for (const player of lobby.players) {
    send(player.ws, { type: 'opponentDisconnected', role: removed.role, reason });
  }

  if (!lobby.players.length || removed.role === 'host') {
    for (const player of lobby.players) {
      player.ws.lobbyId = null;
      player.ws.role = null;
      send(player.ws, { type: 'lobbyClosed', reason: removed.role === 'host' ? 'host disconnected' : reason });
    }
    lobbies.delete(lobby.lobbyId);
  } else {
    lobby.status = 'waiting';
    lobby.state = null;
    lobby.accumulatorMs = 0;
    lobby.simulationTime = 0;
    lobby.updatedAt = nowWall();
    broadcastLobby(lobby);
  }
  broadcastLobbyList();
}

function handleCreateLobby(ws, data) {
  leaveCurrentLobby(ws, 'new lobby');
  const isPublic = data.isPublic !== false;
  const hostColour = cleanColour(data.hostColour) || 'red';
  const lobby = {
    lobbyId: randomId(8),
    gameNumber: nextGameNumber++,
    pin: isPublic ? null : randomPin(),
    isPublic,
    winScore: cleanWinScore(data.winScore),
    hostColour,
    players: [],
    status: 'waiting',
    state: null,
    eventSeq: 0,
    roundSeq: 0,
    simulationTime: 0,
    accumulatorMs: 0,
    gateUntil: 0,
    forceBroadcast: false,
    createdAt: nowWall(),
    updatedAt: nowWall()
  };
  const host = {
    ws,
    role: 'host',
    token: randomId(16),
    colour: hostColour,
    paddle: makePaddle(),
    samples: [],
    boostUntil: 0,
    lastSeen: nowWall()
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
    isPublic,
    winScore: lobby.winScore,
    role: 'host',
    token: host.token
  });
  broadcastLobbyList();
}

function handleJoinLobby(ws, data) {
  const gameNumber = Number(data.gameNumber);
  const lobby = [...lobbies.values()].find((item) => item.gameNumber === gameNumber);
  if (!lobby) return sendError(ws, 'Game not found');
  if (lobby.players.length >= PLAYER_LIMIT) return sendError(ws, 'Lobby is full');
  if (!lobby.isPublic && String(data.pin || '').trim() !== lobby.pin) return sendError(ws, 'Wrong PIN');

  leaveCurrentLobby(ws, 'joining lobby');
  const guest = {
    ws,
    role: 'guest',
    token: randomId(16),
    colour: lobby.hostColour === 'red' ? 'blue' : 'red',
    paddle: makePaddle(),
    samples: [],
    boostUntil: 0,
    lastSeen: nowWall()
  };
  lobby.players.push(guest);
  lobby.updatedAt = nowWall();
  ws.lobbyId = lobby.lobbyId;
  ws.role = 'guest';

  send(ws, {
    type: 'joinedLobby',
    lobby: publicLobby(lobby),
    lobbyId: lobby.lobbyId,
    gameNumber: lobby.gameNumber,
    pin: lobby.isPublic ? null : lobby.pin,
    isPublic: !!lobby.isPublic,
    winScore: lobby.winScore,
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
  if (player.role !== 'host') return sendError(ws, 'Only host can choose colour');
  const colour = cleanColour(data.colour);
  if (!colour) return sendError(ws, 'Invalid colour');
  lobby.hostColour = colour;
  for (const p of lobby.players) p.colour = p.role === 'host' ? colour : (colour === 'red' ? 'blue' : 'red');
  lobby.updatedAt = nowWall();
  broadcastLobby(lobby);
  maybeStartLobby(lobby);
}

function handleClientHello(ws, data) {
  const { player } = currentPlayer(ws);
  if (!player) return;
  const requested = Math.floor(Number(data.intervalMs));
  if (Number.isFinite(requested)) player.broadcastIntervalMs = clamp(requested, 16, 100);
}

function handlePaddle(ws, data) {
  const { lobby, player } = currentPlayer(ws);
  if (!lobby || !player) return;
  const paddle = {
    x: cleanNumber(data.x),
    y: cleanNumber(data.y),
    vx: cleanNumber(data.vx, 0, -3200, 3200),
    vy: cleanNumber(data.vy, 0, -3200, 3200)
  };
  const strongest = strongestVelocity(data, paddle);
  paddle.vx = strongest.vx;
  paddle.vy = strongest.vy;
  clampPaddle(paddle);
  player.paddle = paddle;
  player.samples = cleanSamples(data.samples);
  player.lastSeen = nowWall();
  lobby.updatedAt = player.lastSeen;

  if (data.boost) player.boostUntil = nowWall() + CONFIG.timing.playerBoostWindowMs;
  if (data.serve) launchServe(lobby, player.role);
}

function handleReplay(ws) {
  const { lobby, player } = currentPlayer(ws);
  if (!lobby || !player || lobby.players.length !== PLAYER_LIMIT) return;
  lobby.state = initialState(lobby);
  lobby.simulationTime = 0;
  lobby.accumulatorMs = 0;
  lobby.gateUntil = 0;
  lobby.lastHitRole = null;
  lobby.forceBroadcast = true;
  parkServeBall(lobby);
  for (const p of lobby.players) send(p.ws, { type: 'replay', requestedBy: player.role, lobby: publicLobby(lobby) });
  broadcastState(lobby, true);
}

function handleMessage(ws, raw) {
  if (raw.length > MAX_MESSAGE_BYTES) return sendError(ws, 'Message too large');
  let data;
  try {
    data = JSON.parse(raw.toString());
  } catch (err) {
    return sendError(ws, 'Invalid JSON');
  }

  switch (data.type) {
    case 'listLobbies':
      send(ws, { type: 'lobbyList', lobbies: lobbyList() });
      break;
    case 'createLobby':
      handleCreateLobby(ws, data);
      break;
    case 'joinLobby':
      handleJoinLobby(ws, data);
      break;
    case 'setColour':
      handleSetColour(ws, data);
      break;
    case 'clientHello':
      handleClientHello(ws, data);
      break;
    case 'paddle':
      handlePaddle(ws, data);
      break;
    case 'replay':
      handleReplay(ws);
      break;
    case 'leave':
      leaveCurrentLobby(ws, 'left');
      send(ws, { type: 'leftLobby' });
      break;
    case 'ping':
      send(ws, { type: 'pong', t: Number(data.t) || 0, serverTime: nowWall() });
      break;
    default:
      sendError(ws, 'Unknown message type');
  }
}

const server = http.createServer((req, res) => {
  res.writeHead(200, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store'
  });
  res.end(JSON.stringify({
    ok: true,
    service: 'Mr Francis Ball multiplayer relay',
    transport: 'WebSocket',
    lobbies: lobbies.size
  }));
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.lobbyId = null;
  ws.role = null;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (raw) => handleMessage(ws, raw));
  ws.on('close', () => leaveCurrentLobby(ws, 'disconnected'));
  ws.on('error', () => leaveCurrentLobby(ws, 'connection error'));
  send(ws, { type: 'connected', lobbies: lobbyList() });
});

let lastLoopAt = performance.now();
setInterval(() => {
  const now = performance.now();
  const elapsed = Math.min(MAX_ACCUMULATOR_MS, Math.max(0, now - lastLoopAt));
  lastLoopAt = now;
  for (const lobby of lobbies.values()) advanceLobby(lobby, elapsed);
}, LOOP_MS);

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

setInterval(() => {
  const cutoff = nowWall() - LOBBY_TTL_MS;
  for (const lobby of lobbies.values()) {
    if (lobby.updatedAt >= cutoff) continue;
    for (const player of lobby.players) {
      send(player.ws, { type: 'lobbyClosed', reason: 'inactive' });
      player.ws.lobbyId = null;
      player.ws.role = null;
    }
    lobbies.delete(lobby.lobbyId);
  }
  broadcastLobbyList();
}, CLEANUP_MS);

server.listen(PORT, () => {
  console.log(`Mr Francis Ball multiplayer relay listening on ${PORT}`);
});
