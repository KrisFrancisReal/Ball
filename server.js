'use strict';

const http = require('http');
const crypto = require('crypto');
const { performance } = require('perf_hooks');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 3000;
const HEARTBEAT_MS = 30000;
const SIM_TICK_MS = 1000 / 60;
const SIM_LOOP_MS = 8;
const MAX_ACCUMULATOR_MS = 120;
const MAX_SIM_STEPS = 8;
const STATE_BROADCAST_MS = 16;
const MAX_MESSAGE_BYTES = 16000;
const PLAYER_LIMIT = 2;

const CONFIG = {
  court: { width: 800, height: 560, depth: 1800 },
  // serveSpeedMultiplier raised 0.94 -> 1.10 so multiplayer serves match the
  // punch of a single-player level-3 serve instead of feeling like level 1.
  ball: { radius: 26, spinDecay: 0.985, maxSpeed: 1900, initialZSpeed: 700, serveSpeedMultiplier: 1.10, serveZ: 60 },
  paddle: { width: 170, height: 130 },
  scoreOverlayMs: 3000,
  scoreFadeMs: 450,
  timing: { playerBoostMultiplier: 1.065 },
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
    remotePaddleSlack: 18,
    hitClaimGraceMs: 240,
    hitClaimZWindow: 280,
    hitClaimBallDriftLimit: 440,
    hitClaimValidateSlack: 68
  },
  physics: { substepThreshold: 16 }
};

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
    mode: 'server-authoritative-ball',
    lobbies: lobbies.size
  }));
});

const wss = new WebSocketServer({ server });

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

function cleanWinScore(value) {
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n)) return 10;
  return Math.max(1, Math.min(99, n));
}

function cleanHostColour(value) {
  const colour = String(value || '').toLowerCase();
  return colour === 'red' || colour === 'blue' ? colour : null;
}

function cleanNumber(value, fallback = 0, min = -5000, max = 5000) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, n));
}

function send(ws, payload) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  ws.send(JSON.stringify(payload));
}

function sendError(ws, message) {
  send(ws, { type: 'error', message });
}

function ballSpeed(ball) {
  return Math.hypot(ball.vx || 0, ball.vy || 0, ball.vz || 0);
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

function ballVisualWorldRadius(z) {
  const depthT = clamp(1 - (z / CONFIG.court.depth), 0, 1);
  return CONFIG.ball.radius * (0.95 + Math.pow(depthT, 1.25) * 2.1);
}

function cloneBall(ball) {
  return {
    x: Number(ball && ball.x) || 0,
    y: Number(ball && ball.y) || 0,
    z: Number(ball && ball.z) || CONFIG.ball.serveZ,
    vx: Number(ball && ball.vx) || 0,
    vy: Number(ball && ball.vy) || 0,
    vz: Number(ball && ball.vz) || 0,
    spinX: Number(ball && ball.spinX) || 0,
    spinY: Number(ball && ball.spinY) || 0
  };
}

function cleanBallPayload(ball) {
  return {
    x: cleanNumber(ball && ball.x),
    y: cleanNumber(ball && ball.y),
    z: cleanNumber(ball && ball.z, CONFIG.ball.serveZ, -1000, 3000),
    vx: cleanNumber(ball && ball.vx),
    vy: cleanNumber(ball && ball.vy),
    vz: cleanNumber(ball && ball.vz),
    spinX: cleanNumber(ball && ball.spinX),
    spinY: cleanNumber(ball && ball.spinY)
  };
}

function makePaddle() {
  return { x: 0, y: 0, vx: 0, vy: 0 };
}

function paddleBounds() {
  const hw = CONFIG.paddle.width / 2;
  const hh = CONFIG.paddle.height / 2;
  return {
    minX: -CONFIG.court.width / 2 + hw,
    maxX:  CONFIG.court.width / 2 - hw,
    minY: -CONFIG.court.height / 2 + hh,
    maxY:  CONFIG.court.height / 2 - hh
  };
}

function clampPaddle(paddle) {
  const bounds = paddleBounds();
  paddle.x = clamp(cleanNumber(paddle.x), bounds.minX, bounds.maxX);
  paddle.y = clamp(cleanNumber(paddle.y), bounds.minY, bounds.maxY);
  paddle.vx = cleanNumber(paddle.vx);
  paddle.vy = cleanNumber(paddle.vy);
}

function cleanInputSamples(samples) {
  if (!Array.isArray(samples)) return [];
  return samples.slice(-8).map((sample) => ({
    t: cleanNumber(sample && sample.t, Date.now(), 0, 9999999999999),
    x: cleanNumber(sample && sample.x),
    y: cleanNumber(sample && sample.y),
    vx: cleanNumber(sample && sample.vx, 0, -3200, 3200),
    vy: cleanNumber(sample && sample.vy, 0, -3200, 3200)
  }));
}

function strongestRecentVelocity(data, fallback) {
  const result = {
    vx: cleanNumber(fallback && fallback.vx, 0, -3200, 3200),
    vy: cleanNumber(fallback && fallback.vy, 0, -3200, 3200)
  };
  const peakVX = cleanNumber(data && data.peakVX, 0, -3200, 3200);
  const peakVY = cleanNumber(data && data.peakVY, 0, -3200, 3200);
  if (Math.abs(peakVX) > Math.abs(result.vx)) result.vx = peakVX;
  if (Math.abs(peakVY) > Math.abs(result.vy)) result.vy = peakVY;
  for (const sample of cleanInputSamples(data && data.samples)) {
    if (Math.abs(sample.vx) > Math.abs(result.vx)) result.vx = sample.vx;
    if (Math.abs(sample.vy) > Math.abs(result.vy)) result.vy = sample.vy;
  }
  result.vx = cleanNumber(result.vx, 0, -2400, 2400);
  result.vy = cleanNumber(result.vy, 0, -2400, 2400);
  return result;
}

function lobbyStatus(lobby) {
  if (!lobby) return 'Closed';
  if (lobby.players.length >= PLAYER_LIMIT) return lobby.status === 'playing' ? 'Full' : 'Full';
  return 'Waiting';
}

function serializeState(state) {
  return {
    seq: state.seq || 0,
    serverTime: Number(state.serverTime) || Date.now(),
    simulationTime: Number(state.simulationTime) || 0,
    tickTime: Number(state.tickTime) || 0,
    eventSeq: Number(state.eventSeq) || 0,
    roundSeq: Number(state.roundSeq) || 0,
    roundState: state.roundState || 'serving',
    serveColor: state.serveColor || 'red',
    nextServeColor: state.nextServeColor || state.serveColor || 'red',
    scores: {
      red: Number(state.scores && state.scores.red) || 0,
      blue: Number(state.scores && state.scores.blue) || 0
    },
    winScore: cleanWinScore(state.winScore),
    winner: state.winner || null,
    scoreEventSeq: Number(state.scoreEventSeq) || 0,
    ball: cloneBall(state.ball),
    event: state.event || { seq: 0, type: 'none', role: null }
  };
}

function publicLobby(lobby) {
  return {
    lobbyId: lobby.lobbyId,
    gameNumber: lobby.gameNumber,
    status: lobbyStatus(lobby),
    playerCount: lobby.players.length,
    hostColour: lobby.hostColour || null,
    isPublic: !!lobby.isPublic,
    winScore: cleanWinScore(lobby.winScore),
    createdAt: lobby.createdAt,
    updatedAt: lobby.updatedAt,
    state: lobby.state ? serializeState(lobby.state) : null,
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

function broadcastLobby(lobby, extra = {}) {
  const payload = Object.assign({ type: 'lobbyUpdate', lobby: publicLobby(lobby) }, extra);
  for (const player of lobby.players) send(player.ws, payload);
}

function broadcastState(lobby, force = false) {
  if (!lobby || !lobby.state) return;
  const now = Date.now();
  // Bump seq once per call. Each call corresponds to one logical state
  // version; a player who is gated for this tick will see seq jumps but
  // their reconciler tolerates that fine. Per-player gating below means a
  // 30 Hz mobile client doesn't get hammered with 62.5 Hz snapshots.
  lobby.state.seq = (lobby.state.seq || 0) + 1;
  lobby.state.serverTime = now;
  lobby.state.simulationTime = Number(lobby.simulationTime) || 0;
  lobby.state.tickTime = SIM_TICK_MS / 1000;
  lobby.state.eventSeq = Number(lobby.eventSeq) || 0;
  lobby.state.roundSeq = Number(lobby.roundSeq) || 0;
  const payload = { type: 'gameState', state: serializeState(lobby.state) };
  // Track lobby-wide broadcast time too, for any external code that may
  // want it (and so the previous behavior is unchanged when no player has
  // declared a custom interval).
  let anySent = false;
  for (const player of lobby.players) {
    const interval = player.broadcastIntervalMs || STATE_BROADCAST_MS;
    if (!force && now - (player.lastBroadcastAt || 0) < interval) continue;
    player.lastBroadcastAt = now;
    send(player.ws, payload);
    anySent = true;
  }
  if (anySent) lobby.lastBroadcastAt = now;
}

function currentPlayer(ws) {
  const lobby = ws.lobbyId ? lobbies.get(ws.lobbyId) : null;
  if (!lobby) return { lobby: null, player: null };
  return { lobby, player: lobby.players.find((p) => p.ws === ws) || null };
}

function playerForRole(lobby, role) {
  return lobby.players.find((player) => player.role === role) || null;
}

function roleForColour(lobby, colour) {
  const player = lobby.players.find((item) => item.colour === colour);
  if (player) return player.role;
  return colour === 'blue' ? 'guest' : 'host';
}

function colourForRole(lobby, role) {
  const player = playerForRole(lobby, role);
  return (player && player.colour) || (role === 'guest' ? 'blue' : 'red');
}

function servingRole(lobby) {
  return roleForColour(lobby, lobby.state.serveColor || 'red');
}

function otherRole(role) {
  return role === 'host' ? 'guest' : 'host';
}

function makeEvent(lobby, type, role) {
  lobby.eventSeq = (lobby.eventSeq || 0) + 1;
  if (type === 'serve' || type === 'score') lobby.roundSeq = (lobby.roundSeq || 0) + 1;
  lobby.state.event = {
    seq: lobby.eventSeq,
    type,
    role,
    serverTime: Date.now(),
    simulationTime: Number(lobby.simulationTime) || 0,
    roundSeq: Number(lobby.roundSeq) || 0
  };
  lobby.state.eventSeq = lobby.eventSeq;
  lobby.state.roundSeq = Number(lobby.roundSeq) || 0;
  lobby.forceBroadcast = true;
}

function initialState(lobby) {
  lobby.eventSeq = 0;
  lobby.roundSeq = 0;
  return {
    seq: 0,
    serverTime: Date.now(),
    simulationTime: Number(lobby.simulationTime) || 0,
    tickTime: SIM_TICK_MS / 1000,
    eventSeq: 0,
    roundSeq: 0,
    roundState: 'serving',
    serveColor: 'red',
    nextServeColor: 'red',
    scores: { red: 0, blue: 0 },
    winScore: cleanWinScore(lobby.winScore),
    winner: null,
    scoreEventSeq: 0,
    ball: cloneBall(null),
    event: { seq: 0, type: 'none', role: null }
  };
}

function parkServeBall(lobby) {
  const role = servingRole(lobby);
  const ball = lobby.state.ball;
  ball.x = 0;
  ball.y = 0;
  ball.z = role === 'guest' ? CONFIG.court.depth - CONFIG.ball.serveZ : CONFIG.ball.serveZ;
  ball.vx = 0;
  ball.vy = 0;
  ball.vz = 0;
  ball.spinX = 0;
  ball.spinY = 0;
}

function contactInfoFor(lobby, role, paddle, ball, extraSlack = 0) {
  const dx = ball.x - paddle.x;
  const dy = ball.y - paddle.y;
  const hw = CONFIG.paddle.width / 2;
  const hh = CONFIG.paddle.height / 2;
  const r = Math.max(CONFIG.ball.radius, ballVisualWorldRadius(0));
  const slack = (extraSlack || 0);
  const outsideX = Math.max(0, Math.abs(dx) - hw);
  const outsideY = Math.max(0, Math.abs(dy) - hh);
  return {
    paddle,
    overlaps: Math.abs(dx) <= hw + r + slack && Math.abs(dy) <= hh + r + slack,
    offsetX: clamp(dx / hw, -1.25, 1.25),
    offsetY: clamp(dy / hh, -1.25, 1.25),
    edge: clamp(Math.sqrt((dx / hw) * (dx / hw) + (dy / hh) * (dy / hh)) / 1.35, 0, 1),
    glancing: clamp(Math.max(outsideX, outsideY) / Math.max(1, r), 0, 1)
  };
}

function contactInfo(lobby, role, extraSlack = 0) {
  const player = playerForRole(lobby, role);
  return contactInfoFor(lobby, role, player ? player.paddle : makePaddle(), lobby.state.ball, extraSlack);
}

function applyPaddleShot(ball, role, isServe, contact) {
  const paddle = contact.paddle;
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

function applyBoostIfActive(lobby, role) {
  const player = playerForRole(lobby, role);
  if (!player || !player.boostUntil || Date.now() > player.boostUntil) return;
  const ball = lobby.state.ball;
  ball.vx *= CONFIG.timing.playerBoostMultiplier;
  ball.vy *= CONFIG.timing.playerBoostMultiplier;
  ball.vz *= CONFIG.timing.playerBoostMultiplier;
  clampBallSpeed(ball);
  player.boostUntil = 0;
}

function applyPaddleHit(lobby, role, contact) {
  const ball = lobby.state.ball;
  const dir = role === 'guest' ? -1 : 1;
  const r = Math.max(CONFIG.ball.radius, ballVisualWorldRadius(0));
  ball.z = role === 'guest' ? CONFIG.court.depth - r : r;
  ball.vz = Math.abs(ball.vz) * dir;
  const gain = CONFIG.shot.hitGain;
  ball.vx *= gain;
  ball.vy *= gain;
  ball.vz *= gain;
  clampBallSpeed(ball);
  applyPaddleShot(ball, role, false, contact);
  applyBoostIfActive(lobby, role);
  makeEvent(lobby, 'hit', role);
}

function tryPaddleHit(lobby, role, slack = CONFIG.network.remotePaddleSlack) {
  const info = contactInfo(lobby, role, slack);
  if (!info.overlaps) return false;
  applyPaddleHit(lobby, role, info);
  return true;
}

function launchServe(lobby, role) {
  if (!lobby.state || lobby.state.roundState !== 'serving') return;
  if (servingRole(lobby) !== role) return;
  parkServeBall(lobby);
  const contact = contactInfo(lobby, role, CONFIG.network.remotePaddleSlack);
  if (!contact.overlaps) return;
  const ball = lobby.state.ball;
  const dir = role === 'guest' ? -1 : 1;
  ball.z = role === 'guest' ? CONFIG.court.depth - CONFIG.ball.serveZ : CONFIG.ball.serveZ;
  ball.vz = CONFIG.ball.initialZSpeed * CONFIG.ball.serveSpeedMultiplier * dir;
  ball.vx = 0;
  ball.vy = 0;
  ball.spinX = 0;
  ball.spinY = 0;
  applyPaddleShot(ball, role, true, contact);
  lobby.state.roundState = 'playing';
  makeEvent(lobby, 'serve', role);
  broadcastState(lobby, true);
}

function scorePoint(lobby, scorerRole) {
  const state = lobby.state;
  const colour = colourForRole(lobby, scorerRole) || 'red';
  state.scores[colour] = (state.scores[colour] || 0) + 1;
  state.scoreEventSeq = (state.scoreEventSeq || 0) + 1;
  makeEvent(lobby, 'score', scorerRole);
  const target = cleanWinScore(state.winScore || lobby.winScore);
  if (state.scores[colour] >= target) {
    state.winner = colour;
    state.roundState = 'ended';
    makeEvent(lobby, 'end', scorerRole);
    return;
  }
  state.nextServeColor = (state.serveColor || 'red') === 'red' ? 'blue' : 'red';
  state.roundState = 'scoreHold';
  lobby.scoreGateUntil = Date.now() + CONFIG.scoreOverlayMs;
  state.ball.vx = 0;
  state.ball.vy = 0;
  state.ball.vz = 0;
  state.ball.spinX = 0;
  state.ball.spinY = 0;
}

function queuePotentialMiss(lobby, missedRole, scorerRole) {
  if (lobby.pendingMiss && lobby.pendingMiss.missedRole === missedRole) return;
  const missBall = cloneBall(lobby.state.ball);
  lobby.pendingMiss = {
    missedRole,
    scorerRole,
    missBall,
    until: Date.now() + CONFIG.network.hitClaimGraceMs
  };
}

function positionAtCrossing(ball, prevX, prevY, prevZ, targetZ) {
  const dz = ball.z - prevZ;
  const t = Math.abs(dz) > 0.0001 ? clamp((targetZ - prevZ) / dz, 0, 1) : 1;
  ball.x = lerp(prevX, ball.x, t);
  ball.y = lerp(prevY, ball.y, t);
  ball.z = targetZ;
}

function onWallCollision(lobby) {
  makeEvent(lobby, 'wall', null);
}

function subStepBall(lobby, dt) {
  const ball = lobby.state.ball;
  ball.vx += ball.spinX * dt;
  ball.vy += ball.spinY * dt;
  const decay = Math.pow(CONFIG.ball.spinDecay, dt * 60);
  ball.spinX *= decay;
  ball.spinY *= decay;
  clampBallSpeed(ball);

  const prevX = ball.x;
  const prevY = ball.y;
  const prevZ = ball.z;
  ball.x += ball.vx * dt;
  ball.y += ball.vy * dt;
  ball.z += ball.vz * dt;

  const w = CONFIG.court.width / 2 - CONFIG.ball.radius;
  const h = CONFIG.court.height / 2 - CONFIG.ball.radius;
  let wallHit = false;
  if (ball.x < -w) { ball.x = -w; ball.vx = Math.abs(ball.vx); wallHit = true; }
  if (ball.x >  w) { ball.x =  w; ball.vx = -Math.abs(ball.vx); wallHit = true; }
  if (ball.y < -h) { ball.y = -h; ball.vy = Math.abs(ball.vy); wallHit = true; }
  if (ball.y >  h) { ball.y =  h; ball.vy = -Math.abs(ball.vy); wallHit = true; }
  if (wallHit && !lobby.pendingMiss) onWallCollision(lobby);

  const r = Math.max(CONFIG.ball.radius, ballVisualWorldRadius(0));
  if (ball.vz < 0 && prevZ >= r && ball.z <= r) {
    positionAtCrossing(ball, prevX, prevY, prevZ, r);
    if (!tryPaddleHit(lobby, 'host')) queuePotentialMiss(lobby, 'host', 'guest');
  }
  if (ball.vz > 0 && prevZ <= CONFIG.court.depth - r && ball.z >= CONFIG.court.depth - r) {
    positionAtCrossing(ball, prevX, prevY, prevZ, CONFIG.court.depth - r);
    if (!tryPaddleHit(lobby, 'guest')) queuePotentialMiss(lobby, 'guest', 'host');
  }
}

function stepBall(lobby, dt) {
  const ball = lobby.state.ball;
  const steps = Math.max(1, Math.ceil(ballSpeed(ball) * dt / CONFIG.physics.substepThreshold));
  const sub = dt / steps;
  for (let i = 0; i < steps; i++) subStepBall(lobby, sub);
}

function acceptHitClaim(lobby, player, data) {
  if (!lobby.state || lobby.state.roundState !== 'playing' || lobby.state.winner) return false;
  const role = player.role;
  const ball = lobby.state.ball;
  const r = Math.max(CONFIG.ball.radius, ballVisualWorldRadius(0));
  const plane = role === 'guest' ? CONFIG.court.depth - r : r;
  const pendingMiss = lobby.pendingMiss && lobby.pendingMiss.missedRole === role;
  const movingToward = role === 'guest' ? ball.vz > 0 : ball.vz < 0;
  const closeToPlane = Math.abs(ball.z - plane) <= CONFIG.network.hitClaimZWindow;
  if (!pendingMiss && (!movingToward || !closeToPlane)) return false;

  const claimedBall = cleanBallPayload(data.ball);
  const claimedPaddle = {
    x: cleanNumber(data.paddle && data.paddle.x),
    y: cleanNumber(data.paddle && data.paddle.y),
    vx: cleanNumber(data.paddle && data.paddle.vx),
    vy: cleanNumber(data.paddle && data.paddle.vy)
  };
  const recent = strongestRecentVelocity(data, claimedPaddle);
  claimedPaddle.vx = recent.vx;
  claimedPaddle.vy = recent.vy;
  clampPaddle(claimedPaddle);
  player.paddle = claimedPaddle;
  player.inputSamples = cleanInputSamples(data.samples);
  player.inputTime = cleanNumber(data.inputTime, Date.now(), 0, 9999999999999);

  const savedBall = cloneBall(ball);
  if (Math.abs(claimedBall.x - ball.x) <= CONFIG.network.hitClaimBallDriftLimit &&
      Math.abs(claimedBall.y - ball.y) <= CONFIG.network.hitClaimBallDriftLimit) {
    ball.x = clamp(claimedBall.x, -CONFIG.court.width / 2, CONFIG.court.width / 2);
    ball.y = clamp(claimedBall.y, -CONFIG.court.height / 2, CONFIG.court.height / 2);
  }
  ball.z = plane;
  ball.vx = claimedBall.vx;
  ball.vy = claimedBall.vy;
  ball.vz = claimedBall.vz;
  ball.spinX = claimedBall.spinX;
  ball.spinY = claimedBall.spinY;
  const contact = contactInfoFor(lobby, role, claimedPaddle, ball, CONFIG.network.hitClaimValidateSlack);
  if (!contact.overlaps) {
    Object.assign(ball, savedBall);
    return false;
  }

  lobby.pendingMiss = null;
  if (data.boost) player.boostUntil = Date.now() + 140;
  applyPaddleHit(lobby, role, contact);
  return true;
}

function simulateLobby(lobby, dt, now) {
  if (lobby.status !== 'playing' || !lobby.state) return;
  const state = lobby.state;

  if (state.winner) {
    return;
  }

  if (lobby.pendingMiss && now >= lobby.pendingMiss.until) {
    Object.assign(state.ball, lobby.pendingMiss.missBall);
    const scorer = lobby.pendingMiss.scorerRole;
    lobby.pendingMiss = null;
    scorePoint(lobby, scorer);
  } else if (state.roundState === 'scoreHold' || state.roundState === 'scoreFade') {
    if (now >= lobby.scoreGateUntil) {
      if (state.roundState === 'scoreHold') {
        state.roundState = 'scoreFade';
        lobby.scoreGateUntil = now + CONFIG.scoreFadeMs;
        lobby.forceBroadcast = true;
      } else {
        state.roundState = 'serving';
        state.serveColor = state.nextServeColor || state.serveColor || 'red';
        parkServeBall(lobby);
        lobby.forceBroadcast = true;
      }
    }
  } else if (state.roundState === 'serving') {
    parkServeBall(lobby);
  } else if (state.roundState === 'playing') {
    stepBall(lobby, dt);
  }

}

function advanceLobby(lobby, elapsedMs) {
  if (lobby.status !== 'playing' || !lobby.state) return;
  lobby.accumulatorMs = Math.min(
    MAX_ACCUMULATOR_MS,
    Math.max(0, Number(lobby.accumulatorMs) || 0) + Math.max(0, elapsedMs)
  );
  const wallNow = Date.now();
  let steps = 0;
  while (lobby.accumulatorMs >= SIM_TICK_MS && steps < MAX_SIM_STEPS) {
    simulateLobby(lobby, SIM_TICK_MS / 1000, wallNow);
    lobby.simulationTime = (Number(lobby.simulationTime) || 0) + SIM_TICK_MS;
    lobby.accumulatorMs -= SIM_TICK_MS;
    steps++;
  }
  if (steps >= MAX_SIM_STEPS) lobby.accumulatorMs = 0;
  if (steps > 0 || lobby.forceBroadcast) {
    broadcastState(lobby, lobby.forceBroadcast);
    lobby.forceBroadcast = false;
  }
}

function maybeStartLobby(lobby) {
  if (!lobby || lobby.players.length !== PLAYER_LIMIT || !lobby.hostColour) return;
  const host = playerForRole(lobby, 'host');
  const guest = playerForRole(lobby, 'guest');
  if (!host || !guest) return;
  host.colour = lobby.hostColour;
  guest.colour = lobby.hostColour === 'red' ? 'blue' : 'red';
  lobby.status = 'playing';
  lobby.simulationTime = 0;
  lobby.accumulatorMs = 0;
  lobby.state = initialState(lobby);
  lobby.pendingMiss = null;
  lobby.lastBroadcastAt = 0;
  lobby.updatedAt = Date.now();
  parkServeBall(lobby);
  const publicState = publicLobby(lobby);
  for (const player of lobby.players) {
    send(player.ws, {
      type: 'startGame',
      lobby: publicState,
      role: player.role,
      colour: player.colour,
      pin: lobby.isPublic ? null : lobby.pin,
      isPublic: !!lobby.isPublic,
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
  if (index === -1) return;

  const [removed] = lobby.players.splice(index, 1);
  ws.lobbyId = null;
  ws.role = null;

  for (const player of lobby.players) {
    send(player.ws, { type: 'opponentDisconnected', role: removed.role, reason });
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
    lobby.pendingMiss = null;
    lobby.accumulatorMs = 0;
    lobby.updatedAt = Date.now();
    broadcastLobby(lobby);
  }
  broadcastLobbyList();
}

function handleCreateLobby(ws, data = {}) {
  leaveCurrentLobby(ws, 'new lobby');
  const isPublic = data.isPublic !== false;
  const hostColour = cleanHostColour(data.hostColour);
  const lobby = {
    lobbyId: randomId(8),
    gameNumber: nextGameNumber++,
    pin: isPublic ? null : randomPin(),
    isPublic,
    winScore: cleanWinScore(data.winScore),
    players: [],
    hostColour,
    status: 'waiting',
    state: null,
    pendingMiss: null,
    simulationTime: 0,
    accumulatorMs: 0,
    createdAt: Date.now(),
    updatedAt: Date.now()
  };
  const host = {
    ws,
    role: 'host',
    token: randomId(16),
    colour: hostColour,
    paddle: makePaddle(),
    boostUntil: 0,
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
    isPublic: !!lobby.isPublic,
    winScore: cleanWinScore(lobby.winScore),
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
  if (!lobby.isPublic && pin !== lobby.pin) return sendError(ws, 'Wrong PIN');

  leaveCurrentLobby(ws, 'joining lobby');
  const guest = {
    ws,
    role: 'guest',
    token: randomId(16),
    colour: lobby.hostColour ? (lobby.hostColour === 'red' ? 'blue' : 'red') : null,
    paddle: makePaddle(),
    boostUntil: 0,
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
    pin: lobby.isPublic ? null : lobby.pin,
    isPublic: !!lobby.isPublic,
    winScore: cleanWinScore(lobby.winScore),
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
  for (const p of lobby.players) p.colour = p.role === 'host' ? colour : (colour === 'red' ? 'blue' : 'red');
  lobby.updatedAt = Date.now();
  broadcastLobby(lobby);
  maybeStartLobby(lobby);
}

function handlePaddle(ws, data) {
  const { lobby, player } = currentPlayer(ws);
  if (!lobby || !player) return;
  const paddle = {
    x: cleanNumber(data.x),
    y: cleanNumber(data.y),
    vx: cleanNumber(data.vx),
    vy: cleanNumber(data.vy)
  };
  const recent = strongestRecentVelocity(data, paddle);
  paddle.vx = recent.vx;
  paddle.vy = recent.vy;
  player.paddle = paddle;
  clampPaddle(player.paddle);
  player.inputSamples = cleanInputSamples(data.samples);
  player.inputTime = cleanNumber(data.inputTime, Date.now(), 0, 9999999999999);
  player.lastSeen = Date.now();
  if (data.boost) player.boostUntil = Date.now() + 140;
  lobby.updatedAt = player.lastSeen;

  for (const target of lobby.players) {
    if (target.ws === ws) continue;
    send(target.ws, {
      type: 'paddle',
      role: player.role,
      x: player.paddle.x,
      y: player.paddle.y,
      vx: player.paddle.vx,
      vy: player.paddle.vy,
      boost: !!data.boost,
      seq: Number(data.seq) || 0
    });
  }

  if (data.serve) launchServe(lobby, player.role);
}

function handleHitClaim(ws, data) {
  const { lobby, player } = currentPlayer(ws);
  if (!lobby || !player) return;
  player.lastSeen = Date.now();
  lobby.updatedAt = player.lastSeen;
  if (acceptHitClaim(lobby, player, data)) broadcastState(lobby, true);
}

function handleGameState(ws, data) {
  // Ball, score, and round state are server-authoritative now. Keep this
  // handler as a harmless no-op so older clients do not break the relay.
  void ws;
  void data;
}

// Client declares its desired snapshot interval. Mobile clients running at
// ~30 Hz can ask the server to throttle to that rate, halving network and
// reconciliation overhead. Defaults clamped to [16, 200] ms (62.5–5 Hz).
function handleClientHello(ws, data) {
  const { player } = currentPlayer(ws);
  if (!player) return;
  const requested = Math.floor(Number(data && data.intervalMs));
  if (!Number.isFinite(requested)) return;
  player.broadcastIntervalMs = Math.max(16, Math.min(200, requested));
}

function handleReplay(ws) {
  const { lobby, player } = currentPlayer(ws);
  if (!lobby || !player) return;
  lobby.state = initialState(lobby);
  lobby.pendingMiss = null;
  lobby.simulationTime = 0;
  lobby.accumulatorMs = 0;
  lobby.lastBroadcastAt = 0;
  lobby.status = lobby.players.length === PLAYER_LIMIT ? 'playing' : 'waiting';
  parkServeBall(lobby);
  lobby.updatedAt = Date.now();
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
    case 'paddle':
      handlePaddle(ws, data);
      break;
    case 'hitClaim':
      handleHitClaim(ws, data);
      break;
    case 'gameState':
      handleGameState(ws, data);
      break;
    case 'clientHello':
      handleClientHello(ws, data);
      break;
    case 'replay':
      handleReplay(ws);
      break;
    case 'leave':
      leaveCurrentLobby(ws, 'left');
      send(ws, { type: 'leftLobby' });
      break;
    case 'ping':
      // Echo the client's timestamp so it can measure true round-trip time.
      // Falls back to server time for older clients that didn't send `t`.
      send(ws, { type: 'pong', t: Number(data.t) || Date.now(), serverTime: Date.now() });
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

let lastSimulationLoopAt = performance.now();
setInterval(() => {
  const now = performance.now();
  const elapsedMs = Math.min(MAX_ACCUMULATOR_MS, Math.max(0, now - lastSimulationLoopAt));
  lastSimulationLoopAt = now;
  for (const lobby of lobbies.values()) advanceLobby(lobby, elapsedMs);
}, SIM_LOOP_MS);

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
