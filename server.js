import express from 'express';
import { WebSocketServer } from 'ws';
import crypto from 'crypto';
import http from 'http';
import Database from 'better-sqlite3';

const PORT = Number(process.env.PORT || 10000);
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const ADMIN_KEY = process.env.ADMIN_KEY || '';
const NODE_ENV = process.env.NODE_ENV || 'production';
const DEV_ALLOW_ANY = NODE_ENV !== 'production' && process.env.DEV_ALLOW_ANY === 'true';
const DB_PATH = process.env.SQLITE_DB_PATH || './winzo.sqlite';

// ─── ALLOWED ORIGINS (comma-separated list from env) ───
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);

if (!ADMIN_KEY) console.warn('WARNING: ADMIN_KEY is not set. Admin approval will be disabled.');

const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');
db.exec(`
CREATE TABLE IF NOT EXISTS users (
  username TEXT PRIMARY KEY,
  main_wallet REAL NOT NULL DEFAULT 0,
  play_wallet REAL NOT NULL DEFAULT 0,
  pending_withdrawal REAL NOT NULL DEFAULT 0,
  referral_earnings REAL NOT NULL DEFAULT 0,
  referred_by TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS transactions (
  id TEXT PRIMARY KEY,
  username TEXT NOT NULL,
  type TEXT NOT NULL CHECK(type IN ('deposit','withdraw')),
  amount REAL NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('pending','approved','rejected')),
  timestamp INTEGER NOT NULL,
  reference TEXT DEFAULT '',
  full_name TEXT DEFAULT '',
  phone_number TEXT DEFAULT '',
  image_data TEXT DEFAULT '',
  reason TEXT DEFAULT '',
  wallet_applied INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status, timestamp);
CREATE INDEX IF NOT EXISTS idx_transactions_user ON transactions(username, timestamp);
CREATE TABLE IF NOT EXISTS notifications (
  id TEXT PRIMARY KEY,
  username TEXT NOT NULL,
  type TEXT NOT NULL DEFAULT 'notification',
  message TEXT NOT NULL,
  status TEXT,
  tx_type TEXT,
  tx_id TEXT,
  timestamp INTEGER NOT NULL,
  read INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications(username, timestamp);
CREATE TABLE IF NOT EXISTS rooms (
  id TEXT PRIMARY KEY,
  bet REAL NOT NULL,
  phase TEXT NOT NULL,
  round INTEGER NOT NULL,
  ends_at INTEGER NOT NULL,
  winning_number INTEGER,
  winners_json TEXT NOT NULL DEFAULT '[]',
  prize_pool REAL NOT NULL DEFAULT 0,
  last_result_json TEXT,
  taken_json TEXT NOT NULL DEFAULT '{}',
  players_json TEXT NOT NULL DEFAULT '{}',
  updated_at INTEGER NOT NULL
);
`);

const SELECTION_SECONDS = 30;
const SPINNING_SECONDS = 2;
const RESULTS_SECONDS = 5;
const MAX_PICKS = 3;
const PAYOUT_RATE = 0.80;
const REFERRAL_RATE = 0.05;
const BOT_START_WALLET = 1_000_000;
const BOT_MIN = 3;
const BOT_MAX = 6;

const BOT_NAMES = [
  'Dawit','Abel','Yonas','Biruk','Tesfaye','Mulugeta','Getachew','Henok','Ermias','Amanuel',
  'Solomon','Fikru','Samuel','Kebede','Bereket','Natnael','Tadesse','Mekonnen','Tewodros','Gebre',
  'Hailu','Abebe','Adane','Alemu','Amare','Andualem','Ayele','Bekele','Binyam','Daniel',
  'Demeke','Dereje','Desalegn','Desta','Endale','Esubalew','Fikadu','Gebremedhin','Girmay','Girma',
  'Habtamu','Hussen','Ibrahim','Kifle','Lemma','Melaku','Mengistu','Mesfin','Negash','Nega',
  'Tsegaye','Wondimu','Worku','Yohannes','Yosef','Zerihun','Zewdu','Abdi','Adugna','Afework',
  'Agumas','Alemayehu','Alula','Assefa','Ayalew','Balcha','Belete','Bogale','Chalachew','Dagne',
  'Damtew','Dires','Ewnetu','Fantahun','Fisseha','Gashaw','Gedamu','Getu','Gizaw','Goitom',
  'Goshu','Gudeta','Haile','Hiruy','Isayas','Kassa','Kefelegn','Kiflom','Lema','Melese',
  'Mihret','Million','Molla','Mulatu','Negasi','Nigussie','Petros','Robel','Sisay','Tekle'
];

function validateInitData(initData, maxAge = 86400) {
  if (!BOT_TOKEN) return null;
  try {
    const p = new URLSearchParams(initData);
    const hash = p.get('hash');
    if (!hash) return null;
    p.delete('hash');
    const dcs = [...p.entries()].map(([k, v]) => `${k}=${v}`).sort().join('\n');
    const secret = crypto.createHmac('sha256', 'WebAppData').update(BOT_TOKEN).digest();
    const mac = crypto.createHmac('sha256', secret).update(dcs).digest('hex');
    const a = Buffer.from(mac, 'hex'), b = Buffer.from(hash, 'hex');
    if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return null;
    const authDate = Number(p.get('auth_date') || 0);
    if (!authDate || Date.now() / 1000 - authDate > maxAge) return null;
    const u = p.get('user');
    return u ? JSON.parse(u) : null;
  } catch { return null; }
}

class Mutex {
  constructor() { this._t = Promise.resolve(); }
  run(fn) { const n = this._t.then(fn, fn); this._t = n.then(() => {}, () => {}); return n; }
}

function safeUser(username) { return String(username || '').replace(/[^A-Za-z0-9_]/g, '').slice(0, 32); }
function now() { return Date.now(); }

function ensureUser(username, referredBy = null, bot = false) {
  const clean = safeUser(username);
  if (!clean) return null;
  const existing = db.prepare('SELECT * FROM users WHERE username=?').get(clean);
  if (existing) return existing;
  const t = now();
  db.prepare(`INSERT INTO users(username,main_wallet,play_wallet,pending_withdrawal,referral_earnings,referred_by,created_at,updated_at)
              VALUES(?,?,?,?,?,?,?,?)`).run(
    clean, bot ? 0 : 0, bot ? BOT_START_WALLET : 30, 0, 0,
    referredBy && referredBy !== clean ? safeUser(referredBy) : null, t, t
  );
  return db.prepare('SELECT * FROM users WHERE username=?').get(clean);
}

function walletFor(username) {
  const u = ensureUser(username);
  return {
    main: Number(u?.main_wallet || 0),
    play: Number(u?.play_wallet || 0),
    pending: Number(u?.pending_withdrawal || 0),
    refEarn: Number(u?.referral_earnings || 0),
  };
}

function sendWallet(uid) {
  const payload = { type: 'wallet', ...walletFor(uid) };
  const set = conns.get(uid);
  if (set) for (const ws of set) if (ws.readyState === 1) ws.send(JSON.stringify(payload));
}

function addNotification(username, data) {
  const id = data.id || crypto.randomUUID();
  db.prepare(`INSERT OR REPLACE INTO notifications(id,username,type,message,status,tx_type,tx_id,timestamp,read)
              VALUES(?,?,?,?,?,?,?,?,0)`).run(
    id, username, data.type || 'notification', String(data.message || ''),
    data.status || null, data.txType || null, data.txId || null, Number(data.timestamp || now())
  );
  const payload = { type: 'notification', id, ...data, timestamp: Number(data.timestamp || now()) };
  const set = conns.get(username);
  if (set) for (const ws of set) if (ws.readyState === 1) ws.send(JSON.stringify(payload));
}

function notifyTransaction(username, tx) {
  addNotification(username, {
    type: 'transaction_result',
    status: tx.status,
    txType: tx.type,
    txId: tx.id,
    reason: tx.reason || '',
    message: tx.status === 'approved'
      ? `${tx.type === 'withdraw' ? 'Withdrawal' : 'Deposit'} approved.`
      : 'Your request was rejected by admin.'
  });
  const set = conns.get(username);
  if (set) for (const ws of set) if (ws.readyState === 1) ws.send(JSON.stringify({
    type: 'transaction-status', id: tx.id, status: tx.status, txType: tx.type, reason: tx.reason || ''
  }));
}

function transactionRequest(uid, data) {
  const id = safeUser(data.id) || crypto.randomUUID();
  const type = data.txType;
  const amount = Number(data.amount);
  if (!['deposit', 'withdraw'].includes(type)) return { ok: false, reason: 'bad-type' };
  if (!Number.isFinite(amount) || amount <= 0 || amount > 1_000_000) return { ok: false, reason: 'bad-amount' };
  const reference = String(data.reference || '').slice(0, 128);
  const fullName = String(data.fullName || '').slice(0, 128);
  const phone = String(data.phoneNumber || '').slice(0, 64);
  const imageData = String(data.imageData || '');
  if (imageData.length > 2_500_000) return { ok: false, reason: 'image-too-large' };

  const result = db.transaction(() => {
    const u = ensureUser(uid);
    if (!u) return { ok: false, reason: 'user' };
    if (type === 'withdraw' && Number(u.main_wallet) - Number(u.pending_withdrawal) < amount) {
      return { ok: false, reason: 'insufficient-main' };
    }
    if (type === 'withdraw') {
      db.prepare('UPDATE users SET pending_withdrawal=pending_withdrawal+?,updated_at=? WHERE username=?').run(amount, now(), uid);
    }
    db.prepare(`INSERT INTO transactions(id,username,type,amount,status,timestamp,reference,full_name,phone_number,image_data,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)`).run(id, uid, type, amount, 'pending', now(), reference, fullName, phone, imageData, now());
    return { ok: true, id };
  })();
  if (result.ok) sendWallet(uid);
  return result;
}

function transactionImage(uid, id, imageData) {
  if (!id || !imageData || String(imageData).length > 2_500_000) return false;
  const r = db.prepare('UPDATE transactions SET image_data=?,updated_at=? WHERE id=? AND username=? AND status=?')
    .run(String(imageData), now(), id, uid, 'pending');
  return r.changes > 0;
}

function adminListTransactions() {
  return db.prepare(`SELECT id,username,type,amount,status,timestamp,reference,full_name AS fullName,
                     phone_number AS phoneNumber,image_data AS imageData,reason
                     FROM transactions WHERE status='pending' ORDER BY timestamp DESC`).all();
}

function adminDecision(id, decision, reason = '') {
  if (!['approved', 'rejected'].includes(decision)) return { ok: false, reason: 'bad-decision' };
  const result = db.transaction(() => {
    const tx = db.prepare('SELECT * FROM transactions WHERE id=?').get(id);
    if (!tx) return { ok: false, reason: 'not-found' };
    if (tx.status !== 'pending') return { ok: false, reason: 'already-decided' };
    const u = db.prepare('SELECT * FROM users WHERE username=?').get(tx.username);
    if (!u) return { ok: false, reason: 'user-not-found' };
    if (tx.type === 'deposit' && decision === 'approved') {
      db.prepare('UPDATE users SET play_wallet=play_wallet+?,updated_at=? WHERE username=?').run(tx.amount, now(), tx.username);
    }
    if (tx.type === 'withdraw') {
      if (decision === 'approved') {
        if (Number(u.pending_withdrawal) < Number(tx.amount) || Number(u.main_wallet) < Number(tx.amount)) return { ok: false, reason: 'reserved-balance-changed' };
        db.prepare('UPDATE users SET main_wallet=main_wallet-?,pending_withdrawal=pending_withdrawal-?,updated_at=? WHERE username=?')
          .run(tx.amount, tx.amount, now(), tx.username);
      } else {
        db.prepare('UPDATE users SET pending_withdrawal=MAX(0,pending_withdrawal-?),updated_at=? WHERE username=?')
          .run(tx.amount, now(), tx.username);
      }
    }
    db.prepare('UPDATE transactions SET status=?,reason=?,wallet_applied=?,updated_at=? WHERE id=?')
      .run(decision, String(reason || '').slice(0, 256), decision === 'rejected' ? 0 : 1, now(), id);
    return { ok: true, tx: db.prepare('SELECT * FROM transactions WHERE id=?').get(id) };
  })();
  if (result.ok) {
    sendWallet(result.tx.username);
    notifyTransaction(result.tx.username, result.tx);
  }
  return result;
}

function markNotificationRead(uid, id) {
  db.prepare('UPDATE notifications SET read=1 WHERE id=? AND username=?').run(id, uid);
}

function claimPayout(uid, amount, roomId, round, kind) {
  const key = `${roomId}_${round}_${uid}_${kind}`;
  const marker = db.prepare('SELECT id FROM notifications WHERE id=?').get('payout:' + key);
  if (marker) return false;
  db.prepare('UPDATE users SET main_wallet=main_wallet+?,updated_at=? WHERE username=?').run(amount, now(), uid);
  db.prepare(`INSERT INTO notifications(id,username,type,message,status,tx_type,tx_id,timestamp,read)
              VALUES(?,?,?,?,?,?,?,?,1)`).run('payout:' + key, uid, 'payout_marker', '', null, null, null, now());
  return true;
}

function creditReferral(ref, amount, roomId, round, winnerUid) {
  if (!ref || ref === winnerUid || amount <= 0) return false;
  const key = `ref:${roomId}_${round}_${winnerUid}_${ref}`;
  const marker = db.prepare('SELECT id FROM notifications WHERE id=?').get(key);
  if (marker) return false;
  ensureUser(ref);
  db.prepare('UPDATE users SET main_wallet=main_wallet+?,referral_earnings=referral_earnings+?,updated_at=? WHERE username=?')
    .run(amount, amount, now(), ref);
  db.prepare(`INSERT INTO notifications(id,username,type,message,status,tx_type,tx_id,timestamp,read)
              VALUES(?,?,?,?,?,?,?,?,1)`).run(key, ref, 'referral_marker', '', null, null, null, now());
  addNotification(ref, { message: `🎁 You earned ${amount} Birr commission from ${winnerUid}'s win.` });
  return true;
}

class Room {
  constructor(id, bet, row = null) {
    this.id = id; this.bet = bet;
    this.phase = row?.phase || 'selection';
    this.round = Number(row?.round || 1);
    this.endsAt = Number(row?.ends_at || (now() + SELECTION_SECONDS * 1000));
    this.players = new Map();
    this.taken = new Map();
    this.winningNumber = row?.winning_number == null ? null : Number(row.winning_number);
    this.winners = JSON.parse(row?.winners_json || '[]');
    this.prizePool = Number(row?.prize_pool || 0);
    this.lastResult = row?.last_result_json ? JSON.parse(row.last_result_json) : null;
    this.mutex = new Mutex(); this.botTimers = []; this.dirty = true;
    if (row?.taken_json) for (const [n, uid] of Object.entries(JSON.parse(row.taken_json))) this.taken.set(Number(n), uid);
    if (row?.players_json) {
      const saved = JSON.parse(row.players_json);
      for (const [uid, p] of Object.entries(saved)) this.players.set(uid, {
        uid,
        picks: Array.isArray(p.picks) ? p.picks.map(Number) : [],
        isBot: !!p.isBot,
        active: !!p.active,
        displayName: String(p.displayName || uid)
      });
    }
  }
  snapshot() {
    const players = {};
    for (const [uid, p] of this.players) {
      if (!p.active && !p.isBot) continue;
      players[uid] = { name: p.displayName || uid, picks: [...p.picks], isBot: !!p.isBot, bet: this.bet };
    }
    return {
      gameState: this.phase, round: this.round, betAmount: this.bet,
      selectionEndsAt: this.endsAt, endsAt: this.endsAt,
      numbersTaken: Object.fromEntries([...this.taken.keys()].map(n => [n, true])),
      players,
      winners: [...this.winners].map(uid => this.players.get(uid)?.displayName || uid),
      winningNumber: this.winningNumber,
      prizePool: this.prizePool, potAmount: this.prizePool,
      totalBets: [...this.players.values()].filter(p => p.active || p.isBot).reduce((s, p) => s + p.picks.length * this.bet, 0),
      serverNow: now(), lastResult: this.lastResult,
    };
  }
  persist() {
    const players = {};
    for (const [uid, p] of this.players) players[uid] = {
      picks: [...p.picks],
      isBot: !!p.isBot,
      active: !!p.active,
      displayName: p.displayName || uid
    };
    const taken = Object.fromEntries(this.taken.entries());
    db.prepare(`INSERT INTO rooms(id,bet,phase,round,ends_at,winning_number,winners_json,prize_pool,last_result_json,taken_json,players_json,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET bet=excluded.bet,phase=excluded.phase,round=excluded.round,ends_at=excluded.ends_at,
                winning_number=excluded.winning_number,winners_json=excluded.winners_json,prize_pool=excluded.prize_pool,
                last_result_json=excluded.last_result_json,taken_json=excluded.taken_json,players_json=excluded.players_json,updated_at=excluded.updated_at`)
      .run(this.id, this.bet, this.phase, this.round, this.endsAt, this.winningNumber, JSON.stringify(this.winners), this.prizePool,
        this.lastResult ? JSON.stringify(this.lastResult) : null, JSON.stringify(taken), JSON.stringify(players), now());
    this.dirty = false;
  }
  broadcast() {
    const payload = JSON.stringify({ type: 'state', room: this.id, state: this.snapshot() });
    for (const ws of wss.clients) if (ws.readyState === 1 && ws.authed && ws.roomId === this.id) ws.send(payload);
  }
  push() { this.persist(); this.broadcast(); }
}

const rows = db.prepare('SELECT * FROM rooms WHERE id IN (?,?)').all('room_15','room_30');
const rowMap = new Map(rows.map(r => [r.id, r]));
const rooms = {
  room_15: new Room('room_15', 15, rowMap.get('room_15')),
  room_30: new Room('room_30', 30, rowMap.get('room_30')),
};

for (const room of Object.values(rooms)) { for (const p of room.players.values()) if (!p.isBot) p.active = false; }

function saveRoom(room) { room.persist(); room.broadcast(); }

function debit(uid, amount, roomId, round) {
  const result = db.transaction(() => {
    const u = db.prepare('SELECT * FROM users WHERE username=?').get(uid);
    if (!u || Number(u.play_wallet) < amount) return false;
    db.prepare('UPDATE users SET play_wallet=play_wallet-?,updated_at=? WHERE username=?').run(amount, now(), uid);
    return true;
  })();
  if (result) sendWallet(uid);
  return result;
}

function refundWager(uid, amount, roomId, round) {
  const result = db.transaction(() => {
    const u = db.prepare('SELECT * FROM users WHERE username=?').get(uid);
    if (!u) return false;
    db.prepare('UPDATE users SET play_wallet=play_wallet+?,updated_at=? WHERE username=?').run(amount, now(), uid);
    return true;
  })();
  if (result) sendWallet(uid);
  return result;
}

async function pickIntent(room, uid, num) {
  return room.mutex.run(async () => {
    if (room.phase !== 'selection') return { ok:false, reason:'not-selection' };
    if (!(num >= 1 && num <= 200)) return { ok:false, reason:'range' };
    let p = room.players.get(uid);
    if (!p) { p = { uid, picks:[], isBot:false, active:true, displayName:uid }; room.players.set(uid,p); }
    p.active = true;
    if (p.picks.length >= MAX_PICKS) return { ok:false, reason:'max-picks' };
    if (p.picks.includes(num)) return { ok:false, reason:'duplicate' };
    if (room.taken.has(num)) return { ok:false, reason:'taken' };
    if (!debit(uid, room.bet, room.id, room.round)) return { ok:false, reason:'insufficient' };
    room.taken.set(num, uid); p.picks.push(num); room.dirty = true;
    return { ok:true };
  });
}

async function unpickIntent(room, uid, num) {
  return room.mutex.run(async () => {
    if (room.phase !== 'selection') return { ok:false, reason:'not-selection' };
    const p = room.players.get(uid);
    if (!p || !p.picks.includes(num)) return { ok:false, reason:'not-picked' };
    if (room.taken.get(num) !== uid) return { ok:false, reason:'not-owner' };
    if (!refundWager(uid, room.bet, room.id, room.round)) return { ok:false, reason:'refund-failed' };
    p.picks = p.picks.filter(n => n !== num); room.taken.delete(num); room.dirty = true;
    return { ok:true };
  });
}

async function refundDisconnectedPlayer(room, p) {
  if (!p || p.isBot || !p.picks.length) return;
  const amount = room.bet * p.picks.length;
  await refundWager(p.uid, amount, room.id, room.round);
  for (const n of p.picks) if (room.taken.get(n) === p.uid) room.taken.delete(n);
  p.picks = []; p.active = false; room.dirty = true;
}

async function toSpinning(room) {
  if (room.phase !== 'selection') return;
  const withPicks = [...room.players.values()].filter(p => p.picks.length > 0 && (p.isBot || p.active));
  const humans = withPicks.filter(p => !p.isBot);
  if (!humans.length) return resetRound(room, 'no-humans');
  const nums = [...room.taken.keys()];
  if (!nums.length) return resetRound(room, 'no-picks');
  const winningNumber = nums[Math.floor(Math.random() * nums.length)];
  const winners = withPicks.filter(p => p.picks.includes(winningNumber)).map(p => p.uid);
  const totalPicks = withPicks.reduce((s,p) => s + p.picks.length, 0);
  const prizePool = Math.floor(totalPicks * room.bet * PAYOUT_RATE);
  room.winningNumber = winningNumber; room.winners = winners; room.prizePool = prizePool;
  room.phase = 'spinning'; room.endsAt = now() + SPINNING_SECONDS * 1000; room.dirty = true;
  for (const p of withPicks.filter(p => !p.isBot)) {
    if (p.active) db.prepare('UPDATE users SET updated_at=? WHERE username=?').run(now(), p.uid);
  }
  if (winners.length) {
    const share = Math.floor(prizePool / winners.length);
    for (const uid of winners) {
      if (!claimPayout(uid, share, room.id, room.round, 'win')) continue;
      addNotification(uid, { message:`🏆 You won ${share} Birr in round ${room.round}!`, type:'notification' });
      const u = db.prepare('SELECT referred_by FROM users WHERE username=?').get(uid);
      const ref = u?.referred_by;
      if (ref) creditReferral(ref, Math.floor(share * REFERRAL_RATE), room.id, room.round, uid);
    }
  }
  room.lastResult = {
    round: room.round,
    winningNumber,
    winners: winners.map(uid => room.players.get(uid)?.displayName || uid),
    winAmount: winners.length ? Math.floor(prizePool / winners.length) : 0,
    setAt: now()
  };
  console.log(`[${room.id}] r${room.round} → spin num=${winningNumber} winners=${winners.join(',') || '—'} pool=${prizePool}`);
}

async function toResults(room) {
  if (room.phase !== 'spinning') return;
  room.phase = 'results'; room.endsAt = now() + RESULTS_SECONDS * 1000; room.dirty = true;
}

async function resetRound(room, why) {
  for (const p of room.players.values()) {
    if (p.isBot || !p.picks.length) continue;
    if (room.phase === 'selection' || !p.active) await refundWager(p.uid, room.bet * p.picks.length, room.id, room.round);
  }
  room.round += 1; room.phase='selection'; room.endsAt=now()+SELECTION_SECONDS*1000;
  room.taken.clear(); room.winningNumber=null; room.winners=[]; room.prizePool=0; room.lastResult=null;
  for (const p of room.players.values()) { p.picks=[]; if (!p.isBot) p.active=false; }
  room.dirty=true;
  console.log(`[${room.id}] reset (${why}) → round ${room.round}`);
  scheduleBots(room);
}

function botRoster(count, round) {
  const pool=[...BOT_NAMES]; const off=((round-1)*7)%pool.length;
  const rot=pool.slice(off).concat(pool.slice(0,off));
  return rot.slice(0,Math.min(count,pool.length));
}
function botUid(displayName) { return `__bot_${safeUser(displayName)}`; }
function clearBotTimers(room) { room.botTimers.forEach(clearTimeout); room.botTimers=[]; }
function scheduleBots(room) {
  clearBotTimers(room);
  const count = BOT_MIN + (room.round % (BOT_MAX - BOT_MIN + 1));
  const roster = botRoster(count, room.round); const windowMs=SELECTION_SECONDS*1000-8000;
  roster.forEach((displayName,i)=>{
    const uid = botUid(displayName);
    const delay=2000+(windowMs/roster.length)*i+Math.random()*1500;
    const t=setTimeout(async()=>{ await room.mutex.run(async()=>{
      if(room.phase!=='selection') return;
      ensureUser(uid,null,true);
      let p=room.players.get(uid);
      if(!p){
        p={uid,picks:[],isBot:true,active:true,displayName};
        room.players.set(uid,p);
      } else {
        p.active=true;
        p.isBot=true;
        p.displayName=displayName;
      }
      for(let k=0;k<MAX_PICKS;k++){
        if(room.phase!=='selection') return;
        const free=[]; for(let n=1;n<=200;n++) if(!room.taken.has(n)) free.push(n); if(!free.length)return;
        const num=free[Math.floor(Math.random()*free.length)]; room.taken.set(num,uid); p.picks.push(num); room.dirty=true;
        await new Promise(r=>setTimeout(r,250+Math.random()*700));
      }
    }); },delay); room.botTimers.push(t);
  });
}

setInterval(async()=>{
  for(const room of Object.values(rooms)){
    await room.mutex.run(async()=>{
      const t=now();
      if(room.phase==='selection' && t>=room.endsAt) await toSpinning(room);
      else if(room.phase==='spinning' && t>=room.endsAt) await toResults(room);
      else if(room.phase==='results' && t>=room.endsAt) await resetRound(room,'results-done');
    });
    if(room.dirty) saveRoom(room);
  }
},500);

const conns=new Map();
const wss=new WebSocketServer({noServer:true});

function send(ws,obj){ if(ws.readyState===1) ws.send(JSON.stringify(obj)); }
function sendRoomState(room){ if(room) room.broadcast(); }

wss.on('connection',(ws)=>{
  ws.isAlive=true; ws.uid=null; ws.roomId=null; ws.authed=false; ws.isAdmin=false;
  ws.on('pong',()=>{ws.isAlive=true;});
  ws.on('message',async raw=>{let m;try{m=JSON.parse(raw.toString());}catch{return}try{await handle(ws,m);}catch(e){console.error('[WS-MSG]',e);send(ws,{type:'error',message:'server-error'});}});
  ws.on('close',()=>{
    if(ws.uid){const set=conns.get(ws.uid);if(set){set.delete(ws);if(!set.size)conns.delete(ws.uid);}
      if(ws.roomId){const room=rooms[ws.roomId];const p=room?.players.get(ws.uid);if(p&&!p.isBot){p.active=false;room.dirty=true;}}
    }
  });
  ws.on('error',()=>{});
});

async function handle(ws,m){
  if(m.type==='ping') return send(ws,{type:'pong',t:now()});

  if(m.type==='auth'){
    let uid=null; const user=validateInitData(m.initData||'');
    if(user?.username) uid=safeUser(user.username);
    else if(DEV_ALLOW_ANY && m.devUsername && /^[A-Za-z0-9_]{1,32}$/.test(m.devUsername)) uid=m.devUsername;
    if(!uid) return send(ws,{type:'error',message:'auth-failed'});
    ensureUser(uid,m.ref||null,false);
    ws.uid=uid;ws.authed=true;
    let set=conns.get(uid);if(!set){set=new Set();conns.set(uid,set);}set.add(ws);
    send(ws,{type:'authed',uid}); send(ws,{type:'wallet',...walletFor(uid)});
    const pending=db.prepare("SELECT id,type,amount,status FROM transactions WHERE username=? AND status='pending' ORDER BY timestamp DESC LIMIT 10").all(uid);
    send(ws,{type:'pending-transactions',transactions:pending});
    return;
  }

  if(m.type==='admin-auth'){
    if(!ADMIN_KEY || m.key !== ADMIN_KEY) return send(ws,{type:'admin-auth-fail'});
    ws.authed=true;ws.isAdmin=true;send(ws,{type:'admin-authed'});return;
  }
  if(ws.isAdmin){
    if(m.type==='admin-list-transactions') return send(ws,{type:'admin-transactions',transactions:adminListTransactions()});
    if(m.type==='admin-decision'){
      const r=adminDecision(String(m.id||''),String(m.decision||''),String(m.reason||''));
      if(!r.ok) return send(ws,{type:'admin-error',message:r.reason});
      send(ws,{type:'admin-decision-ok',id:m.id,status:r.tx.status});
      return;
    }
    return;
  }

  if(!ws.authed) return send(ws,{type:'error',message:'not-authed'});

  if(m.type==='join'){
    const room=rooms[m.room];if(!room)return send(ws,{type:'error',message:'bad-room'});
    if(ws.roomId && rooms[ws.roomId] && ws.roomId!==m.room){ const oldPlayer=rooms[ws.roomId].players.get(ws.uid); if(oldPlayer) oldPlayer.active=false; }
    ws.roomId=m.room;
    const existing=room.players.get(ws.uid);
    if(existing && existing.picks.length && !existing.active){
      await refundDisconnectedPlayer(room,existing);
    }
    if(!room.players.has(ws.uid)) room.players.set(ws.uid,{uid:ws.uid,picks:[],isBot:false,active:true,displayName:ws.uid});
    else room.players.get(ws.uid).active=true;
    room.dirty=true; saveRoom(room); return;
  }
  if(m.type==='leave'){
    const room=rooms[ws.roomId]; const p=room?.players.get(ws.uid);
    if(room&&p&&!p.isBot){ await refundDisconnectedPlayer(room,p); room.players.delete(ws.uid); room.dirty=true;saveRoom(room); }
    ws.roomId=null;return;
  }
  if(m.type==='pick'){
    const room=rooms[ws.roomId];if(!room)return;
    const r=await pickIntent(room,ws.uid,Number(m.number));
    send(ws,r.ok?{type:'pick-ok',number:Number(m.number)}:{type:'pick-fail',number:Number(m.number),reason:r.reason});
    send(ws,{type:'wallet',...walletFor(ws.uid)});if(room.dirty)saveRoom(room);return;
  }
  if(m.type==='unpick'){
    const room=rooms[ws.roomId];if(!room)return;
    const r=await unpickIntent(room,ws.uid,Number(m.number));
    send(ws,r.ok?{type:'unpick-ok',number:Number(m.number)}:{type:'unpick-fail',number:Number(m.number),reason:r.reason});
    send(ws,{type:'wallet',...walletFor(ws.uid)});if(room.dirty)saveRoom(room);return;
  }
  if(m.type==='transfer'){
    const amount=Number(m.amount);if(!Number.isFinite(amount)||amount<=0)return;
    const ok=db.transaction(()=>{const u=db.prepare('SELECT * FROM users WHERE username=?').get(ws.uid);if(!u)return false;const available=Number(u.main_wallet)-Number(u.pending_withdrawal);if(available<amount)return false;db.prepare('UPDATE users SET main_wallet=main_wallet-?,play_wallet=play_wallet+?,updated_at=? WHERE username=?').run(amount,amount,now(),ws.uid);return true;})();
    send(ws,{type:'wallet',...walletFor(ws.uid)});if(!ok)send(ws,{type:'error',message:'transfer-failed'});return;
  }
  if(m.type==='wallet-refresh'){send(ws,{type:'wallet',...walletFor(ws.uid)});return;}
  if(m.type==='transaction-request'){
    const r=transactionRequest(ws.uid,m);if(!r.ok)return send(ws,{type:'error',message:r.reason});
    send(ws,{type:'transaction-created',id:r.id});send(ws,{type:'wallet',...walletFor(ws.uid)});return;
  }
  if(m.type==='transaction-image'){
    transactionImage(ws.uid,String(m.id||''),String(m.imageData||''));return;
  }
  if(m.type==='notification-read'){markNotificationRead(ws.uid,String(m.id||''));return;}
}

setInterval(()=>{for(const ws of wss.clients){if(!ws.isAlive){ws.terminate();continue;}ws.isAlive=false;try{ws.ping();}catch{}}},25000);

const app=express();

app.get('/health',(_,res)=>res.json({ok:true,database:'sqlite',dbPath:DB_PATH,rooms:Object.fromEntries(Object.values(rooms).map(r=>[r.id,{phase:r.phase,round:r.round,players:r.snapshot().players?Object.keys(r.snapshot().players).length:0,taken:r.taken.size}]))}));
app.get('/',(_,res)=>res.type('text').send('Winzo authoritative server is running.'));

const server=http.createServer(app);

// ─── ALLOWED ORIGINS CHECK ───
server.on('upgrade',(req,socket,head)=>{
  const origin=req.headers.origin||'';
  const ok =
    ALLOWED_ORIGINS.length === 0 ||                              // no whitelist → allow all
    ALLOWED_ORIGINS.includes(origin) ||                          // exact match
    origin.startsWith('http://localhost') ||
    origin.startsWith('http://127.0.0.1') ||
    origin === 'https://minex1976.github.io' ||
    origin === 'https://web.telegram.org' ||
    origin.startsWith('https://web.telegram.org') ||
    (DEV_ALLOW_ANY && origin.endsWith('.github.dev'));

  if(!ok){
    console.warn(`[WS-REJECT] origin="${origin}"`);
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req,socket,head,ws=>wss.emit('connection',ws,req));
});

server.listen(PORT,'0.0.0.0',()=>console.log(`Winzo SQLite server listening on :${PORT}`));

for(const room of Object.values(rooms)){
  if(room.phase==='selection' && room.endsAt<=now()) { room.endsAt=now()+SELECTION_SECONDS*1000; room.dirty=true; }
  room.persist();
  scheduleBots(room);
}
console.log(`SQLite database: ${DB_PATH}`);
console.log(`Bot pool size: ${BOT_NAMES.length}`);
console.log(`Bots per round: ${BOT_MIN}–${BOT_MAX}`);
console.log(`Allowed origins: ${ALLOWED_ORIGINS.length ? ALLOWED_ORIGINS.join(', ') : 'ALL (no whitelist)'}`);
