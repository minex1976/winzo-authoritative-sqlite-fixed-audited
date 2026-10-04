import express from 'express';
import { WebSocketServer } from 'ws';
import crypto from 'crypto';
import http from 'http';
import admin from 'firebase-admin';

// --- Firebase Initialization ---
const FIREBASE_PROJECT_ID = process.env.FIREBASE_PROJECT_ID;
const FIREBASE_CLIENT_EMAIL = process.env.FIREBASE_CLIENT_EMAIL;
const FIREBASE_PRIVATE_KEY = process.env.FIREBASE_PRIVATE_KEY?.replace(/\\n/g, '\n');
const FIREBASE_DATABASE_URL = process.env.FIREBASE_DATABASE_URL;

if (!FIREBASE_PROJECT_ID || !FIREBASE_CLIENT_EMAIL || !FIREBASE_PRIVATE_KEY || !FIREBASE_DATABASE_URL) {
  console.error('CRITICAL: Missing Firebase environment variables. Exiting.');
  process.exit(1);
}

admin.initializeApp({
  credential: admin.credential.cert({
    projectId: FIREBASE_PROJECT_ID,
    clientEmail: FIREBASE_CLIENT_EMAIL,
    privateKey: FIREBASE_PRIVATE_KEY,
  }),
  databaseURL: FIREBASE_DATABASE_URL
});

const db = admin.database();

// --- Configuration ---
const PORT = Number(process.env.PORT || 10000);
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const ADMIN_KEY = process.env.ADMIN_KEY || '';
const NODE_ENV = process.env.NODE_ENV || 'production';
const DEV_ALLOW_ANY = NODE_ENV !== 'production' && process.env.DEV_ALLOW_ANY === 'true';

if (!ADMIN_KEY) console.warn('WARNING: ADMIN_KEY is not set. Admin approval will be disabled.');

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

// --- Realtime Database Helpers ---

async function ensureUser(username, referredBy = null, bot = false) {
  const clean = safeUser(username);
  if (!clean) return null;
  const userRef = db.ref('users/' + clean);
  const snapshot = await userRef.once('value');
  if (snapshot.exists()) return snapshot.val();

  const t = now();
  const newUser = {
    username: clean,
    main_wallet: 0,
    play_wallet: bot ? BOT_START_WALLET : 30,
    pending_withdrawal: 0,
    referral_earnings: 0,
    referred_by: referredBy && referredBy !== clean ? safeUser(referredBy) : null,
    created_at: t,
    updated_at: t
  };
  await userRef.set(newUser);
  return newUser;
}

async function walletFor(username) {
  const u = await ensureUser(username);
  return {
    main: Number(u?.main_wallet || 0),
    play: Number(u?.play_wallet || 0),
    pending: Number(u?.pending_withdrawal || 0),
    refEarn: Number(u?.referral_earnings || 0),
  };
}

async function sendWallet(uid) {
  const payload = { type: 'wallet', ...(await walletFor(uid)) };
  const set = conns.get(uid);
  if (set) for (const ws of set) if (ws.readyState === 1) ws.send(JSON.stringify(payload));
}

async function addNotification(username, data) {
  const id = data.id || crypto.randomUUID();
  const payload = { type: 'notification', id, ...data, timestamp: Number(data.timestamp || now()) };
  
  await db.ref('notifications/' + id).set({
    username,
    type: data.type || 'notification',
    message: String(data.message || ''),
    status: data.status || null,
    tx_type: data.txType || null,
    tx_id: data.txId || null,
    timestamp: payload.timestamp,
    read: false
  });

  const set = conns.get(username);
  if (set) for (const ws of set) if (ws.readyState === 1) ws.send(JSON.stringify(payload));
}

async function notifyTransaction(username, tx) {
  await addNotification(username, {
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

async function transactionRequest(uid, data) {
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

  try {
    let balanceUpdated = false;
    await db.ref('users/' + uid).transaction(u => {
      if (!u) return u;
      if (type === 'withdraw' && (u.main_wallet - u.pending_withdrawal < amount)) {
        return;
      }
      if (type === 'withdraw') {
        u.pending_withdrawal = (u.pending_withdrawal || 0) + amount;
      }
      u.updated_at = now();
      balanceUpdated = true;
      return u;
    });

    if (!balanceUpdated) return { ok: false, reason: 'insufficient-main' };

    await db.ref('transactions/' + id).set({
      username: uid, type, amount, status: 'pending', timestamp: now(),
      reference, full_name: fullName, phone_number: phone, image_data: imageData,
      reason: '', wallet_applied: 0, updated_at: now()
    });

    await sendWallet(uid);
    return { ok: true, id };
  } catch (e) {
    return { ok: false, reason: e.message || 'transaction-failed' };
  }
}

async function transactionImage(uid, id, imageData) {
  if (!id || !imageData || String(imageData).length > 2_500_000) return false;
  const txRef = db.ref('transactions/' + id);
  const snap = await txRef.once('value');
  if (!snap.exists() || snap.val().username !== uid || snap.val().status !== 'pending') return false;
  await txRef.update({ image_data: String(imageData), updated_at: now() });
  return true;
}

async function adminListTransactions() {
  const snapshot = await db.ref('transactions').orderByChild('status').equalTo('pending').once('value');
  const txs = [];
  snapshot.forEach(child => {
    const d = child.val();
    txs.push({
      id: child.key, username: d.username, type: d.type, amount: d.amount,
      status: d.status, timestamp: d.timestamp, reference: d.reference,
      fullName: d.full_name, phoneNumber: d.phone_number, imageData: d.image_data, reason: d.reason
    });
  });
  return txs.sort((a, b) => b.timestamp - a.timestamp);
}

async function adminDecision(id, decision, reason = '') {
  if (!['approved', 'rejected'].includes(decision)) return { ok: false, reason: 'bad-decision' };
  try {
    const txRef = db.ref('transactions/' + id);
    const txSnap = await txRef.once('value');
    if (!txSnap.exists()) return { ok: false, reason: 'not-found' };
    const tx = txSnap.val();
    if (tx.status !== 'pending') return { ok: false, reason: 'already-decided' };

    let success = false;
    await db.ref('users/' + tx.username).transaction(u => {
      if (!u) return u;
      if (tx.type === 'deposit' && decision === 'approved') {
        u.play_wallet = (u.play_wallet || 0) + tx.amount;
      }
      if (tx.type === 'withdraw') {
        if (decision === 'approved') {
          if (Number(u.pending_withdrawal) < Number(tx.amount) || Number(u.main_wallet) < Number(tx.amount)) return;
          u.main_wallet -= tx.amount;
          u.pending_withdrawal -= tx.amount;
        } else {
          u.pending_withdrawal = Math.max(0, (u.pending_withdrawal || 0) - tx.amount);
        }
      }
      u.updated_at = now();
      success = true;
      return u;
    });

    if (!success) return { ok: false, reason: 'reserved-balance-changed' };

    await txRef.update({
      status: decision,
      reason: String(reason || '').slice(0, 256),
      wallet_applied: decision === 'rejected' ? 0 : 1,
      updated_at: now()
    });

    const updatedTx = { ...tx, status: decision, reason, id };
    await sendWallet(tx.username);
    await notifyTransaction(tx.username, updatedTx);
    return { ok: true, tx: updatedTx };
  } catch (e) {
    return { ok: false, reason: e.message };
  }
}

async function markNotificationRead(uid, id) {
  await db.ref('notifications/' + id).update({ read: true });
}

async function claimPayout(uid, amount, roomId, round, kind) {
  const key = `payout:${roomId}_${round}_${uid}_${kind}`;
  const markerRef = db.ref('notifications/' + key);
  const marker = await markerRef.once('value');
  if (marker.exists()) return false;

  let success = false;
  await db.ref('users/' + uid).transaction(u => {
    if (!u) return u;
    u.main_wallet = (u.main_wallet || 0) + amount;
    u.updated_at = now();
    success = true;
    return u;
  });

  if (success) {
    await markerRef.set({ username: uid, type: 'payout_marker', read: true, timestamp: now() });
  }
  return success;
}

async function creditReferral(ref, amount, roomId, round, winnerUid) {
  if (!ref || ref === winnerUid || amount <= 0) return false;
  const key = `ref:${roomId}_${round}_${winnerUid}_${ref}`;
  const markerRef = db.ref('notifications/' + key);
  const marker = await markerRef.once('value');
  if (marker.exists()) return false;

  await ensureUser(ref);
  let success = false;
  await db.ref('users/' + ref).transaction(u => {
    if (!u) return u;
    u.main_wallet = (u.main_wallet || 0) + amount;
    u.referral_earnings = (u.referral_earnings || 0) + amount;
    u.updated_at = now();
    success = true;
    return u;
  });

  if (success) {
    await markerRef.set({ username: ref, type: 'referral_marker', read: true, timestamp: now() });
    await addNotification(ref, { message: `🎁 You earned ${amount} Birr commission from ${winnerUid}'s win.` });
  }
  return success;
}

// --- Room / Game Logic ---

class Room {
  constructor(id, bet, row = null) {
    this.id = id; this.bet = bet;
    this.phase = row?.phase || 'selection';
    this.round = Number(row?.round || 1);
    this.endsAt = Number(row?.ends_at || (now() + SELECTION_SECONDS * 1000));
    this.players = new Map();
    this.taken = new Map();
    this.winningNumber = row?.winning_number == null ? null : Number(row.winning_number);
    this.winners = row?.winners_json ? JSON.parse(row.winners_json) : [];
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
  async persist() {
    const players = {};
    for (const [uid, p] of this.players) players[uid] = {
      picks: [...p.picks], isBot: !!p.isBot, active: !!p.active, displayName: p.displayName || uid
    };
    const taken = Object.fromEntries(this.taken.entries());
    
    await db.ref('rooms/' + this.id).set({
      bet: this.bet, phase: this.phase, round: this.round, ends_at: this.endsAt,
      winning_number: this.winningNumber, winners_json: JSON.stringify(this.winners),
      prize_pool: this.prizePool,
      last_result_json: this.lastResult ? JSON.stringify(this.lastResult) : null,
      taken_json: JSON.stringify(taken), players_json: JSON.stringify(players),
      updated_at: now()
    });
    this.dirty = false;
  }
  broadcast() {
    const payload = JSON.stringify({ type: 'state', room: this.id, state: this.snapshot() });
    for (const ws of wss.clients) if (ws.readyState === 1 && ws.authed && ws.roomId === this.id) ws.send(payload);
  }
  async push() { await this.persist(); this.broadcast(); }
}

let rooms = {};

async function loadRooms() {
  const snap = await db.ref('rooms').once('value');
  const rowMap = new Map();
  if (snap.exists()) {
    snap.forEach(child => { rowMap.set(child.key, child.val()); });
  }
  rooms = {
    room_15: new Room('room_15', 15, rowMap.get('room_15')),
    room_30: new Room('room_30', 30, rowMap.get('room_30')),
  };
  for (const room of Object.values(rooms)) { 
    for (const p of room.players.values()) if (!p.isBot) p.active = false; 
  }
}

async function saveRoom(room) { await room.persist(); room.broadcast(); }

async function debit(uid, amount, roomId, round) {
  let success = false;
  await db.ref('users/' + uid).transaction(u => {
    if (!u || Number(u.play_wallet) < amount) return;
    u.play_wallet -= amount;
    u.updated_at = now();
    success = true;
    return u;
  });
  if (success) await sendWallet(uid);
  return success;
}

async function refundWager(uid, amount, roomId, round) {
  let success = false;
  await db.ref('users/' + uid).transaction(u => {
    if (!u) return;
    u.play_wallet = (u.play_wallet || 0) + amount;
    u.updated_at = now();
    success = true;
    return u;
  });
  if (success) await sendWallet(uid);
  return success;
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
    if (!(await debit(uid, room.bet, room.id, room.round))) return { ok:false, reason:'insufficient' };
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
    if (!(await refundWager(uid, room.bet, room.id, room.round))) return { ok:false, reason:'refund-failed' };
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
  
  if (winners.length) {
    const share = Math.floor(prizePool / winners.length);
    for (const uid of winners) {
      if (!(await claimPayout(uid, share, room.id, room.round, 'win'))) continue;
      await addNotification(uid, { message:`🏆 You won ${share} Birr in round ${room.round}!`, type:'notification' });
      const uSnap = await db.ref('users/' + uid).once('value');
      const ref = uSnap.val()?.referred_by;
      if (ref) await creditReferral(ref, Math.floor(share * REFERRAL_RATE), room.id, room.round, uid);
    }
  }
  room.lastResult = {
    round: room.round, winningNumber,
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
      await ensureUser(uid,null,true);
      let p=room.players.get(uid);
      if(!p){
        p={uid,picks:[],isBot:true,active:true,displayName};
        room.players.set(uid,p);
      } else {
        p.active=true; p.isBot=true; p.displayName=displayName;
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
    if(room.dirty) await saveRoom(room);
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
    await ensureUser(uid,m.ref||null,false);
    ws.uid=uid;ws.authed=true;
    let set=conns.get(uid);if(!set){set=new Set();conns.set(uid,set);}set.add(ws);
    send(ws,{type:'authed',uid}); send(ws,{type:'wallet',...(await walletFor(uid))});
    
    const txSnap = await db.ref('transactions').orderByChild('username').equalTo(uid).once('value');
    const pending = [];
    txSnap.forEach(child => {
      const d = child.val();
      if (d.status === 'pending') pending.push({ id: child.key, type: d.type, amount: d.amount, status: d.status });
    });
    pending.sort((a,b) => b.timestamp - a.timestamp);
    send(ws,{type:'pending-transactions',transactions:pending.slice(0, 10)});
    return;
  }

  if(m.type==='admin-auth'){
    if(!ADMIN_KEY || m.key !== ADMIN_KEY) return send(ws,{type:'admin-auth-fail'});
    ws.authed=true;ws.isAdmin=true;send(ws,{type:'admin-authed'});return;
  }
  if(ws.isAdmin){
    if(m.type==='admin-list-transactions') return send(ws,{type:'admin-transactions',transactions:await adminListTransactions()});
    if(m.type==='admin-decision'){
      const r=await adminDecision(String(m.id||''),String(m.decision||''),String(m.reason||''));
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
    room.dirty=true; await saveRoom(room); return;
  }
  if(m.type==='leave'){
    const room=rooms[ws.roomId]; const p=room?.players.get(ws.uid);
    if(room&&p&&!p.isBot){ await refundDisconnectedPlayer(room,p); room.players.delete(ws.uid); room.dirty=true;await saveRoom(room); }
    ws.roomId=null;return;
  }
  if(m.type==='pick'){
    const room=rooms[ws.roomId];if(!room)return;
    const r=await pickIntent(room,ws.uid,Number(m.number));
    send(ws,r.ok?{type:'pick-ok',number:Number(m.number)}:{type:'pick-fail',number:Number(m.number),reason:r.reason});
    send(ws,{type:'wallet',...(await walletFor(ws.uid))});if(room.dirty)await saveRoom(room);return;
  }
  if(m.type==='unpick'){
    const room=rooms[ws.roomId];if(!room)return;
    const r=await unpickIntent(room,ws.uid,Number(m.number));
    send(ws,r.ok?{type:'unpick-ok',number:Number(m.number)}:{type:'unpick-fail',number:Number(m.number),reason:r.reason});
    send(ws,{type:'wallet',...(await walletFor(ws.uid))});if(room.dirty)await saveRoom(room);return;
  }
  if(m.type==='transfer'){
    const amount=Number(m.amount);if(!Number.isFinite(amount)||amount<=0)return;
    let success = false;
    await db.ref('users/' + ws.uid).transaction(u => {
      if (!u) return u;
      const available = Number(u.main_wallet) - Number(u.pending_withdrawal);
      if (available < amount) return;
      u.main_wallet -= amount;
      u.play_wallet = (u.play_wallet || 0) + amount;
      u.updated_at = now();
      success = true;
      return u;
    });
    send(ws,{type:'wallet',...(await walletFor(ws.uid))});
    if(!success) send(ws,{type:'error',message:'transfer-failed'});
    return;
  }
  if(m.type==='wallet-refresh'){send(ws,{type:'wallet',...(await walletFor(ws.uid))});return;}
  if(m.type==='transaction-request'){
    const r=await transactionRequest(ws.uid,m);if(!r.ok)return send(ws,{type:'error',message:r.reason});
    send(ws,{type:'transaction-created',id:r.id});send(ws,{type:'wallet',...(await walletFor(ws.uid))});return;
  }
  if(m.type==='transaction-image'){
    await transactionImage(ws.uid,String(m.id||''),String(m.imageData||''));return;
  }
  if(m.type==='notification-read'){await markNotificationRead(ws.uid,String(m.id||''));return;}
}

setInterval(()=>{for(const ws of wss.clients){if(!ws.isAlive){ws.terminate();continue;}ws.isAlive=false;try{ws.ping();}catch{}}},25000);

const app=express();
app.get('/health',async (_,res)=>{
  const roomInfo = {};
  for (const room of Object.values(rooms)) {
    roomInfo[room.id] = { phase: room.phase, round: room.round, players: Object.keys(room.snapshot().players).length, taken: room.taken.size };
  }
  res.json({ok:true,database:'realtime-database',rooms:roomInfo});
});
app.get('/',(_,res)=>res.type('text').send('Winzo authoritative server is running.'));

const server=http.createServer(app);

// ─── ALLOW EVERY ORIGIN (no whitelist) ───
server.on('upgrade',(req,socket,head)=>{
  const origin=req.headers.origin||'(none)';
  console.log(`[WS-ACCEPT] origin="${origin}"`);
  wss.handleUpgrade(req,socket,head,ws=>wss.emit('connection',ws,req));
});

loadRooms().then(() => {
  server.listen(PORT,'0.0.0.0',()=>console.log(`Winzo Realtime Database server listening on :${PORT}`));
  
  for(const room of Object.values(rooms)){
    if(room.phase==='selection' && room.endsAt<=now()) { room.endsAt=now()+SELECTION_SECONDS*1000; room.dirty=true; }
    room.persist();
    scheduleBots(room);
  }
  console.log(`Realtime Database: ${FIREBASE_DATABASE_URL}`);
  console.log(`Bot pool size: ${BOT_NAMES.length}`);
  console.log(`Bots per round: ${BOT_MIN}–${BOT_MAX}`);
  console.log(`Origin check: DISABLED (all origins accepted)`);
}).catch(err => {
  console.error("Failed to load rooms from Realtime Database. Check your credentials and database URL.", err);
  process.exit(1);
});
