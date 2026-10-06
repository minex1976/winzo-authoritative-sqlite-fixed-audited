import express from 'express';
import { WebSocketServer } from 'ws';
import crypto from 'crypto';
import http from 'http';
import admin from 'firebase-admin';

const FIREBASE_PROJECT_ID = process.env.FIREBASE_PROJECT_ID;
const FIREBASE_CLIENT_EMAIL = process.env.FIREBASE_CLIENT_EMAIL;
const FIREBASE_PRIVATE_KEY = process.env.FIREBASE_PRIVATE_KEY?.replace(/\\n/g, '\n');
const FIREBASE_DATABASE_URL = process.env.FIREBASE_DATABASE_URL;

if (!FIREBASE_PROJECT_ID || !FIREBASE_CLIENT_EMAIL || !FIREBASE_PRIVATE_KEY || !FIREBASE_DATABASE_URL) {
  console.error('CRITICAL: Missing Firebase environment variables.');
  process.exit(1);
}
admin.initializeApp({
  credential: admin.credential.cert({ projectId: FIREBASE_PROJECT_ID, clientEmail: FIREBASE_CLIENT_EMAIL, privateKey: FIREBASE_PRIVATE_KEY }),
  databaseURL: FIREBASE_DATABASE_URL
});
const db = admin.database();

const PORT = Number(process.env.PORT || 10000);
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const ADMIN_KEY = process.env.ADMIN_KEY || '';
const NODE_ENV = process.env.NODE_ENV || 'production';
const DEV_ALLOW_ANY = NODE_ENV !== 'production' && process.env.DEV_ALLOW_ANY === 'true';
const ALLOWED_ORIGINS = String(process.env.ALLOWED_ORIGINS || process.env.ALLOWED_ORIGIN || 'https://minex1976.github.io,https://web.telegram.org')
  .split(',').map(v => v.trim()).filter(Boolean);
if (!ADMIN_KEY) console.warn('WARNING: ADMIN_KEY not set.');

const SELECTION_SECONDS = 30, SPINNING_SECONDS = 2, RESULTS_SECONDS = 6;
const MAX_PICKS = 3, PAYOUT_RATE = 0.80, REFERRAL_RATE = 0.05;
const BOT_START_WALLET = 1_000_000, BOT_MIN = 3, BOT_MAX = 6;

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

function toFiniteNumber(v, fb = 0) { const n = Number(v); return Number.isFinite(n) ? n : fb; }
function safeUser(u) { return String(u || '').replace(/[^A-Za-z0-9_]/g, '').slice(0, 32); }
function safeUid(v) { return String(v || '').replace(/[^A-Za-z0-9_:-]/g, '').slice(0, 64); }
function now() { return Date.now(); }
function constantTimeEqual(a, b) {
  const aa = Buffer.from(String(a || '')), bb = Buffer.from(String(b || ''));
  return aa.length === bb.length && crypto.timingSafeEqual(aa, bb);
}

// ===== IDENTITY (single canonical key; nothing else) =====

function uidForTelegram(telegramId) {
  const n = Number(telegramId);
  if (!Number.isSafeInteger(n) || n <= 0) return null;
  return `tg_${n}`;
}

async function resolveTelegramUser(user, referredBy = null) {
  if (!user?.id) return null;
  const uid = uidForTelegram(user.id);
  if (!uid) return null;
  const telegramId = Number(user.id);
  const username = safeUser(user.username || '');
  const displayName = ([user.first_name, user.last_name].filter(Boolean).join(' ').trim() || username || uid).slice(0, 128);

  const userRef = db.ref('users/' + uid);
  const snap = await userRef.once('value');

  if (snap.exists()) {
    const cur = snap.val() || {};
    const patch = {};
    if (Number(cur.telegram_id) !== telegramId) patch.telegram_id = telegramId;
    if (username && cur.telegram_username !== username) patch.telegram_username = username;
    if (displayName && cur.display_name !== displayName) patch.display_name = displayName;
    if (!cur.username) patch.username = uid;
    // Backfill missing wallet fields without touching existing ones.
    if (cur.main_wallet == null) patch.main_wallet = 0;
    if (cur.play_wallet == null) patch.play_wallet = 30;
    if (cur.pending_withdrawal == null) patch.pending_withdrawal = 0;
    if (cur.referral_earnings == null) patch.referral_earnings = 0;
    if (!cur.wallet_updated_at) patch.wallet_updated_at = now();
    if (Object.keys(patch).length) {
      patch.updated_at = now();
      await userRef.update(patch);
    }
    return { uid, user: { ...cur, ...patch }, telegramId, telegramUsername: username };
  }

  const t = now();
  const newUser = {
    username: uid,
    display_name: displayName,
    telegram_id: telegramId,
    telegram_username: username || null,
    main_wallet: 0,
    play_wallet: 30,
    pending_withdrawal: 0,
    referral_earnings: 0,
    wallet_updated_at: t,
    referred_by: referredBy && referredBy !== uid ? safeUser(referredBy) : null,
    created_at: t,
    updated_at: t
  };
  await userRef.set(newUser);
  console.log(`[IDENTITY] new uid=${uid} tg=${telegramId}`);
  return { uid, user: newUser, telegramId, telegramUsername: username };
}

async function readUserByUid(uid) {
  const clean = safeUid(uid); if (!clean) return null;
  const ref = db.ref('users/' + clean);
  const snap = await ref.once('value');
  return snap.exists() ? { ref, uid: clean, data: snap.val() || {} } : null;
}

// Plain read-modify-write. No .transaction() — that API returns spurious nulls
// for records that exist, which is what caused the whole class of failures.
async function mutateUser(uid, mutateFn) {
  const clean = safeUid(uid);
  if (!clean) return { ok: false, reason: 'bad-uid' };
  const userRef = db.ref('users/' + clean);
  const snap = await userRef.once('value');
  if (!snap.exists()) return { ok: false, reason: 'user-record-missing', uid: clean };

  const current = snap.val() || {};
  const out = mutateFn(current);
  if (!out || out.abort) {
    return { ok: false, reason: (out && out.reason) || 'mutation-aborted', uid: clean, user: current };
  }
  const patch = { ...(out.patch || out), updated_at: now() };
  delete patch.abort; delete patch.patch; delete patch.reason;
  await userRef.update(patch);
  const fresh = await userRef.once('value');
  return { ok: true, uid: clean, user: fresh.val() || {} };
}

// Derive the target wallet from the transaction. One computation, no lookups.
async function findUserRefForWallet(tx) {
  // 1. telegram_id → canonical uid.
  if (tx.telegram_id != null) {
    const uid = uidForTelegram(tx.telegram_id);
    if (uid) { const u = await readUserByUid(uid); if (u) return u; }
  }
  // 2. wallet_uid that already looks canonical.
  const wuid = String(tx.wallet_uid || tx.uid || '');
  if (/^tg_\d+$/.test(wuid)) { const u = await readUserByUid(wuid); if (u) return u; }
  // 3. Legacy: nothing we can do safely. Return null and let admin fix manually.
  return null;
}

// ===== Wallet helpers =====

async function walletFor(uid) {
  const clean = safeUid(uid); if (!clean) return { main: 0, play: 0, pending: 0, refEarn: 0, updatedAt: 0 };
  const snap = await db.ref('users/' + clean).once('value');
  if (!snap.exists()) return { main: 0, play: 0, pending: 0, refEarn: 0, updatedAt: 0 };
  const u = snap.val() || {};
  return {
    main: toFiniteNumber(u.main_wallet ?? u.mainWallet ?? u.main_balance ?? u.mainBalance ?? 0, 0),
    play: toFiniteNumber(u.play_wallet ?? u.playWallet ?? u.play_balance ?? u.playBalance ?? 0, 0),
    pending: toFiniteNumber(u.pending_withdrawal ?? u.pendingWithdrawal ?? 0, 0),
    refEarn: toFiniteNumber(u.referral_earnings ?? u.referralEarnings ?? 0, 0),
    updatedAt: toFiniteNumber(u.wallet_updated_at ?? u.updated_at, 0)
  };
}

async function sendWallet(uid) {
  const payload = { type: 'wallet', ...(await walletFor(uid)) };
  const set = conns.get(uid);
  if (set) for (const ws of set) if (ws.readyState === 1) ws.send(JSON.stringify(payload));
}

async function ensureUser(uid, bot, extra = {}) {
  const clean = safeUid(uid); if (!clean) return null;
  const snap = await db.ref('users/' + clean).once('value');
  if (snap.exists()) return snap.val();
  const t = now();
  const newUser = {
    username: clean,
    display_name: String(extra.display_name || clean).slice(0, 128),
    telegram_id: extra.telegram_id != null ? Number(extra.telegram_id) : null,
    telegram_username: extra.telegram_username || null,
    main_wallet: 0,
    play_wallet: bot ? BOT_START_WALLET : 30,
    pending_withdrawal: 0,
    referral_earnings: 0,
    wallet_updated_at: t,
    created_at: t,
    updated_at: t
  };
  await db.ref('users/' + clean).set(newUser);
  return newUser;
}

// ===== Notifications =====

async function addNotification(uid, data) {
  const id = data.id || crypto.randomUUID();
  const payload = { type: 'notification', id, ...data, timestamp: Number(data.timestamp || now()) };
  await db.ref('notifications/' + id).set({
    username: uid, type: data.type || 'notification', message: String(data.message || ''),
    status: data.status || null, tx_type: data.txType || null, tx_id: data.txId || null,
    timestamp: payload.timestamp, read: false
  });
  const set = conns.get(uid);
  if (set) for (const ws of set) if (ws.readyState === 1) ws.send(JSON.stringify(payload));
}

async function notifyTransaction(uid, tx) {
  await addNotification(uid, {
    type: 'transaction_result', status: tx.status, txType: tx.type, txId: tx.id, reason: tx.reason || '',
    message: tx.status === 'approved'
      ? `${tx.type === 'withdraw' ? 'Withdrawal' : 'Deposit'} approved.`
      : 'Your request was rejected by admin.'
  });
  const set = conns.get(uid);
  if (set) for (const ws of set) if (ws.readyState === 1) ws.send(JSON.stringify({
    type: 'transaction-status', id: tx.id, status: tx.status, txType: tx.type, reason: tx.reason || ''
  }));
}

// ===== Transaction creation =====

async function transactionRequest(uid, data) {
  const type = data.txType;
  const amount = Number(data.amount);
  if (!['deposit', 'withdraw'].includes(type)) return { ok: false, reason: 'bad-type' };
  if (!Number.isFinite(amount) || amount <= 0 || amount > 1_000_000) return { ok: false, reason: 'bad-amount' };

  const clientRequestId = String(data.clientRequestId || '').slice(0, 128);
  if (clientRequestId) {
    const prior = await db.ref('transaction_requests/' + uid + '/' + safeUid(clientRequestId)).once('value');
    if (prior.exists() && prior.val()?.tx_id) {
      const priorId = String(prior.val().tx_id);
      if ((await db.ref('transactions/' + priorId).once('value')).exists()) {
        return { ok: true, id: priorId, clientRequestId, duplicate: true };
      }
    }
  }

  const reference = String(data.reference || '').slice(0, 128);
  const fullName = String(data.fullName || '').slice(0, 128);
  const phone = String(data.phoneNumber || '').slice(0, 64);
  const imageData = String(data.imageData || '');
  if (imageData.length > 2_500_000) return { ok: false, reason: 'image-too-large' };

  const userSnap = await db.ref('users/' + uid).once('value');
  if (!userSnap.exists()) return { ok: false, reason: 'user-not-found' };
  const userRecord = userSnap.val() || {};

  const txRef = db.ref('transactions').push();
  const id = txRef.key;

  // Reserve withdrawal amount via mutateUser (no .transaction()).
  let reservationApplied = false;
  if (type === 'withdraw') {
    const m = await mutateUser(uid, (u) => {
      const main = toFiniteNumber(u.main_wallet ?? u.mainWallet ?? 0, 0);
      const pending = toFiniteNumber(u.pending_withdrawal ?? u.pendingWithdrawal ?? 0, 0);
      if (main - pending < amount) return { abort: true, reason: 'insufficient-main' };
      reservationApplied = true;
      return { patch: { pending_withdrawal: pending + amount, wallet_updated_at: now() } };
    });
    if (!m.ok) return { ok: false, reason: m.reason };
  }

  try {
    const telegramId = Number(userRecord.telegram_id) || null;
    await txRef.set({
      username: uid, uid, wallet_uid: uid,
      telegram_id: telegramId,
      telegram_username: userRecord.telegram_username || null,
      display_name: String(userRecord.display_name || uid).slice(0, 128),
      type, amount, status: 'pending', timestamp: now(),
      reference, full_name: fullName, phone_number: phone, image_data: imageData,
      reason: '', wallet_applied: 0, client_request_id: clientRequestId || null, updated_at: now()
    });
    if (clientRequestId) {
      await db.ref('transaction_requests/' + uid + '/' + safeUid(clientRequestId)).set({
        tx_id: id, type, amount, created_at: now()
      });
    }
    await sendWallet(uid);
    return { ok: true, id, clientRequestId, duplicate: false };
  } catch (e) {
    if (reservationApplied) {
      await mutateUser(uid, (u) => {
        const p = toFiniteNumber(u.pending_withdrawal ?? u.pendingWithdrawal ?? 0, 0);
        return { patch: { pending_withdrawal: Math.max(0, p - amount), wallet_updated_at: now() } };
      }).catch(() => {});
    }
    console.error('[TX-REQUEST]', e);
    return { ok: false, reason: 'transaction-failed' };
  }
}

async function transactionImage(uid, id, imageData) {
  if (!id || !imageData || String(imageData).length > 2_500_000) return false;
  const txRef = db.ref('transactions/' + id);
  const snap = await txRef.once('value');
  if (!snap.exists()) return false;
  const tx = snap.val();
  if (tx.wallet_uid !== uid || tx.status !== 'pending') return false;
  await txRef.update({ image_data: String(imageData), updated_at: now() });
  return true;
}

// ===== Admin =====

async function adminListTransactions() {
  const snap = await db.ref('transactions').orderByChild('status').equalTo('pending').once('value');
  const txs = [];
  snap.forEach(c => {
    const d = c.val();
    txs.push({
      id: c.key, username: d.username, uid: d.uid || d.wallet_uid || d.username,
      walletUid: d.wallet_uid || d.uid || d.username,
      telegramId: d.telegram_id || null, telegramUsername: d.telegram_username || null,
      displayName: d.display_name || d.full_name || d.username,
      type: d.type, amount: d.amount, status: d.status, timestamp: d.timestamp, reference: d.reference,
      fullName: d.full_name, phoneNumber: d.phone_number, imageData: d.image_data, reason: d.reason,
      walletApplied: Number(d.wallet_applied || 0)
    });
  });
  return txs.sort((a, b) => b.timestamp - a.timestamp);
}

async function adminDecision(id, decision, reason = '') {
  decision = String(decision || '').toLowerCase();
  if (decision === 'approve') decision = 'approved';
  if (decision === 'reject') decision = 'rejected';
  if (!['approved', 'rejected'].includes(decision)) return { ok: false, reason: 'bad-decision' };

  const txRef = db.ref('transactions/' + id);
  const txSnap = await txRef.once('value');
  if (!txSnap.exists()) return { ok: false, reason: 'not-found' };

  const tx = { ...(txSnap.val() || {}), id };
  const txAmount = toFiniteNumber(tx.amount, 0);
  if (txAmount <= 0) return { ok: false, reason: 'bad-amount' };
  if (!['deposit', 'withdraw'].includes(tx.type)) return { ok: false, reason: 'bad-type' };
  if (tx.status !== 'pending' && tx.status !== 'processing') return { ok: false, reason: 'already-decided', tx };

  // Determine the target uid.
  let targetUid = null;
  if (tx.telegram_id != null) targetUid = uidForTelegram(tx.telegram_id);
  if (!targetUid && /^tg_\d+$/.test(String(tx.wallet_uid || ''))) targetUid = String(tx.wallet_uid);
  if (!targetUid && /^tg_\d+$/.test(String(tx.uid || ''))) targetUid = String(tx.uid);

  if (!targetUid) {
    return { ok: false, reason: 'no-telegram-id-on-transaction' };
  }

  // Ensure the user record exists (create it if missing).
  const existing = await readUserByUid(targetUid);
  if (!existing) {
    await ensureUser(targetUid, false, {
      telegram_id: tx.telegram_id != null ? Number(tx.telegram_id) : null,
      telegram_username: tx.telegram_username || null,
      display_name: tx.display_name || tx.full_name || targetUid
    });
  }

  // Rejection: just mark the tx; for withdraw, release the reservation.
  if (decision === 'rejected') {
    if (tx.type === 'withdraw') {
      await mutateUser(targetUid, (u) => {
        const pending = toFiniteNumber(u.pending_withdrawal ?? u.pendingWithdrawal ?? 0, 0);
        return { patch: { pending_withdrawal: Math.max(0, pending - txAmount), wallet_updated_at: now() } };
      });
    }
    const claim = await txRef.transaction(cur => {
      if (!cur || cur.status !== 'pending') return;
      cur.status = 'rejected'; cur.reason = String(reason || '').slice(0, 256);
      cur.wallet_applied = 0; cur.wallet_uid = targetUid; cur.uid = targetUid; cur.updated_at = now();
      return cur;
    });
    if (!claim?.committed) return { ok: false, reason: 'already-decided' };
    const utx = { ...tx, status: 'rejected', reason: String(reason || '').slice(0, 256), wallet_applied: 0, wallet_uid: targetUid, uid: targetUid };
    await sendWallet(targetUid);
    await notifyTransaction(targetUid, utx);
    return { ok: true, tx: utx };
  }

  // Approval: read-modify-write the wallet.
  const mutation = await mutateUser(targetUid, (u) => {
    const ledger = (u.wallet_ledger && typeof u.wallet_ledger === 'object') ? u.wallet_ledger[id] : null;
    if (ledger) return { abort: true, reason: 'already-in-ledger' };

    const play    = toFiniteNumber(u.play_wallet        ?? u.playWallet        ?? 0, 0);
    const main    = toFiniteNumber(u.main_wallet        ?? u.mainWallet        ?? 0, 0);
    const pending = toFiniteNumber(u.pending_withdrawal ?? u.pendingWithdrawal ?? 0, 0);

    let newPlay = play, newMain = main, newPending = pending;
    if (tx.type === 'deposit') {
      newPlay = play + txAmount;
    } else {
      if (pending < txAmount) return { abort: true, reason: `pending-too-low (pending=${pending} need=${txAmount})` };
      if (main    < txAmount) return { abort: true, reason: `main-too-low (main=${main} need=${txAmount})` };
      newMain = main - txAmount;
      newPending = pending - txAmount;
    }

    const newLedger = { ...(u.wallet_ledger || {}) };
    newLedger[id] = { type: tx.type, decision: 'approved', amount: txAmount, applied_at: now() };

    return {
      patch: {
        play_wallet: newPlay,
        main_wallet: newMain,
        pending_withdrawal: newPending,
        wallet_ledger: newLedger,
        wallet_updated_at: now()
      }
    };
  });

  if (!mutation.ok) {
    console.error(`[ADMIN] approve failed id=${id} uid=${targetUid} reason="${mutation.reason}"`);
    return { ok: false, reason: `wallet-write-not-committed (${mutation.reason})` };
  }

  await txRef.update({
    status: 'approved', reason: String(reason || '').slice(0, 256),
    wallet_applied: 1, wallet_uid: targetUid, uid: targetUid, updated_at: now()
  });
  const updatedTx = { ...tx, status: 'approved', reason: String(reason || '').slice(0, 256), wallet_applied: 1, wallet_uid: targetUid, uid: targetUid };

  const newPlay = toFiniteNumber(mutation.user.play_wallet ?? 0, 0);
  console.log(`[ADMIN] approved ${tx.type} id=${id} uid=${targetUid} amount=${txAmount} → play=${newPlay}`);
  await sendWallet(targetUid);
  await notifyTransaction(targetUid, updatedTx);
  return { ok: true, tx: updatedTx };
}

// ===== Payout / referral / wager (all read-modify-write) =====

async function claimPayout(uid, amount, roomId, round, kind) {
  const amt = toFiniteNumber(amount, 0);
  if (amt <= 0) return false;
  const key = `payout:${roomId}_${round}_${uid}_${kind}`;
  const markerRef = db.ref('notifications/' + key);
  if ((await markerRef.once('value')).exists()) return false;

  const m = await mutateUser(uid, (u) => {
    const main = toFiniteNumber(u.main_wallet ?? u.mainWallet ?? 0, 0);
    return { patch: { main_wallet: main + amt, wallet_updated_at: now() } };
  });
  if (!m.ok) { console.warn(`[PAYOUT] failed uid=${uid} reason=${m.reason}`); return false; }

  await markerRef.set({ username: uid, type: 'payout_marker', read: true, timestamp: now() });
  await sendWallet(uid);
  console.log(`[PAYOUT] uid=${uid} amount=${amt} → main=${toFiniteNumber(m.user.main_wallet, 0)}`);
  return true;
}

async function creditReferral(ref, amount, roomId, round, winnerUid) {
  if (!ref || ref === winnerUid || amount <= 0) return false;
  const key = `ref:${roomId}_${round}_${winnerUid}_${ref}`;
  const markerRef = db.ref('notifications/' + key);
  if ((await markerRef.once('value')).exists()) return false;

  await ensureUser(ref, false);
  const m = await mutateUser(ref, (u) => {
    const main = toFiniteNumber(u.main_wallet ?? u.mainWallet ?? 0, 0);
    const refEarn = toFiniteNumber(u.referral_earnings ?? u.referralEarnings ?? 0, 0);
    return { patch: { main_wallet: main + amount, referral_earnings: refEarn + amount, wallet_updated_at: now() } };
  });
  if (!m.ok) return false;

  await markerRef.set({ username: ref, type: 'referral_marker', read: true, timestamp: now() });
  await sendWallet(ref);
  await addNotification(ref, { message: `🎁 You earned ${amount} Birr commission from ${winnerUid}'s win.` });
  return true;
}

async function debit(uid, amount, roomId, round) {
  const amt = toFiniteNumber(amount, 0);
  if (amt <= 0) return { ok: false, reason: 'bad-amount' };
  const m = await mutateUser(uid, (u) => {
    const bal = toFiniteNumber(u.play_wallet ?? u.playWallet ?? 0, 0);
    if (bal < amt) return { abort: true, reason: 'insufficient', balance: bal };
    return { patch: { play_wallet: bal - amt, wallet_updated_at: now() } };
  });
  if (!m.ok) { await sendWallet(uid); return { ok: false, reason: m.reason, balance: m.user?.play_wallet }; }
  await sendWallet(uid);
  return { ok: true, balance: toFiniteNumber(m.user.play_wallet, 0) };
}

async function refundWager(uid, amount, roomId, round) {
  const amt = toFiniteNumber(amount, 0);
  if (amt <= 0) return false;
  const m = await mutateUser(uid, (u) => {
    const play = toFiniteNumber(u.play_wallet ?? u.playWallet ?? 0, 0);
    return { patch: { play_wallet: play + amt, wallet_updated_at: now() } };
  });
  if (m.ok) await sendWallet(uid);
  return m.ok;
}

// ===== Game / room logic (unchanged from your last version) =====

function validateInitData(initData, maxAge = 86400) {
  if (!BOT_TOKEN) return null;
  try {
    const p = new URLSearchParams(initData);
    const hash = p.get('hash'); if (!hash) return null;
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

class Room {
  constructor(id, bet, row = null) {
    this.id = id; this.bet = bet;
    this.phase = row?.phase || 'selection';
    this.round = Number(row?.round || 1);
    this.endsAt = Number(row?.ends_at || (now() + SELECTION_SECONDS * 1000));
    this.players = new Map(); this.taken = new Map();
    this.winningNumber = row?.winning_number == null ? null : Number(row.winning_number);
    this.winners = row?.winners_json ? JSON.parse(row.winners_json) : [];
    this.prizePool = Number(row?.prize_pool || 0);
    this.lastResult = row?.last_result_json ? JSON.parse(row.last_result_json) : null;
    this.mutex = new Mutex(); this.botTimers = []; this.dirty = true;
    if (row?.taken_json) for (const [n, uid] of Object.entries(JSON.parse(row.taken_json))) this.taken.set(Number(n), uid);
    if (row?.players_json) for (const [uid, p] of Object.entries(JSON.parse(row.players_json))) this.players.set(uid, {
      uid, picks: Array.isArray(p.picks) ? p.picks.map(Number) : [], isBot: !!p.isBot, active: !!p.active, displayName: String(p.displayName || uid)
    });
  }
  snapshot() {
    const players = {};
    for (const [uid, p] of this.players) { if (!p.active && !p.isBot) continue; players[uid] = { name: p.displayName || uid, picks: [...p.picks], isBot: !!p.isBot, bet: this.bet }; }
    const active = [...this.players.values()].filter(p => p.active);
    const totalPicks = active.reduce((s, p) => s + p.picks.length, 0);
    const totalBets = totalPicks * this.bet;
    const humanBets = active.filter(p => !p.isBot).reduce((s, p) => s + p.picks.length * this.bet, 0);
    return {
      gameState: this.phase, round: this.round, betAmount: this.bet,
      selectionEndsAt: this.endsAt, endsAt: this.endsAt,
      numbersTaken: Object.fromEntries([...this.taken.keys()].map(n => [n, true])),
      players, winners: [...this.winners].map(uid => this.players.get(uid)?.displayName || uid),
      winningNumber: this.winningNumber, prizePool: this.prizePool, potAmount: this.prizePool,
      totalBets, humanBets, totalPicks,
      displayedPrizePool: Math.floor(totalBets * PAYOUT_RATE),
      onlineCount: active.length, botCount: active.filter(p => p.isBot).length,
      realPlayerCount: active.filter(p => !p.isBot).length,
      serverNow: now(), lastResult: this.lastResult
    };
  }
  async persist() {
    const players = {}; for (const [uid, p] of this.players) players[uid] = { picks: [...p.picks], isBot: !!p.isBot, active: !!p.active, displayName: p.displayName || uid };
    await db.ref('rooms/' + this.id).set({
      bet: this.bet, phase: this.phase, round: this.round, ends_at: this.endsAt,
      winning_number: this.winningNumber, winners_json: JSON.stringify(this.winners),
      prize_pool: this.prizePool,
      last_result_json: this.lastResult ? JSON.stringify(this.lastResult) : null,
      taken_json: JSON.stringify(Object.fromEntries(this.taken.entries())),
      players_json: JSON.stringify(players), updated_at: now()
    });
    this.dirty = false;
  }
  broadcast() {
    const payload = JSON.stringify({ type: 'state', room: this.id, state: this.snapshot() });
    for (const ws of wss.clients) if (ws.readyState === 1 && ws.authed && ws.roomId === this.id) ws.send(payload);
  }
}

let rooms = {};
async function loadRooms() {
  const snap = await db.ref('rooms').once('value');
  const rowMap = new Map();
  if (snap.exists()) snap.forEach(c => rowMap.set(c.key, c.val()));
  rooms = {
    room_15: new Room('room_15', 15, rowMap.get('room_15')),
    room_30: new Room('room_30', 30, rowMap.get('room_30'))
  };
  for (const r of Object.values(rooms)) for (const p of r.players.values()) if (!p.isBot) p.active = false;
}
async function saveRoom(room) { await room.persist(); room.broadcast(); }

async function pickIntent(room, uid, num) {
  return room.mutex.run(async () => {
    if (room.phase !== 'selection') return { ok: false, reason: 'not-selection' };
    if (!(num >= 1 && num <= 200)) return { ok: false, reason: 'range' };
    let p = room.players.get(uid);
    if (!p) { p = { uid, picks: [], isBot: false, active: true, displayName: uid }; room.players.set(uid, p); }
    p.active = true;
    if (p.picks.length >= MAX_PICKS) return { ok: false, reason: 'max-picks' };
    if (p.picks.includes(num)) return { ok: false, reason: 'duplicate' };
    if (room.taken.has(num)) return { ok: false, reason: 'taken' };
    const dr = await debit(uid, room.bet, room.id, room.round);
    if (!dr.ok) return { ok: false, reason: dr.reason || 'insufficient' };
    room.taken.set(num, uid);
    p.picks.push(num);
    room.dirty = true;
    return { ok: true };
  });
}

async function unpickIntent(room, uid, num) {
  return room.mutex.run(async () => {
    if (room.phase !== 'selection') return { ok: false, reason: 'not-selection' };
    const p = room.players.get(uid);
    if (!p || !p.picks.includes(num)) return { ok: false, reason: 'not-picked' };
    if (room.taken.get(num) !== uid) return { ok: false, reason: 'not-owner' };
    if (!(await refundWager(uid, room.bet, room.id, room.round))) return { ok: false, reason: 'refund-failed' };
    p.picks = p.picks.filter(n => n !== num); room.taken.delete(num); room.dirty = true;
    return { ok: true };
  });
}

async function refundDisconnectedPlayer(room, p) {
  if (!p || p.isBot || !p.picks.length) return;
  await refundWager(p.uid, room.bet * p.picks.length, room.id, room.round);
  for (const n of p.picks) if (room.taken.get(n) === p.uid) room.taken.delete(n);
  p.picks = []; p.active = false; room.dirty = true;
}

async function toSpinning(room) {
  if (room.phase !== 'selection') return;
  const all = [...room.players.values()].filter(p => p.picks.length > 0 && (p.isBot || p.active));
  const humans = all.filter(p => !p.isBot);
  const bots = all.filter(p => p.isBot);
  if (!all.length) return resetRound(room, 'no-picks');

  let winningNumber = null, winners = [], prizePool = 0;
  if (humans.length) {
    const nums = [...new Set(humans.flatMap(p => p.picks))];
    if (!nums.length) return resetRound(room, 'no-human-picks');
    winningNumber = nums[Math.floor(Math.random() * nums.length)];
    winners = humans.filter(p => p.picks.includes(winningNumber)).map(p => p.uid);
    prizePool = Math.floor(humans.reduce((s, p) => s + p.picks.length, 0) * room.bet * PAYOUT_RATE);
  } else {
    const nums = [...new Set(bots.flatMap(p => p.picks))];
    if (!nums.length) return resetRound(room, 'no-picks');
    winningNumber = nums[Math.floor(Math.random() * nums.length)];
    winners = bots.filter(p => p.picks.includes(winningNumber)).map(p => p.uid);
    prizePool = 0;
  }

  room.winningNumber = winningNumber; room.winners = winners; room.prizePool = prizePool;
  room.phase = 'spinning'; room.endsAt = now() + SPINNING_SECONDS * 1000; room.dirty = true;

  if (winners.length && prizePool > 0) {
    const share = Math.floor(prizePool / winners.length);
    for (const uid of winners) {
      if (share <= 0) break;
      const p = room.players.get(uid);
      if (!p || p.isBot) continue;
      try {
        if (!(await claimPayout(uid, share, room.id, room.round, 'win'))) continue;
        await addNotification(uid, { message: `🏆 You won ${share} Birr in round ${room.round}!`, type: 'notification' });
        const uSnap = await db.ref('users/' + uid).once('value');
        const ref = uSnap.val()?.referred_by;
        if (ref) await creditReferral(ref, Math.floor(share * REFERRAL_RATE), room.id, room.round, uid);
      } catch (e) { console.error(`[WIN] ${uid}`, e); }
    }
  }

  room.lastResult = {
    round: room.round, winningNumber,
    winners: winners.map(uid => room.players.get(uid)?.displayName || uid),
    winAmount: (winners.length && prizePool > 0) ? Math.floor(prizePool / winners.length) : 0,
    setAt: now()
  };
  console.log(`[${room.id}] r${room.round} → num=${winningNumber} winners=${room.lastResult.winners.join(',') || '—'} pool=${prizePool}`);
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
  room.round += 1; room.phase = 'selection'; room.endsAt = now() + SELECTION_SECONDS * 1000;
  room.taken.clear(); room.winningNumber = null; room.winners = []; room.prizePool = 0; room.lastResult = null;
  for (const p of room.players.values()) { p.picks = []; p.active = false; }
  room.dirty = true;
  scheduleBots(room);
}

function botRoster(count, round) {
  const pool = [...BOT_NAMES], off = ((round - 1) * 7) % pool.length;
  return pool.slice(off).concat(pool.slice(0, off)).slice(0, Math.min(count, pool.length));
}
function botUid(name) { return `__bot_${safeUser(name)}`; }
function clearBotTimers(room) { room.botTimers.forEach(clearTimeout); room.botTimers = []; }
function currentBotRoster(room) { return botRoster(BOT_MIN + (room.round % (BOT_MAX - BOT_MIN + 1)), room.round); }

function normalizeRoundBots(room) {
  const roster = new Set(currentBotRoster(room));
  const active = new Set([...roster].map(botUid));
  for (const [uid, p] of room.players) {
    if (!p.isBot) continue;
    if (!active.has(uid)) {
      for (const n of p.picks || []) if (room.taken.get(n) === uid) room.taken.delete(n);
      p.picks = []; p.active = false;
    } else { p.active = true; p.displayName = p.displayName || uid.replace(/^__bot_/, ''); }
  }
  return roster;
}

function scheduleBots(room) {
  clearBotTimers(room);
  if (room.phase !== 'selection') return;
  const roster = normalizeRoundBots(room);
  const names = [...roster]; if (!names.length) return;
  const order = [];
  for (let i = 0; i < MAX_PICKS; i++) {
    const pass = [...names];
    for (let k = pass.length - 1; k > 0; k--) { const j = Math.floor(Math.random() * (k + 1)); [pass[k], pass[j]] = [pass[j], pass[k]]; }
    order.push(...pass);
  }
  let idx = 0;
  const run = async () => {
    if (room.phase !== 'selection' || idx >= order.length) return;
    if (now() >= room.endsAt - 1000) return;
    const displayName = order[idx++], uid = botUid(displayName);
    try {
      await room.mutex.run(async () => {
        if (room.phase !== 'selection') return;
        await ensureUser(uid, true, { display_name: displayName });
        let p = room.players.get(uid);
        if (!p) { p = { uid, picks: [], isBot: true, active: true, displayName }; room.players.set(uid, p); }
        else { p.active = true; p.isBot = true; p.displayName = displayName; }
        if (p.picks.length >= MAX_PICKS) return;
        let num = null;
        for (let i = 0; i < 12; i++) { const c = 1 + Math.floor(Math.random() * 200); if (!room.taken.has(c) && !p.picks.includes(c)) { num = c; break; } }
        if (num == null) for (let c = 1; c <= 200; c++) if (!room.taken.has(c)) { num = c; break; }
        if (num == null) return;
        room.taken.set(num, uid); p.picks.push(num); room.dirty = true; room.broadcast();
      });
    } catch (e) { console.error('[BOT-PICK]', e); }
    if (room.phase !== 'selection' || idx >= order.length) return;
    room.botTimers.push(setTimeout(run, 300));
  };
  room.botTimers.push(setTimeout(run, 350));
}

setInterval(async () => {
  for (const room of Object.values(rooms)) {
    await room.mutex.run(async () => {
      const t = now();
      if (room.phase === 'selection' && t >= room.endsAt) await toSpinning(room);
      else if (room.phase === 'spinning' && t >= room.endsAt) await toResults(room);
      else if (room.phase === 'results' && t >= room.endsAt) await resetRound(room, 'results-done');
      if (room.dirty) await room.persist();
      room.broadcast();
    });
  }
}, 500);

const conns = new Map();
const wss = new WebSocketServer({ noServer: true });
function send(ws, obj) { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); }

wss.on('connection', (ws) => {
  ws.isAlive = true; ws.uid = null; ws.roomId = null; ws.authed = false; ws.isAdmin = false;
  ws.authTimer = setTimeout(() => { if (!ws.authed) { try { ws.close(4001, 'auth timeout'); } catch {} } }, 10000);
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', async raw => {
    let m; try { m = JSON.parse(raw.toString()); } catch { return; }
    try { await handle(ws, m); } catch (e) { console.error('[WS-MSG]', e); send(ws, { type: 'error', message: 'server-error' }); }
  });
  ws.on('close', async () => {
    clearTimeout(ws.authTimer);
    if (!ws.uid) return;
    const set = conns.get(ws.uid);
    if (set) { set.delete(ws); if (set.size) return; conns.delete(ws.uid); }
    if (ws.roomId) {
      const room = rooms[ws.roomId]; if (!room) return;
      await room.mutex.run(async () => {
        const p = room.players.get(ws.uid);
        if (!p || p.isBot) return;
        if (room.phase === 'selection') await refundDisconnectedPlayer(room, p);
        p.active = false; room.dirty = true;
        await room.persist(); room.broadcast();
      }).catch(err => console.error('[WS-CLOSE]', err));
    }
  });
  ws.on('error', () => {});
});

async function handle(ws, m) {
  if (m.type === 'ping') return send(ws, { type: 'pong', t: now() });

  if (m.type === 'auth') {
    const user = validateInitData(m.initData || '');
    let resolved = null;
    if (user) resolved = await resolveTelegramUser(user, m.ref || null);
    if (!resolved && DEV_ALLOW_ANY && m.devUsername && /^[A-Za-z0-9_]{1,32}$/.test(m.devUsername)) {
      const uid = safeUser(m.devUsername);
      resolved = { uid, user: await ensureUser(uid, false, { display_name: uid }) };
    }
    if (!resolved?.uid) { send(ws, { type: 'error', message: 'auth-failed', code: 'AUTH_REQUIRED' }); try { ws.close(4001, 'auth required'); } catch {} return; }
    clearTimeout(ws.authTimer);
    ws.uid = resolved.uid;
    ws.telegramId = user ? Number(user.id) : null;
    ws.telegramUsername = user ? safeUser(user.username || '') : '';
    ws.displayName = user ? ([user.first_name, user.last_name].filter(Boolean).join(' ') || user.username || resolved.uid) : resolved.uid;
    ws.authed = true;
    let set = conns.get(ws.uid); if (!set) { set = new Set(); conns.set(ws.uid, set); }
    set.add(ws);
    send(ws, { type: 'authed', uid: ws.uid, displayName: ws.displayName, telegramId: ws.telegramId, telegramUsername: ws.telegramUsername || null });
    send(ws, { type: 'wallet', ...(await walletFor(ws.uid)) });

    const txSnap = await db.ref('transactions').orderByChild('wallet_uid').equalTo(ws.uid).once('value');
    const pending = [];
    txSnap.forEach(c => { const d = c.val(); if (d.status === 'pending') pending.push({ id: c.key, type: d.type, amount: d.amount, status: d.status, timestamp: Number(d.timestamp || 0) }); });
    pending.sort((a, b) => b.timestamp - a.timestamp);
    send(ws, { type: 'pending-transactions', transactions: pending.slice(0, 10) });
    return;
  }

  if (m.type === 'admin-auth') {
    if (!ADMIN_KEY || typeof m.key !== 'string' || !constantTimeEqual(m.key, ADMIN_KEY)) {
      send(ws, { type: 'admin-auth-fail' }); try { ws.close(4003, 'admin auth failed'); } catch {} return;
    }
    ws.authed = true; ws.isAdmin = true;
    send(ws, { type: 'admin-authed' });
    return;
  }
  if (ws.isAdmin) {
    if (m.type === 'admin-list-transactions') return send(ws, { type: 'admin-transactions', transactions: await adminListTransactions() });
    if (m.type === 'admin-decision') {
      const r = await adminDecision(String(m.id || ''), String(m.decision || ''), String(m.reason || ''));
      if (!r.ok) return send(ws, { type: 'admin-error', message: r.reason });
      send(ws, { type: 'admin-decision-ok', id: m.id, status: r.tx.status, transaction: r.tx });
      send(ws, { type: 'admin-transactions', transactions: await adminListTransactions() });
      return;
    }
    return;
  }

  if (!ws.authed) return send(ws, { type: 'error', message: 'not-authed' });

  if (m.type === 'join') {
    const room = rooms[m.room]; if (!room) return send(ws, { type: 'error', message: 'bad-room' });
    if (ws.roomId && rooms[ws.roomId] && ws.roomId !== m.room) { const op = rooms[ws.roomId].players.get(ws.uid); if (op) op.active = false; }
    ws.roomId = m.room;
    const existing = room.players.get(ws.uid);
    if (existing && existing.picks.length && !existing.active) await refundDisconnectedPlayer(room, existing);
    if (!room.players.has(ws.uid)) room.players.set(ws.uid, { uid: ws.uid, picks: [], isBot: false, active: true, displayName: ws.displayName || ws.uid });
    else { const p = room.players.get(ws.uid); p.active = true; p.displayName = ws.displayName || p.displayName || ws.uid; }
    room.dirty = true; await saveRoom(room);
    await sendWallet(ws.uid);
    return;
  }
  if (m.type === 'leave') {
    const room = rooms[ws.roomId], p = room?.players.get(ws.uid);
    if (room && p && !p.isBot) { await refundDisconnectedPlayer(room, p); room.players.delete(ws.uid); room.dirty = true; await saveRoom(room); }
    ws.roomId = null; return;
  }
  if (m.type === 'pick') {
    const room = rooms[ws.roomId]; if (!room) return;
    const r = await pickIntent(room, ws.uid, Number(m.number));
    send(ws, r.ok ? { type: 'pick-ok', number: Number(m.number) } : { type: 'pick-fail', number: Number(m.number), reason: r.reason });
    send(ws, { type: 'wallet', ...(await walletFor(ws.uid)) });
    if (room.dirty) await saveRoom(room);
    return;
  }
  if (m.type === 'unpick') {
    const room = rooms[ws.roomId]; if (!room) return;
    const r = await unpickIntent(room, ws.uid, Number(m.number));
    send(ws, r.ok ? { type: 'unpick-ok', number: Number(m.number) } : { type: 'unpick-fail', number: Number(m.number), reason: r.reason });
    send(ws, { type: 'wallet', ...(await walletFor(ws.uid)) });
    if (room.dirty) await saveRoom(room);
    return;
  }
  if (m.type === 'transfer') {
    const amount = Number(m.amount);
    if (!Number.isFinite(amount) || amount <= 0 || amount > 1000000) return send(ws, { type: 'error', message: 'bad-amount' });
    const m2 = await mutateUser(ws.uid, (u) => {
      const main = toFiniteNumber(u.main_wallet ?? u.mainWallet ?? 0, 0);
      const pending = toFiniteNumber(u.pending_withdrawal ?? u.pendingWithdrawal ?? 0, 0);
      const play = toFiniteNumber(u.play_wallet ?? u.playWallet ?? 0, 0);
      if (main - pending < amount) return { abort: true, reason: 'transfer-failed' };
      return { patch: { main_wallet: main - amount, play_wallet: play + amount, wallet_updated_at: now() } };
    });
    await sendWallet(ws.uid);
    if (!m2.ok) send(ws, { type: 'error', message: 'transfer-failed' });
    return;
  }
  if (m.type === 'wallet-refresh') { await sendWallet(ws.uid); return; }
  if (m.type === 'transaction-request') {
    const r = await transactionRequest(ws.uid, m);
    if (!r.ok) return send(ws, { type: 'error', message: r.reason });
    send(ws, { type: 'transaction-created', id: r.id, clientRequestId: r.clientRequestId || null });
    send(ws, { type: 'wallet', ...(await walletFor(ws.uid)) });
    return;
  }
  if (m.type === 'transaction-image') { await transactionImage(ws.uid, String(m.id || ''), String(m.imageData || '')); return; }
  if (m.type === 'notification-read') {
    const id = String(m.id || '');
    const snap = await db.ref('notifications/' + id).once('value');
    if (snap.exists() && snap.val()?.username === ws.uid) await db.ref('notifications/' + id).update({ read: true });
    return;
  }
}

setInterval(() => { for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; try { ws.ping(); } catch {} } }, 25000);

// ===== HTTP =====

const app = express();
app.use((req, res, next) => {
  const origin = req.headers.origin || '';
  if (origin && (ALLOWED_ORIGINS.includes('*') || ALLOWED_ORIGINS.includes(origin))) {
    res.header('Access-Control-Allow-Origin', origin);
    res.header('Vary', 'Origin');
    res.header('Access-Control-Allow-Headers', 'Content-Type, X-Admin-Key');
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  }
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

function checkAdmin(req, res) {
  const supplied = String(req.get('x-admin-key') || '');
  if (!ADMIN_KEY || !constantTimeEqual(supplied, ADMIN_KEY)) { res.status(403).json({ ok: false, error: 'forbidden' }); return false; }
  return true;
}

app.get('/health', async (_, res) => {
  const roomInfo = {};
  for (const r of Object.values(rooms)) roomInfo[r.id] = { phase: r.phase, round: r.round, players: Object.keys(r.snapshot().players).length, taken: r.taken.size };
  try {
    await Promise.race([db.ref('rooms').limitToFirst(1).once('value'), new Promise((_, rj) => setTimeout(() => rj(new Error('db-timeout')), 2500))]);
    res.json({ ok: true, service: 'winzo-authoritative-server', database: 'firebase-realtime-database', websocket: true, uptime: process.uptime(), origins: ALLOWED_ORIGINS, rooms: roomInfo });
  } catch (e) { res.status(503).json({ ok: false, error: e.message || 'db-unavailable' }); }
});

app.get('/config', (_, res) => res.json({ ok: true, service: 'winzo-authoritative-server', database: 'firebase-realtime-database', websocket: true }));

app.get('/admin/pending', async (req, res) => {
  if (!checkAdmin(req, res)) return;
  try { res.json({ ok: true, transactions: await adminListTransactions() }); }
  catch (e) { res.status(503).json({ ok: false, error: e.message || 'list-failed' }); }
});

app.get('/admin/decision', async (req, res) => {
  if (!checkAdmin(req, res)) return;
  const id = String(req.query.id || '').replace(/[^A-Za-z0-9_-]/g, '').slice(0, 128);
  const decision = String(req.query.decision || '').toLowerCase();
  const reason = String(req.query.reason || '').slice(0, 256);
  if (!id) return res.status(400).json({ ok: false, error: 'bad-id' });
  const r = await adminDecision(id, decision, reason);
  if (!r.ok) return res.status(400).json({ ok: false, error: r.reason });
  res.json({ ok: true, id, status: r.tx.status, transaction: r.tx });
});

app.get('/admin/diagnose', async (req, res) => {
  if (!checkAdmin(req, res)) return;
  try {
    const txSnap = await db.ref('transactions').orderByChild('status').equalTo('pending').once('value');
    const usersSnap = await db.ref('users').once('value');
    const users = usersSnap.exists() ? usersSnap.val() : {};
    const out = [];
    txSnap.forEach(c => {
      const t = c.val() || {};
      const tid = t.telegram_id != null ? Number(t.telegram_id) : null;
      const expected = tid ? `tg_${tid}` : null;
      out.push({
        id: c.key, type: t.type, amount: t.amount,
        telegram_id: tid,
        wallet_uid: t.wallet_uid || null,
        expected_uid: expected,
        expected_exists: !!(expected && users[expected])
      });
    });
    res.json({ ok: true, pendingCount: out.length, transactions: out });
  } catch (e) { res.status(503).json({ ok: false, error: e.message || 'diagnose-failed' }); }
});

app.get('/', (_, res) => res.type('text').send('Winzo server running.'));

const server = http.createServer(app);
server.on('upgrade', (req, socket, head) => {
  const origin = req.headers.origin || '';
  const allowed = !origin || ALLOWED_ORIGINS.includes('*') || ALLOWED_ORIGINS.includes(origin);
  if (!allowed) { socket.write('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n'); socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
});

async function gracefulShutdown(sig) {
  console.log(`[SHUTDOWN] ${sig}`);
  try { for (const r of Object.values(rooms)) { await r.mutex.run(async () => { if (r.dirty) await r.persist(); }); clearBotTimers(r); } }
  catch (e) { console.error('[SHUTDOWN]', e); }
  for (const ws of wss.clients) { try { ws.close(1001, 'shutting down'); } catch {} }
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000).unref();
}
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

loadRooms().then(() => {
  server.listen(PORT, '0.0.0.0', () => console.log(`Winzo server listening on :${PORT}`));
  for (const r of Object.values(rooms)) {
    if (r.phase === 'selection' && r.endsAt <= now()) { r.endsAt = now() + SELECTION_SECONDS * 1000; r.dirty = true; }
    if (r.phase === 'selection') normalizeRoundBots(r);
    r.persist(); scheduleBots(r);
  }
  console.log(`DB: ${FIREBASE_DATABASE_URL}`);
}).catch(err => { console.error('loadRooms failed', err); process.exit(1); });
