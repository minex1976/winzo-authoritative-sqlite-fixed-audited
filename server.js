import express from 'express';
import { WebSocketServer } from 'ws';
import crypto from 'crypto';
import http from 'http';
import admin from 'firebase-admin';

// --- Firebase init ---
const FIREBASE_PROJECT_ID = process.env.FIREBASE_PROJECT_ID;
const FIREBASE_CLIENT_EMAIL = process.env.FIREBASE_CLIENT_EMAIL;
const FIREBASE_PRIVATE_KEY = process.env.FIREBASE_PRIVATE_KEY?.replace(/\\n/g, '\n');
const FIREBASE_DATABASE_URL = process.env.FIREBASE_DATABASE_URL;

if (!FIREBASE_PROJECT_ID || !FIREBASE_CLIENT_EMAIL || !FIREBASE_PRIVATE_KEY || !FIREBASE_DATABASE_URL) {
  console.error('CRITICAL: Missing Firebase environment variables. Exiting.');
  process.exit(1);
}
admin.initializeApp({
  credential: admin.credential.cert({ projectId: FIREBASE_PROJECT_ID, clientEmail: FIREBASE_CLIENT_EMAIL, privateKey: FIREBASE_PRIVATE_KEY }),
  databaseURL: FIREBASE_DATABASE_URL
});
const db = admin.database();

// --- Config ---
const PORT = Number(process.env.PORT || 10000);
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const ADMIN_KEY = process.env.ADMIN_KEY || '';
const NODE_ENV = process.env.NODE_ENV || 'production';
const DEV_ALLOW_ANY = NODE_ENV !== 'production' && process.env.DEV_ALLOW_ANY === 'true';
const ALLOWED_ORIGINS = String(process.env.ALLOWED_ORIGINS || process.env.ALLOWED_ORIGIN || 'https://minex1976.github.io,https://web.telegram.org')
  .split(',').map(v => v.trim()).filter(Boolean);

if (!ADMIN_KEY) console.warn('WARNING: ADMIN_KEY is not set. Admin approval will be disabled.');

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

// --- Helpers ---
function toFiniteNumber(v, fb = 0) { const n = Number(v); return Number.isFinite(n) ? n : fb; }
function safeUser(u) { return String(u || '').replace(/[^A-Za-z0-9_]/g, '').slice(0, 32); }
function safeUid(v) { return String(v || '').replace(/[^A-Za-z0-9_:-]/g, '').slice(0, 64); }
function now() { return Date.now(); }
function constantTimeEqual(a, b) {
  const aa = Buffer.from(String(a || '')), bb = Buffer.from(String(b || ''));
  return aa.length === bb.length && crypto.timingSafeEqual(aa, bb);
}

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

// Follow any migrated_to pointer. Legacy uids redirect to the canonical tg_<id>.
async function resolveCanonicalUid(uid) {
  const clean = safeUid(uid);
  if (!clean) return clean;
  try {
    const snap = await db.ref('users/' + clean + '/migrated_to').once('value');
    if (snap.exists()) {
      const t = safeUid(snap.val());
      if (t && t !== clean) return t;
    }
  } catch (e) { console.warn('[UID] resolve failed', e?.message || e); }
  return clean;
}

// --- User records ---
async function ensureUser(username, referredBy = null, bot = false, extra = {}) {
  const clean = safeUid(username);
  if (!clean) return null;
  const userRef = db.ref('users/' + clean);
  const snap = await userRef.once('value');

  if (snap.exists()) {
    const cur = snap.val() || {};
    if (cur.migrated_to) return cur; // never mutate a redirect
    const patch = {};
    const main = toFiniteNumber(cur.main_wallet ?? cur.mainWallet ?? cur.main_balance ?? cur.mainBalance ?? 0, 0);
    const play = toFiniteNumber(cur.play_wallet ?? cur.playWallet ?? cur.play_balance ?? cur.playBalance ?? (bot ? BOT_START_WALLET : 0), 0);
    const pending = toFiniteNumber(cur.pending_withdrawal ?? cur.pendingWithdrawal ?? 0, 0);
    const refEarn = toFiniteNumber(cur.referral_earnings ?? cur.referralEarnings ?? 0, 0);
    if (cur.main_wallet == null) patch.main_wallet = main;
    if (cur.play_wallet == null) patch.play_wallet = play;
    if (cur.pending_withdrawal == null) patch.pending_withdrawal = pending;
    if (cur.referral_earnings == null) patch.referral_earnings = refEarn;
    if (extra.telegram_id != null && (!cur.telegram_id || Number(cur.telegram_id) !== Number(extra.telegram_id))) patch.telegram_id = Number(extra.telegram_id);
    if (extra.telegram_username !== undefined && String(cur.telegram_username || '') !== String(extra.telegram_username || '')) patch.telegram_username = extra.telegram_username || null;
    if (extra.display_name && String(cur.display_name || '') !== String(extra.display_name)) patch.display_name = String(extra.display_name).slice(0, 128);
    if (!cur.username) patch.username = clean;
    if (!cur.wallet_updated_at) patch.wallet_updated_at = Number(cur.updated_at || now());
    if (Object.keys(patch).length) { patch.updated_at = now(); await userRef.update(patch); }
    return { ...cur, ...patch };
  }

  const t = now();
  const newUser = {
    username: clean,
    display_name: String(extra.display_name || clean).slice(0, 128),
    telegram_id: extra.telegram_id != null ? Number(extra.telegram_id) : null,
    telegram_username: extra.telegram_username || null,
    // Honour balances passed in via extra (used for username → tg_<id> migration).
    main_wallet:        extra.main_wallet        != null ? toFiniteNumber(extra.main_wallet, 0)        : 0,
    play_wallet:        extra.play_wallet        != null ? toFiniteNumber(extra.play_wallet, bot ? BOT_START_WALLET : 30) : (bot ? BOT_START_WALLET : 30),
    pending_withdrawal: extra.pending_withdrawal != null ? toFiniteNumber(extra.pending_withdrawal, 0) : 0,
    referral_earnings:  extra.referral_earnings  != null ? toFiniteNumber(extra.referral_earnings, 0)  : 0,
    wallet_updated_at: t,
    referred_by: referredBy && referredBy !== clean ? safeUser(referredBy) : null,
    created_at: t,
    updated_at: t
  };
  await userRef.set(newUser);
  return newUser;
}

async function readUserByUid(uid) {
  const clean = safeUid(uid); if (!clean) return null;
  const ref = db.ref('users/' + clean);
  const snap = await ref.once('value');
  return snap.exists() ? { ref, uid: clean, data: snap.val() || {} } : null;
}

async function findUserByTelegramId(telegramId) {
  const id = Number(telegramId);
  if (!Number.isSafeInteger(id) || id <= 0) return null;
  const snap = await db.ref('users').orderByChild('telegram_id').equalTo(id).once('value');
  let found = null;
  snap.forEach(c => { if (!found) found = { ref: c.ref, uid: c.key, data: c.val() || {} }; });
  return found;
}

async function findUserByStoredUsername(username) {
  const clean = safeUser(username); if (!clean) return null;
  const snap = await db.ref('users').orderByChild('username').equalTo(clean).once('value');
  let found = null;
  snap.forEach(c => { if (!found) found = { ref: c.ref, uid: c.key, data: c.val() || {} }; });
  return found;
}

async function findUserByTelegramUsername(username) {
  const clean = safeUser(username); if (!clean) return null;
  const snap = await db.ref('users').orderByChild('telegram_username').equalTo(clean).once('value');
  let found = null;
  snap.forEach(c => { if (!found) found = { ref: c.ref, uid: c.key, data: c.val() || {} }; });
  return found;
}

async function resolveTelegramUser(user, referredBy = null) {
  if (!user?.id) return null;
  const telegramId = Number(user.id);
  if (!Number.isSafeInteger(telegramId) || telegramId <= 0) return null;
  const telegramUsername = safeUser(user.username || '');
  const displayName = [user.first_name, user.last_name].filter(Boolean).join(' ').trim() || telegramUsername || `tg_${telegramId}`;
  const canonicalUid = `tg_${telegramId}`;
  const mapRef = db.ref('telegram_users/' + telegramId);

  const mappedSnap = await mapRef.once('value');
  if (mappedSnap.exists() && mappedSnap.val()?.uid) {
    const mappedUid = safeUid(mappedSnap.val().uid);
    const mappedUser = await readUserByUid(mappedUid);
    if (mappedUser && (!mappedUser.data.telegram_id || Number(mappedUser.data.telegram_id) === telegramId)) {
      await ensureUser(mappedUid, referredBy, false, { telegram_id: telegramId, display_name: displayName, telegram_username: telegramUsername || null });
      await mapRef.update({ uid: mappedUid, username: mappedUser.data.username || mappedUid, updated_at: now() });
      return { uid: mappedUid, user: (await readUserByUid(mappedUid)).data, telegramId, telegramUsername };
    }
    console.warn(`[IDENTITY] stale mapping telegram_users/${telegramId}`);
  }

  let found = await readUserByUid(canonicalUid);
  if (!found) found = await findUserByTelegramId(telegramId);

  if (found) {
    const existingTid = found.data.telegram_id != null ? Number(found.data.telegram_id) : null;
    if (existingTid && existingTid !== telegramId) {
      console.error(`[IDENTITY] conflict uid=${found.uid} tg=${existingTid} login=${telegramId}`);
      return null;
    }
    if (found.uid !== canonicalUid) {
      console.log(`[IDENTITY] migrating ${found.uid} → ${canonicalUid}`);
      const migrated = await ensureUser(canonicalUid, referredBy, false, {
        telegram_id: telegramId, display_name: displayName, telegram_username: telegramUsername || null,
        main_wallet:        found.data.main_wallet        ?? found.data.mainWallet        ?? found.data.main_balance ?? found.data.mainBalance ?? 0,
        play_wallet:        found.data.play_wallet        ?? found.data.playWallet        ?? found.data.play_balance ?? found.data.playBalance ?? 0,
        pending_withdrawal: found.data.pending_withdrawal ?? found.data.pendingWithdrawal ?? 0,
        referral_earnings:  found.data.referral_earnings  ?? found.data.referralEarnings  ?? 0
      });
      // Mark as redirect instead of deleting, so pending transactions that
      // still reference the legacy uid resolve correctly.
      await db.ref('users/' + found.uid).update({
        migrated_to: canonicalUid, migrated_at: now(), telegram_id: telegramId,
        main_wallet: 0, play_wallet: 0, pending_withdrawal: 0, referral_earnings: 0, updated_at: now()
      });
      found = { uid: canonicalUid, data: migrated };
    } else {
      found.data = await ensureUser(found.uid, referredBy, false, { telegram_id: telegramId, display_name: displayName, telegram_username: telegramUsername || null });
    }
    await mapRef.set({ uid: found.uid, username: found.data.username || found.uid, telegram_username: telegramUsername || null, updated_at: now() });
    return { uid: found.uid, user: found.data, telegramId, telegramUsername };
  }

  const created = await ensureUser(canonicalUid, referredBy, false, { telegram_id: telegramId, display_name: displayName, telegram_username: telegramUsername || null });
  if (!created) return null;
  await mapRef.set({ uid: canonicalUid, username: created.username || canonicalUid, telegram_username: telegramUsername || null, updated_at: now() });
  return { uid: canonicalUid, user: created, telegramId, telegramUsername };
}

async function walletFor(username) {
  const clean = safeUid(username);
  if (!clean) return { main: 0, play: 0, pending: 0, refEarn: 0, updatedAt: 0 };
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
  const canonical = await resolveCanonicalUid(uid);
  const payload = { type: 'wallet', ...(await walletFor(canonical)) };
  for (const key of new Set([canonical, uid])) {
    const set = conns.get(key);
    if (set) for (const ws of set) if (ws.readyState === 1) ws.send(JSON.stringify(payload));
  }
}

async function addNotification(username, data) {
  const canonical = await resolveCanonicalUid(username);
  const id = data.id || crypto.randomUUID();
  const payload = { type: 'notification', id, ...data, timestamp: Number(data.timestamp || now()) };
  await db.ref('notifications/' + id).set({
    username: canonical, type: data.type || 'notification', message: String(data.message || ''),
    status: data.status || null, tx_type: data.txType || null, tx_id: data.txId || null,
    timestamp: payload.timestamp, read: false
  });
  const set = conns.get(canonical);
  if (set) for (const ws of set) if (ws.readyState === 1) ws.send(JSON.stringify(payload));
}

async function notifyTransaction(username, tx) {
  const canonical = await resolveCanonicalUid(username);
  await addNotification(canonical, {
    type: 'transaction_result', status: tx.status, txType: tx.type, txId: tx.id, reason: tx.reason || '',
    message: tx.status === 'approved'
      ? `${tx.type === 'withdraw' ? 'Withdrawal' : 'Deposit'} approved.`
      : 'Your request was rejected by admin.'
  });
  const set = conns.get(canonical);
  if (set) for (const ws of set) if (ws.readyState === 1) ws.send(JSON.stringify({
    type: 'transaction-status', id: tx.id, status: tx.status, txType: tx.type, reason: tx.reason || ''
  }));
}

// --- Transactions ---
async function transactionRequest(uid, data) {
  const type = data.txType;
  const amount = Number(data.amount);
  if (!['deposit', 'withdraw'].includes(type)) return { ok: false, reason: 'bad-type' };
  if (!Number.isFinite(amount) || amount <= 0 || amount > 1_000_000) return { ok: false, reason: 'bad-amount' };

  const canonicalUid = await resolveCanonicalUid(uid);
  const clientRequestId = String(data.clientRequestId || '').slice(0, 128);
  const requestKey = clientRequestId ? safeUid(canonicalUid) + ':' + safeUid(clientRequestId) : '';
  if (requestKey) {
    const prior = await db.ref('transaction_requests/' + safeUid(canonicalUid) + '/' + safeUid(clientRequestId)).once('value');
    if (prior.exists() && prior.val()?.tx_id) {
      const priorId = String(prior.val().tx_id);
      const priorTx = await db.ref('transactions/' + priorId).once('value');
      if (priorTx.exists()) return { ok: true, id: priorId, clientRequestId, duplicate: true };
    }
  }

  const reference = String(data.reference || '').slice(0, 128);
  const fullName = String(data.fullName || '').slice(0, 128);
  const phone = String(data.phoneNumber || '').slice(0, 64);
  const imageData = String(data.imageData || '');
  if (imageData.length > 2_500_000) return { ok: false, reason: 'image-too-large' };

  const userRef = db.ref('users/' + canonicalUid);
  const userSnap = await userRef.once('value');
  if (!userSnap.exists()) return { ok: false, reason: 'user-not-found' };

  const txRef = db.ref('transactions').push();
  const id = txRef.key;
  let reservationApplied = false;

  try {
    if (type === 'withdraw') {
      const result = await userRef.transaction(u => {
        if (!u) return;
        const main = toFiniteNumber(u.main_wallet ?? u.mainWallet ?? u.main_balance ?? u.mainBalance ?? 0, 0);
        const pending = toFiniteNumber(u.pending_withdrawal ?? u.pendingWithdrawal ?? 0, 0);
        if (main - pending < amount) return;
        u.pending_withdrawal = pending + amount;
        u.wallet_updated_at = now(); u.updated_at = u.wallet_updated_at;
        reservationApplied = true;
        return u;
      });
      if (!result?.committed || !reservationApplied) return { ok: false, reason: 'insufficient-main' };
    }

    const userRecord = (await userRef.once('value')).val() || {};
    // Derive telegram_id from the uid itself when possible — the uid is
    // always "tg_<telegram_id>" for real players, so it can never disagree
    // with the wallet it points at.
    const m = /^tg_(\d+)$/.exec(canonicalUid);
    const derivedTid = m ? Number(m[1]) : (userRecord.telegram_id != null ? Number(userRecord.telegram_id) : null);

    await txRef.set({
      username: canonicalUid, uid: canonicalUid, wallet_uid: canonicalUid,
      telegram_id: derivedTid,
      telegram_username: userRecord.telegram_username || null,
      display_name: String(userRecord.display_name || userRecord.username || canonicalUid).slice(0, 128),
      type, amount, status: 'pending', timestamp: now(),
      reference, full_name: fullName, phone_number: phone, image_data: imageData,
      reason: '', wallet_applied: 0, client_request_id: clientRequestId || null, updated_at: now()
    });

    if (requestKey) {
      await db.ref('transaction_requests/' + safeUid(canonicalUid) + '/' + safeUid(clientRequestId)).set({
        tx_id: id, type, amount, created_at: now()
      });
    }
    await sendWallet(canonicalUid);
    return { ok: true, id, clientRequestId, duplicate: false };
  } catch (e) {
    if (reservationApplied) {
      await userRef.transaction(u => {
        if (!u) return u;
        const p = toFiniteNumber(u.pending_withdrawal ?? u.pendingWithdrawal ?? 0, 0);
        u.pending_withdrawal = Math.max(0, p - amount);
        u.wallet_updated_at = now(); u.updated_at = u.wallet_updated_at;
        return u;
      }).catch(() => {});
    }
    console.error('[TX-REQUEST] failed', e);
    return { ok: false, reason: 'transaction-failed' };
  }
}

async function transactionImage(uid, id, imageData) {
  if (!id || !imageData || String(imageData).length > 2_500_000) return false;
  const canonicalUid = await resolveCanonicalUid(uid);
  const txRef = db.ref('transactions/' + id);
  const snap = await txRef.once('value');
  if (!snap.exists()) return false;
  const tx = snap.val();
  if (tx.username !== canonicalUid && tx.wallet_uid !== canonicalUid) return false;
  if (tx.status !== 'pending') return false;
  await txRef.update({ image_data: String(imageData), updated_at: now() });
  return true;
}

async function adminListTransactions() {
  const snapshot = await db.ref('transactions').orderByChild('status').equalTo('pending').once('value');
  const txs = [];
  snapshot.forEach(child => {
    const d = child.val();
    txs.push({
      id: child.key, username: d.username, uid: d.uid || d.wallet_uid || d.username,
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

async function findUserRefForWallet(identity = {}) {
  const raw = typeof identity === 'string' ? identity : '';
  const id = typeof identity === 'object' && identity ? identity : {};
  const walletUid = safeUid(id.wallet_uid || id.uid || raw);
  const telegramId = Number(id.telegram_id || 0);
  const username = safeUser(id.telegram_username || id.username || raw);

  if (walletUid) {
    const direct = await readUserByUid(walletUid);
    if (direct) {
      const redirect = safeUid(direct.data.migrated_to || '');
      if (redirect && redirect !== walletUid) {
        const t = await readUserByUid(redirect);
        if (t) return t;
      }
      if (!telegramId || !direct.data.telegram_id || Number(direct.data.telegram_id) === telegramId) return direct;
    }
  }
  if (telegramId > 0) {
    const byTid = await findUserByTelegramId(telegramId);
    if (byTid) {
      const redirect = safeUid(byTid.data.migrated_to || '');
      if (redirect && redirect !== byTid.uid) { const t = await readUserByUid(redirect); if (t) return t; }
      return byTid;
    }
    const canonical = await readUserByUid(`tg_${telegramId}`);
    if (canonical) return canonical;
  }
  if (username) {
    const byKey = await readUserByUid(username);
    if (byKey) {
      const redirect = safeUid(byKey.data.migrated_to || '');
      if (redirect && redirect !== byKey.uid) { const t = await readUserByUid(redirect); if (t) return t; }
      return byKey;
    }
    const byName = await findUserByStoredUsername(username);
    if (byName) {
      const redirect = safeUid(byName.data.migrated_to || '');
      if (redirect && redirect !== byName.uid) { const t = await readUserByUid(redirect); if (t) return t; }
      return byName;
    }
    const byTgName = await findUserByTelegramUsername(username);
    if (byTgName) {
      const redirect = safeUid(byTgName.data.migrated_to || '');
      if (redirect && redirect !== byTgName.uid) { const t = await readUserByUid(redirect); if (t) return t; }
      return byTgName;
    }
  }
  return null;
}

async function adminDecision(id, decision, reason = '') {
  decision = String(decision || '').toLowerCase();
  if (decision === 'approve') decision = 'approved';
  if (decision === 'reject') decision = 'rejected';
  if (!['approved', 'rejected'].includes(decision)) return { ok: false, reason: 'bad-decision' };

  try {
    const txRef = db.ref('transactions/' + id);
    const txSnap = await txRef.once('value');
    if (!txSnap.exists()) return { ok: false, reason: 'not-found' };
    const tx = { ...(txSnap.val() || {}), id };
    const txAmount = toFiniteNumber(tx.amount, 0);
    if (txAmount <= 0) return { ok: false, reason: 'bad-amount' };
    if (!['deposit', 'withdraw'].includes(tx.type)) return { ok: false, reason: 'bad-type' };
    if (tx.status !== 'pending' && tx.status !== 'processing') return { ok: false, reason: 'already-decided', tx };

    if (tx.type === 'deposit' && decision === 'rejected') {
      const userInfo = await findUserRefForWallet(tx);
      if (!userInfo) return { ok: false, reason: 'wallet-user-not-found' };
      const claim = await txRef.transaction(cur => {
        if (!cur || cur.status !== 'pending') return;
        cur.status = 'rejected'; cur.reason = String(reason || '').slice(0, 256);
        cur.wallet_applied = 0; cur.wallet_uid = userInfo.uid; cur.updated_at = now();
        return cur;
      });
      if (!claim?.committed) return { ok: false, reason: 'already-decided' };
      const utx = { ...tx, status: 'rejected', reason: String(reason || '').slice(0, 256), wallet_applied: 0, wallet_uid: userInfo.uid };
      await sendWallet(userInfo.uid);
      await notifyTransaction(userInfo.uid, utx);
      return { ok: true, tx: utx };
    }

    let userInfo = await findUserRefForWallet(tx);

    // If the wallet can't be resolved but the tx has a telegram_id, create
    // the canonical record on the spot so approval can proceed.
    if (!userInfo && tx.telegram_id != null) {
      const tId = Number(tx.telegram_id);
      if (Number.isSafeInteger(tId) && tId > 0) {
        const canonicalUid = `tg_${tId}`;
        const existing = await readUserByUid(canonicalUid);
        if (existing) {
          userInfo = existing;
        } else {
          const created = await ensureUser(canonicalUid, null, false, {
            telegram_id: tId,
            display_name: tx.display_name || tx.full_name || canonicalUid,
            telegram_username: tx.telegram_username || null
          });
          userInfo = { ref: db.ref('users/' + canonicalUid), uid: canonicalUid, data: created };
        }
      }
    }
    if (!userInfo) return { ok: false, reason: 'wallet-user-not-found' };

    let finalUid = userInfo.uid, finalUser = null, finalLedger = null, lastReason = 'callback-not-run';

    for (let attempt = 0; attempt < 3; attempt++) {
      const userRef = userInfo.ref, userUid = userInfo.uid;
      finalUid = userUid;

      const pre = await userRef.once('value');
      if (!pre.exists()) {
        const retry = await findUserRefForWallet(tx);
        if (!retry) return { ok: false, reason: 'wallet-user-not-found' };
        userInfo = retry; continue;
      }

      let abortReason = 'callback-not-run';
      const result = await userRef.transaction(u => {
        if (!u) { abortReason = 'user-record-missing'; return; }
        const ledger = (u.wallet_ledger && typeof u.wallet_ledger === 'object') ? u.wallet_ledger[id] : null;
        if (ledger) { abortReason = 'already-in-ledger'; return u; }
        const play    = toFiniteNumber(u.play_wallet        ?? u.playWallet        ?? u.play_balance  ?? u.playBalance  ?? 0, 0);
        const main    = toFiniteNumber(u.main_wallet        ?? u.mainWallet        ?? u.main_balance  ?? u.mainBalance  ?? 0, 0);
        const pending = toFiniteNumber(u.pending_withdrawal ?? u.pendingWithdrawal ?? 0, 0);
        if (tx.type === 'deposit') {
          if (decision !== 'approved') { abortReason = 'deposit-not-approved'; return; }
          u.play_wallet = play + txAmount;
        } else if (tx.type === 'withdraw') {
          if (decision === 'approved') {
            if (pending < txAmount) { abortReason = `pending-too-low (pending=${pending} need=${txAmount})`; return; }
            if (main < txAmount)    { abortReason = `main-too-low (main=${main} need=${txAmount})`;       return; }
            u.main_wallet = main - txAmount;
            u.pending_withdrawal = pending - txAmount;
          } else {
            if (pending < txAmount) { abortReason = `pending-too-low (pending=${pending} need=${txAmount})`; return; }
            u.pending_withdrawal = pending - txAmount;
          }
        }
        if (!u.wallet_ledger || typeof u.wallet_ledger !== 'object') u.wallet_ledger = {};
        u.wallet_ledger[id] = { type: tx.type, decision, amount: txAmount, applied_at: now() };
        u.wallet_updated_at = now(); u.updated_at = u.wallet_updated_at;
        abortReason = 'ok';
        return u;
      });

      lastReason = abortReason;

      let fresh = null, ledger = null;
      for (let i = 0; i < 3 && !ledger; i++) {
        const s = await userRef.once('value');
        fresh = s.exists() ? (s.val() || {}) : null;
        ledger = (fresh && fresh.wallet_ledger && typeof fresh.wallet_ledger === 'object') ? fresh.wallet_ledger[id] : null;
        if (!ledger && i < 2) await new Promise(r => setTimeout(r, 120));
      }
      if (ledger) { finalUser = fresh; finalLedger = ledger; break; }

      if (abortReason === 'user-record-missing') {
        const retry = await findUserRefForWallet(tx);
        if (!retry) return { ok: false, reason: 'wallet-user-not-found' };
        if (retry.uid === userUid) break;
        userInfo = retry; continue;
      }
      break;
    }

    if (!finalUser || !finalLedger) {
      console.error(`[ADMIN] ledger missing id=${id} uid=${finalUid} type=${tx.type} decision=${decision} amount=${txAmount} reason="${lastReason}"`);
      return { ok: false, reason: `wallet-write-not-committed (${lastReason})` };
    }
    if (String(finalLedger.decision) !== decision) return { ok: false, reason: 'already-decided' };

    const newPlay = toFiniteNumber(finalUser.play_wallet ?? finalUser.playWallet ?? 0, 0);
    const newMain = toFiniteNumber(finalUser.main_wallet ?? finalUser.mainWallet ?? 0, 0);

    await txRef.update({ status: decision, reason: String(reason || '').slice(0, 256), wallet_applied: 1, wallet_uid: finalUid, uid: finalUid, updated_at: now() });
    const updatedTx = { ...tx, status: decision, reason: String(reason || '').slice(0, 256), wallet_applied: 1, wallet_uid: finalUid, uid: finalUid };

    console.log(`[ADMIN] ${decision} ${tx.type} id=${id} walletUid=${finalUid} amount=${txAmount} → play=${newPlay} main=${newMain}`);
    await sendWallet(finalUid);
    await notifyTransaction(finalUid, updatedTx);
    return { ok: true, tx: updatedTx };
  } catch (e) {
    console.error('[ADMIN] decision failed', e);
    return { ok: false, reason: e.message || 'admin-decision-failed' };
  }
}

async function markNotificationRead(uid, id) { await db.ref('notifications/' + id).update({ read: true }); }

async function claimPayout(uid, amount, roomId, round, kind) {
  const canonical = await resolveCanonicalUid(uid);
  const amt = toFiniteNumber(amount, 0);
  if (amt <= 0) return false;
  const key = `payout:${roomId}_${round}_${canonical}_${kind}`;
  const markerRef = db.ref('notifications/' + key);
  if ((await markerRef.once('value')).exists()) return false;
  try {
    const result = await db.ref('users/' + canonical).transaction(u => {
      if (!u) return;
      u.main_wallet = toFiniteNumber(u.main_wallet ?? u.mainWallet ?? u.main_balance ?? u.mainBalance ?? 0, 0) + amt;
      u.wallet_updated_at = now(); u.updated_at = u.wallet_updated_at;
      return u;
    });
    if (!result?.committed) { console.warn(`[PAYOUT] not committed uid=${canonical} amount=${amt}`); return false; }
    await markerRef.set({ username: canonical, type: 'payout_marker', read: true, timestamp: now() });
    await sendWallet(canonical);
    const newMain = toFiniteNumber(result.snapshot.val()?.main_wallet, 0);
    console.log(`[PAYOUT] uid=${canonical} amount=${amt} → main=${newMain}`);
    return true;
  } catch (e) { console.error(`[PAYOUT] uid=${canonical} ERROR`, e); return false; }
}

async function creditReferral(ref, amount, roomId, round, winnerUid) {
  if (!ref || ref === winnerUid || amount <= 0) return false;
  const canonical = await resolveCanonicalUid(ref);
  const key = `ref:${roomId}_${round}_${winnerUid}_${canonical}`;
  const markerRef = db.ref('notifications/' + key);
  if ((await markerRef.once('value')).exists()) return false;
  await ensureUser(canonical);
  try {
    const result = await db.ref('users/' + canonical).transaction(u => {
      if (!u) return;
      u.main_wallet = toFiniteNumber(u.main_wallet ?? u.mainWallet ?? u.main_balance ?? u.mainBalance ?? 0, 0) + amount;
      u.referral_earnings = toFiniteNumber(u.referral_earnings ?? u.referralEarnings ?? 0, 0) + amount;
      u.wallet_updated_at = now(); u.updated_at = u.wallet_updated_at;
      return u;
    });
    if (!result?.committed) { console.warn(`[REFERRAL] not committed ref=${canonical} amount=${amount}`); return false; }
    await markerRef.set({ username: canonical, type: 'referral_marker', read: true, timestamp: now() });
    await sendWallet(canonical);
    await addNotification(canonical, { message: `🎁 You earned ${amount} Birr commission from ${winnerUid}'s win.` });
    return true;
  } catch (e) { console.error(`[REFERRAL] ref=${canonical} ERROR`, e); return false; }
}

// --- Room / game ---
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
    if (row?.players_json) {
      for (const [uid, p] of Object.entries(JSON.parse(row.players_json))) this.players.set(uid, {
        uid, picks: Array.isArray(p.picks) ? p.picks.map(Number) : [], isBot: !!p.isBot, active: !!p.active, displayName: String(p.displayName || uid)
      });
    }
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

async function debit(uid, amount, roomId, round) {
  const canonical = await resolveCanonicalUid(uid);
  const amt = toFiniteNumber(amount, 0);
  if (amt <= 0) return { ok: false, reason: 'bad-amount' };
  try {
    const result = await db.ref('users/' + canonical).transaction(u => {
      if (!u) return u;
      const bal = toFiniteNumber(u.play_wallet ?? u.playWallet ?? u.play_balance ?? u.playBalance ?? 0, 0);
      if (bal < amt) return u;
      u.play_wallet = bal - amt;
      u.updated_at = now(); u.wallet_updated_at = u.updated_at;
      return u;
    });
    if (result?.committed) {
      const newPlay = toFiniteNumber(result.snapshot.val()?.play_wallet, 0);
      await sendWallet(canonical);
      return { ok: true, balance: newPlay };
    }
    const refSnap = await db.ref('users/' + canonical).once('value');
    if (!refSnap.exists()) return { ok: false, reason: 'user-not-found' };
    const live = toFiniteNumber(refSnap.val()?.play_wallet, 0);
    await sendWallet(canonical);
    return { ok: false, reason: 'insufficient', balance: live };
  } catch (e) { console.error(`[DEBIT] ${canonical}`, e); return { ok: false, reason: 'debit-error' }; }
}

async function refundWager(uid, amount, roomId, round) {
  const canonical = await resolveCanonicalUid(uid);
  const amt = toFiniteNumber(amount, 0);
  if (amt <= 0) return false;
  try {
    const result = await db.ref('users/' + canonical).transaction(u => {
      if (!u) return;
      u.play_wallet = toFiniteNumber(u.play_wallet ?? u.playWallet ?? u.play_balance ?? u.playBalance ?? 0, 0) + amt;
      u.updated_at = now(); u.wallet_updated_at = u.updated_at;
      return u;
    });
    if (result?.committed) { await sendWallet(canonical); return true; }
    return false;
  } catch (e) { console.error(`[REFUND] ${canonical}`, e); return false; }
}

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
        const canonical = await resolveCanonicalUid(uid);
        const uSnap = await db.ref('users/' + canonical).once('value');
        const ref = uSnap.val()?.referred_by;
        if (ref) await creditReferral(ref, Math.floor(share * REFERRAL_RATE), room.id, room.round, uid);
      } catch (e) { console.error(`[WIN] payout failed uid=${uid}`, e); }
    }
  }

  room.lastResult = {
    round: room.round, winningNumber,
    winners: winners.map(uid => room.players.get(uid)?.displayName || uid),
    winAmount: (winners.length && prizePool > 0) ? Math.floor(prizePool / winners.length) : 0,
    setAt: now()
  };
  console.log(`[${room.id}] r${room.round} → spin num=${winningNumber} winners=${room.lastResult.winners.join(',') || '—'} pool=${prizePool}`);
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
  console.log(`[${room.id}] reset (${why}) → r${room.round}`);
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
    } else {
      p.active = true; p.displayName = p.displayName || uid.replace(/^__bot_/, '');
    }
  }
  return roster;
}

function scheduleBots(room) {
  clearBotTimers(room);
  if (room.phase !== 'selection') return;
  const roster = normalizeRoundBots(room);
  const names = [...roster];
  if (!names.length) return;
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
        await ensureUser(uid, null, true);
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

async function maybeMigrateSession(ws) {
  if (!ws.uid) return;
  try {
    const canonical = await resolveCanonicalUid(ws.uid);
    if (canonical && canonical !== ws.uid) {
      const old = conns.get(ws.uid);
      if (old) { old.delete(ws); if (old.size === 0) conns.delete(ws.uid); }
      ws.uid = canonical;
      let n = conns.get(canonical); if (!n) { n = new Set(); conns.set(canonical, n); }
      n.add(ws);
      await sendWallet(canonical);
    }
  } catch {}
}

async function handle(ws, m) {
  if (m.type === 'ping') return send(ws, { type: 'pong', t: now() });

  if (m.type === 'auth') {
    const user = validateInitData(m.initData || '');
    let resolved = null;
    if (user) resolved = await resolveTelegramUser(user, m.ref || null);
    if (!resolved && DEV_ALLOW_ANY && m.devUsername && /^[A-Za-z0-9_]{1,32}$/.test(m.devUsername)) {
      const uid = safeUser(m.devUsername);
      resolved = { uid, user: await ensureUser(uid, m.ref || null, false, { display_name: uid }) };
    }
    if (!resolved?.uid) { send(ws, { type: 'error', message: 'auth-failed', code: 'AUTH_REQUIRED' }); try { ws.close(4001, 'auth required'); } catch {} return; }
    const uid = resolved.uid;
    await ensureUser(uid, m.ref || null, false, {
      telegram_id: user?.id,
      telegram_username: user ? safeUser(user.username || '') : null,
      display_name: user ? ([user.first_name, user.last_name].filter(Boolean).join(' ') || user.username || uid) : uid
    });
    clearTimeout(ws.authTimer);
    ws.uid = uid;
    ws.telegramId = user ? Number(user.id) : null;
    ws.telegramUsername = user ? safeUser(user.username || '') : '';
    ws.displayName = user ? ([user.first_name, user.last_name].filter(Boolean).join(' ') || user.username || uid) : uid;
    ws.authed = true;
    let set = conns.get(uid); if (!set) { set = new Set(); conns.set(uid, set); }
    set.add(ws);
    send(ws, { type: 'authed', uid, displayName: ws.displayName, telegramId: ws.telegramId, telegramUsername: ws.telegramUsername || null });
    send(ws, { type: 'wallet', ...(await walletFor(uid)) });

    const txSnap = await db.ref('transactions').orderByChild('wallet_uid').equalTo(uid).once('value');
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
  if (['pick','unpick','transfer','transaction-request','transaction-image','join','leave'].includes(m.type)) await maybeMigrateSession(ws);

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
    let success = false;
    const canonical = await resolveCanonicalUid(ws.uid);
    await db.ref('users/' + canonical).transaction(u => {
      if (!u) return u;
      const main = toFiniteNumber(u.main_wallet ?? u.mainWallet ?? u.main_balance ?? u.mainBalance ?? 0, 0);
      const pending = toFiniteNumber(u.pending_withdrawal ?? u.pendingWithdrawal ?? 0, 0);
      const play = toFiniteNumber(u.play_wallet ?? u.playWallet ?? u.play_balance ?? u.playBalance ?? 0, 0);
      if (main - pending < amount) return;
      u.main_wallet = main - amount; u.play_wallet = play + amount;
      u.updated_at = now(); u.wallet_updated_at = u.updated_at;
      success = true; return u;
    });
    await sendWallet(canonical);
    if (!success) send(ws, { type: 'error', message: 'transfer-failed' });
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
    if (snap.exists() && snap.val()?.username === ws.uid) await markNotificationRead(ws.uid, id);
    return;
  }
}

setInterval(() => { for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; try { ws.ping(); } catch {} } }, 25000);

// --- Express ---
const app = express();

// CORS so the GitHub Pages admin panel can call us.
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
  if (!ADMIN_KEY || !constantTimeEqual(supplied, ADMIN_KEY)) {
    res.status(403).json({ ok: false, error: 'forbidden' });
    return false;
  }
  return true;
}

app.get('/health', async (_, res) => {
  const roomInfo = {};
  for (const r of Object.values(rooms)) roomInfo[r.id] = { phase: r.phase, round: r.round, players: Object.keys(r.snapshot().players).length, taken: r.taken.size };
  try {
    await Promise.race([db.ref('rooms').limitToFirst(1).once('value'), new Promise((_, rj) => setTimeout(() => rj(new Error('db-timeout')), 2500))]);
    res.json({ ok: true, service: 'winzo-authoritative-server', database: 'firebase-realtime-database', websocket: true, uptime: process.uptime(), origins: ALLOWED_ORIGINS, rooms: roomInfo });
  } catch (e) {
    res.status(503).json({ ok: false, service: 'winzo-authoritative-server', database: 'unavailable', error: e.message || 'db-unavailable' });
  }
});

app.get('/config', (_, res) => res.json({
  ok: true, service: 'winzo-authoritative-server', database: 'firebase-realtime-database',
  walletSchema: 'users/<uid>/{main_wallet,play_wallet,pending_withdrawal,referral_earnings}', websocket: true
}));

app.get('/identity/:telegramId', async (req, res) => {
  if (!checkAdmin(req, res)) return;
  const telegramId = Number(req.params.telegramId);
  if (!Number.isSafeInteger(telegramId) || telegramId <= 0) return res.status(400).json({ ok: false, error: 'bad-telegram-id' });
  try {
    const mapped = await db.ref('telegram_users/' + telegramId).once('value');
    const byTid = await findUserByTelegramId(telegramId);
    const canonical = await readUserByUid(`tg_${telegramId}`);
    res.json({
      ok: true, telegramId,
      mapping: mapped.exists() ? mapped.val() : null,
      byTelegram: byTid ? { uid: byTid.uid, username: byTid.data.username, displayName: byTid.data.display_name, telegramId: byTid.data.telegram_id, main: toFiniteNumber(byTid.data.main_wallet), play: toFiniteNumber(byTid.data.play_wallet), pending: toFiniteNumber(byTid.data.pending_withdrawal) } : null,
      canonical: canonical ? { uid: canonical.uid, username: canonical.data.username, displayName: canonical.data.display_name, telegramId: canonical.data.telegram_id, main: toFiniteNumber(canonical.data.main_wallet), play: toFiniteNumber(canonical.data.play_wallet), pending: toFiniteNumber(canonical.data.pending_withdrawal) } : null
    });
  } catch (e) { res.status(503).json({ ok: false, error: e.message || 'identity-check-failed' }); }
});

// List pending transactions (used by the admin panel).
app.get('/admin/pending', async (req, res) => {
  if (!checkAdmin(req, res)) return;
  try { res.json({ ok: true, transactions: await adminListTransactions() }); }
  catch (e) { res.status(503).json({ ok: false, error: e.message || 'list-failed' }); }
});

// Approve or reject a pending transaction.
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

// Diagnose: dump every pending transaction and how it resolves.
app.get('/admin/diagnose', async (req, res) => {
  if (!checkAdmin(req, res)) return;
  try {
    const txSnap = await db.ref('transactions').orderByChild('status').equalTo('pending').once('value');
    const usersSnap = await db.ref('users').once('value');
    const users = usersSnap.exists() ? usersSnap.val() : {};
    const byTid = {}, byTgName = {}, byName = {};
    for (const [uid, u] of Object.entries(users)) {
      if (u.telegram_id != null) byTid[String(u.telegram_id)] = uid;
      if (u.telegram_username) byTgName[String(u.telegram_username).toLowerCase()] = uid;
      if (u.username) byName[String(u.username).toLowerCase()] = uid;
    }
    const out = [];
    txSnap.forEach(c => {
      const t = c.val() || {};
      const wuid = safeUid(t.wallet_uid || t.uid || t.username || '');
      const txUser = safeUser(t.username || '');
      const tid = t.telegram_id != null ? String(t.telegram_id) : '';
      const tgUser = (t.telegram_username || '').toLowerCase();
      let resolved = null, how = null;
      if (wuid && users[wuid] && !users[wuid].migrated_to) { resolved = wuid; how = 'wallet_uid-direct'; }
      if (!resolved && wuid && users[wuid]?.migrated_to) { const t2 = safeUid(users[wuid].migrated_to); if (users[t2]) { resolved = t2; how = 'wallet_uid-redirect'; } }
      if (!resolved && tid && byTid[tid]) { resolved = byTid[tid]; how = 'telegram_id'; }
      if (!resolved && tgUser && byTgName[tgUser]) { resolved = byTgName[tgUser]; how = 'telegram_username'; }
      if (!resolved && txUser && byName[txUser]) { resolved = byName[txUser]; how = 'username-field'; }
      if (!resolved && wuid && byName[wuid]) { resolved = byName[wuid]; how = 'wallet_uid-as-username'; }
      out.push({
        id: c.key, type: t.type, amount: t.amount, status: t.status,
        tx_wallet_uid: t.wallet_uid || null, tx_uid: t.uid || null,
        tx_username: t.username || null, tx_telegram_id: t.telegram_id ?? null,
        tx_telegram_username: t.telegram_username ?? null, tx_display_name: t.display_name || null,
        walletUid_exists: !!(wuid && users[wuid]),
        walletUid_is_redirect: !!(wuid && users[wuid]?.migrated_to),
        redirect_target: (wuid && users[wuid]?.migrated_to) || null,
        resolved_uid: resolved, resolved_how: how,
        resolved_wallet: resolved && users[resolved] ? {
          play: toFiniteNumber(users[resolved].play_wallet ?? users[resolved].playWallet ?? 0),
          main: toFiniteNumber(users[resolved].main_wallet ?? users[resolved].mainWallet ?? 0),
          pending: toFiniteNumber(users[resolved].pending_withdrawal ?? users[resolved].pendingWithdrawal ?? 0),
          telegram_username: users[resolved].telegram_username || null,
          display_name: users[resolved].display_name || null
        } : null,
        has_ledger_entry: !!(resolved && users[resolved]?.wallet_ledger?.[c.key])
      });
    });
    res.json({ ok: true, pendingCount: out.length, transactions: out });
  } catch (e) { res.status(503).json({ ok: false, error: e.message || 'diagnose-failed' }); }
});

// Repair: rewrite stale telegram_id values on all pending transactions so
// they agree with their own wallet_uid.
app.get('/admin/repair-tx-ids', async (req, res) => {
  if (!checkAdmin(req, res)) return;
  try {
    const snap = await db.ref('transactions').once('value');
    const repaired = [], skipped = [];
    for (const [id, t] of Object.entries(snap.val() || {})) {
      if (t.status !== 'pending') continue;
      const wuid = String(t.wallet_uid || t.uid || '');
      const m = /^tg_(\d+)$/.exec(wuid);
      if (!m) { skipped.push({ id, reason: 'no-tg-prefix', walletUid: wuid }); continue; }
      const correct = Number(m[1]);
      if (t.telegram_id === correct) { skipped.push({ id, reason: 'already-correct' }); continue; }
      await db.ref('transactions/' + id).update({ telegram_id: correct, updated_at: now() });
      repaired.push({ id, from: t.telegram_id, to: correct });
    }
    res.json({ ok: true, repaired, skipped });
  } catch (e) { res.status(503).json({ ok: false, error: e.message || 'repair-failed' }); }
});

// Manual credit: force-credit a user's play wallet and mark the tx approved.
// Use when a deposit got stuck and adminDecision can't recover it.
app.get('/admin/credit', async (req, res) => {
  if (!checkAdmin(req, res)) return;
  const uid = String(req.query.uid || '').replace(/[^A-Za-z0-9_:-]/g, '').slice(0, 64);
  const amount = Number(req.query.amount);
  const txId = String(req.query.txId || '').replace(/[^A-Za-z0-9_-]/g, '').slice(0, 128) || null;
  if (!uid) return res.status(400).json({ ok: false, error: 'bad-uid' });
  if (!Number.isFinite(amount) || amount <= 0) return res.status(400).json({ ok: false, error: 'bad-amount' });
  try {
    if (txId) {
      const u0 = await db.ref('users/' + uid + '/wallet_ledger/' + txId).once('value');
      if (u0.exists()) return res.json({ ok: true, alreadyCredited: true, uid, txId });
    }
    const result = await db.ref('users/' + uid).transaction(u => {
      if (!u) return;
      const play = toFiniteNumber(u.play_wallet ?? u.playWallet ?? u.play_balance ?? u.playBalance ?? 0, 0);
      u.play_wallet = play + amount;
      u.wallet_updated_at = now(); u.updated_at = u.wallet_updated_at;
      if (txId) {
        if (!u.wallet_ledger || typeof u.wallet_ledger !== 'object') u.wallet_ledger = {};
        u.wallet_ledger[txId] = { type: 'deposit', decision: 'approved', amount, applied_at: now(), manual: true };
      }
      return u;
    });
    if (!result?.committed) return res.status(400).json({ ok: false, error: 'user-not-found-or-transaction-aborted', uid });
    if (txId) {
      const txRef = db.ref('transactions/' + txId);
      const txSnap = await txRef.once('value');
      if (txSnap.exists()) await txRef.update({ status: 'approved', wallet_applied: 1, wallet_uid: uid, uid, reason: 'manual recovery', updated_at: now() });
    }
    const after = await db.ref('users/' + uid).once('value');
    const finalPlay = toFiniteNumber(after.val()?.play_wallet ?? 0, 0);
    await sendWallet(uid);
    res.json({ ok: true, uid, amount, txId, newPlay: finalPlay });
  } catch (e) { res.status(500).json({ ok: false, error: e.message || 'manual-credit-failed' }); }
});

app.get('/', (_, res) => res.type('text').send('Winzo authoritative Firebase server is running.'));

const server = http.createServer(app);
server.on('upgrade', (req, socket, head) => {
  const origin = req.headers.origin || '';
  const allowed = !origin || ALLOWED_ORIGINS.includes('*') || ALLOWED_ORIGINS.includes(origin);
  if (!allowed) { socket.write('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n'); socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
});

async function gracefulShutdown(signal) {
  console.log(`[SHUTDOWN] ${signal}`);
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
  console.log(`Origins: ${ALLOWED_ORIGINS.join(', ')}`);
}).catch(err => { console.error('loadRooms failed', err); process.exit(1); });
