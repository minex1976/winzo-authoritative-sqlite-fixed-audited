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
const ALLOWED_ORIGINS = String(process.env.ALLOWED_ORIGINS || process.env.ALLOWED_ORIGIN || 'https://minex1976.github.io,https://web.telegram.org')
  .split(',').map(v => v.trim()).filter(Boolean);

if (!ADMIN_KEY) console.warn('WARNING: ADMIN_KEY is not set. Admin approval will be disabled.');

const SELECTION_SECONDS = 20;
const SPINNING_SECONDS = 5;
const RESULTS_SECONDS = 15;
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

function toFiniteNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

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

function safeUser(username) {
  return String(username || '').replace(/[^A-Za-z0-9_]/g, '').slice(0, 32);
}
function safeUid(value) {
  return String(value || '').replace(/[^A-Za-z0-9_:-]/g, '').slice(0, 64);
}
function now() { return Date.now(); }
function constantTimeEqual(a, b) {
  const aa = Buffer.from(String(a || ''));
  const bb = Buffer.from(String(b || ''));
  return aa.length === bb.length && crypto.timingSafeEqual(aa, bb);
}

async function ensureUser(username, referredBy = null, bot = false, extra = {}) {
  const clean = safeUid(username);
  if (!clean) return null;
  const userRef = db.ref('users/' + clean);
  const snapshot = await userRef.once('value');
  if (snapshot.exists()) {
    const current = snapshot.val() || {};
    const patch = {};
    const main = toFiniteNumber(current.main_wallet ?? current.mainWallet ?? current.main_balance ?? current.mainBalance ?? 0, 0);
    const play = toFiniteNumber(current.play_wallet ?? current.playWallet ?? current.play_balance ?? current.playBalance ?? (bot ? BOT_START_WALLET : 0), 0);
    const pending = toFiniteNumber(current.pending_withdrawal ?? current.pendingWithdrawal ?? 0, 0);
    const refEarn = toFiniteNumber(current.referral_earnings ?? current.referralEarnings ?? 0, 0);
    if (current.main_wallet == null) patch.main_wallet = main;
    if (current.play_wallet == null) patch.play_wallet = play;
    if (current.pending_withdrawal == null) patch.pending_withdrawal = pending;
    if (current.referral_earnings == null) patch.referral_earnings = refEarn;
    if (extra.telegram_id != null && (!current.telegram_id || Number(current.telegram_id) !== Number(extra.telegram_id))) patch.telegram_id = Number(extra.telegram_id);
    if (extra.telegram_username !== undefined && String(current.telegram_username || '') !== String(extra.telegram_username || '')) patch.telegram_username = extra.telegram_username || null;
    if (extra.display_name && String(current.display_name || '') !== String(extra.display_name)) {
      patch.display_name = String(extra.display_name).slice(0, 128);
    }
    if (!current.username) patch.username = clean;
    if (!current.wallet_updated_at) patch.wallet_updated_at = Number(current.updated_at || now());
    if (Object.keys(patch).length) {
      patch.updated_at = now();
      await userRef.update(patch);
    }
    return { ...current, ...patch };
  }
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
    referred_by: referredBy && referredBy !== clean ? safeUser(referredBy) : null,
    created_at: t,
    updated_at: t
  };
  await userRef.set(newUser);
  return newUser;
}

async function readUserByUid(uid) {
  const clean = safeUid(uid);
  if (!clean) return null;
  const ref = db.ref('users/' + clean);
  const snap = await ref.once('value');
  return snap.exists() ? { ref, uid: clean, data: snap.val() || {} } : null;
}

async function findUserByTelegramId(telegramId) {
  const id = Number(telegramId);
  if (!Number.isSafeInteger(id) || id <= 0) return null;
  const snap = await db.ref('users').orderByChild('telegram_id').equalTo(id).once('value');
  let found = null;
  snap.forEach(child => {
    if (!found) found = { ref: child.ref, uid: child.key, data: child.val() || {} };
  });
  return found;
}

async function findUserByStoredUsername(username) {
  const clean = safeUser(username);
  if (!clean) return null;
  const snap = await db.ref('users').orderByChild('username').equalTo(clean).once('value');
  let found = null;
  snap.forEach(child => {
    if (!found) found = { ref: child.ref, uid: child.key, data: child.val() || {} };
  });
  return found;
}

async function resolveTelegramUser(user, referredBy = null) {
  if (!user?.id) return null;
  const telegramId = Number(user.id);
  if (!Number.isSafeInteger(telegramId) || telegramId <= 0) return null;

  const telegramUsername = safeUser(user.username || '');
  const displayName = [user.first_name, user.last_name].filter(Boolean).join(' ').trim()
    || telegramUsername
    || `tg_${telegramId}`;
    
  const canonicalUid = `tg_${telegramId}`;
  const mapRef = db.ref('telegram_users/' + telegramId);

  const mappedSnap = await mapRef.once('value');
  if (mappedSnap.exists() && mappedSnap.val()?.uid) {
    const mappedUid = safeUid(mappedSnap.val().uid);
    const mappedUser = await readUserByUid(mappedUid);
    if (mappedUser && (!mappedUser.data.telegram_id || Number(mappedUser.data.telegram_id) === telegramId)) {
      await ensureUser(mappedUid, referredBy, false, {
        telegram_id: telegramId,
        display_name: displayName,
        telegram_username: telegramUsername || null
      });
      await mapRef.update({ uid: mappedUid, username: mappedUser.data.username || mappedUid, updated_at: now() });
      return { uid: mappedUid, user: (await readUserByUid(mappedUid)).data, telegramId, telegramUsername };
    }
    console.warn(`[IDENTITY] stale/conflicting telegram_users/${telegramId}; repairing mapping`);
  }

  let found = await readUserByUid(canonicalUid);
  if (!found) found = await findUserByTelegramId(telegramId);

  if (found) {
    const existingTelegramId = found.data.telegram_id != null ? Number(found.data.telegram_id) : null;
    if (existingTelegramId && existingTelegramId !== telegramId) {
      console.error(`[IDENTITY] refusing conflicting user uid=${found.uid} telegram_id=${existingTelegramId} login_id=${telegramId}`);
      return null;
    }

    if (found.uid !== canonicalUid) {
        console.log(`[IDENTITY] Migrating legacy user ${found.uid} to ${canonicalUid}`);
        const migratedUser = await ensureUser(canonicalUid, referredBy, false, {
            telegram_id: telegramId,
            display_name: displayName,
            telegram_username: telegramUsername || null,
            main_wallet: found.data.main_wallet,
            play_wallet: found.data.play_wallet,
            pending_withdrawal: found.data.pending_withdrawal,
            referral_earnings: found.data.referral_earnings
        });
        await db.ref('users/' + found.uid).remove();
        found = { uid: canonicalUid, data: migratedUser };
    } else {
        const updated = await ensureUser(found.uid, referredBy, false, {
            telegram_id: telegramId,
            display_name: displayName,
            telegram_username: telegramUsername || null
        });
        found.data = updated;
    }

    await mapRef.set({
      uid: found.uid,
      username: found.data.username || found.uid,
      telegram_username: telegramUsername || null,
      updated_at: now()
    });
    console.log(`[IDENTITY] telegram_id=${telegramId} username=@${telegramUsername || '(none)'} → uid=${found.uid}`);
    return { uid: found.uid, user: found.data, telegramId, telegramUsername };
  }

  const created = await ensureUser(canonicalUid, referredBy, false, {
    telegram_id: telegramId,
    display_name: displayName,
    telegram_username: telegramUsername || null
  });
  if (!created) return null;
  
  await mapRef.set({
    uid: canonicalUid,
    username: created.username || canonicalUid,
    telegram_username: telegramUsername || null,
    updated_at: now()
  });
  console.log(`[IDENTITY] NEW telegram_id=${telegramId} username=@${telegramUsername || '(none)'} → uid=${canonicalUid}`);
  return { uid: canonicalUid, user: created, telegramId, telegramUsername };
}

async function walletFor(username) {
  const clean = safeUid(username);
  if (!clean) return { main: 0, play: 0, pending: 0, refEarn: 0, updatedAt: 0 };
  const snap = await db.ref('users/' + clean).once('value');
  if (!snap.exists()) {
    console.warn(`[WALLET] user-not-found uid=${clean}`);
    return { main: 0, play: 0, pending: 0, refEarn: 0, updatedAt: 0 };
  }
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
  const type = data.txType;
  const amount = Number(data.amount);
  if (!['deposit', 'withdraw'].includes(type)) return { ok: false, reason: 'bad-type' };
  if (!Number.isFinite(amount) || amount <= 0 || amount > 1_000_000) return { ok: false, reason: 'bad-amount' };

  const clientRequestId = String(data.clientRequestId || '').slice(0, 128);
  const requestKey = clientRequestId ? safeUid(uid) + ':' + safeUid(clientRequestId) : '';
  if (requestKey) {
    const prior = await db.ref('transaction_requests/' + safeUid(uid) + '/' + safeUid(clientRequestId)).once('value');
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

  const userRef = db.ref('users/' + uid);
  const userSnap = await userRef.once('value');
  if (!userSnap.exists()) return { ok: false, reason: 'user-not-found' };

  const txRef = db.ref('transactions').push();
  const id = txRef.key;
  let reservationApplied = false;

  try {
    if (type === 'withdraw') {
      const result = await userRef.transaction(u => {
        if (!u) return u;
        const main = toFiniteNumber(u.main_wallet ?? u.mainWallet ?? u.main_balance ?? u.mainBalance ?? 0, 0);
        const pending = toFiniteNumber(u.pending_withdrawal ?? u.pendingWithdrawal ?? 0, 0);
        const available = main - pending;
        if (available < amount) return u;
        u.pending_withdrawal = pending + amount;
        u.wallet_updated_at = now();
        u.updated_at = u.wallet_updated_at;
        reservationApplied = true;
        return u;
      });
      if (!result?.committed || !reservationApplied) return { ok: false, reason: 'insufficient-main' };
    }

    const userRecord = (await userRef.once('value')).val() || {};
    const displayName = String(userRecord.display_name || userRecord.username || uid).slice(0, 128);
    await txRef.set({
      username: uid,
      uid,
      wallet_uid: uid,
      telegram_id: userRecord.telegram_id != null ? Number(userRecord.telegram_id) : null,
      telegram_username: userRecord.telegram_username || null,
      display_name: displayName,
      type,
      amount,
      status: 'pending',
      timestamp: now(),
      reference,
      full_name: fullName,
      phone_number: phone,
      image_data: imageData,
      reason: '',
      wallet_applied: 0,
      client_request_id: clientRequestId || null,
      updated_at: now()
    });

    if (requestKey) {
      await db.ref('transaction_requests/' + safeUid(uid) + '/' + safeUid(clientRequestId)).set({
        tx_id: id,
        type,
        amount,
        created_at: now()
      });
    }

    await sendWallet(uid);
    return { ok: true, id, clientRequestId, duplicate: false };
  } catch (e) {
    if (reservationApplied) {
      await userRef.transaction(u => {
        if (!u) return u;
        const pending = toFiniteNumber(u.pending_withdrawal ?? u.pendingWithdrawal ?? 0, 0);
        u.pending_withdrawal = Math.max(0, pending - amount);
        u.wallet_updated_at = now();
        u.updated_at = u.wallet_updated_at;
        return u;
      }).catch(() => {});
    }
    console.error('[TX-REQUEST] failed', e);
    return { ok: false, reason: 'transaction-failed' };
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
  const tx = typeof identity === 'object' && identity ? identity : {};
  const walletUid = safeUid(tx.wallet_uid || tx.uid || raw);
  const telegramId = Number(tx.telegram_id || 0);
  const username = safeUser(tx.telegram_username || tx.username || raw);

  if (walletUid) {
    const direct = await readUserByUid(walletUid);
    if (direct) {
      if (!telegramId || !direct.data.telegram_id || Number(direct.data.telegram_id) === telegramId) return direct;
    }
  }
  if (telegramId > 0) {
    const byTelegram = await findUserByTelegramId(telegramId);
    if (byTelegram) return byTelegram;
    const canonical = await readUserByUid(`tg_${telegramId}`);
    if (canonical) return canonical;
  }
  if (username) {
    const byKey = await readUserByUid(username);
    if (byKey) return byKey;
    const byUsername = await findUserByStoredUsername(username);
    if (byUsername) return byUsername;
  }
  return null;
}

/**
 * Admin decision — ROBUST version.
 *
 * Strategy:
 *  1. Read the tx.
 *  2. If already finalized → return 'already-decided'.
 *  3. Deposit rejection = only update tx status (no wallet change).
 *  4. For everything else:
 *     a. Resolve the correct wallet UID.
 *     b. Read fresh balance snapshot.
 *     c. If ledger already has this tx ID → treat as success (idempotent).
 *     d. Pre-check balance preconditions and return SPECIFIC error reasons.
 *     e. Apply the change via an atomic transaction.
 *     f. Verify the ledger exists after write; if not, return a precise reason.
 *     g. Update the tx record to the final status.
 */
async function adminDecision(id, decision, reason = '') {
  decision = String(decision || '').trim().toLowerCase();
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

    // Already finalized? Never touch the wallet again.
    if (tx.status !== 'pending' && tx.status !== 'processing') {
      return { ok: false, reason: 'already-decided', tx };
    }

    // ---- Deposit rejection: no wallet change ----
    if (tx.type === 'deposit' && decision === 'rejected') {
      const userInfo = await findUserRefForWallet({
        uid: tx.uid, wallet_uid: tx.wallet_uid, telegram_id: tx.telegram_id,
        telegram_username: tx.telegram_username, username: tx.username
      });
      if (!userInfo) return { ok: false, reason: 'wallet-user-not-found' };

      const claim = await txRef.transaction(current => {
        if (!current || current.status !== 'pending') return current;
        current.status = 'rejected';
        current.reason = String(reason || '').slice(0, 256);
        current.wallet_applied = 0;
        current.wallet_uid = userInfo.uid;
        current.updated_at = now();
        return current;
      });
      if (!claim?.committed) {
        // Either already decided or race. Treat as success if the status matches.
        const fresh = await txRef.once('value');
        const f = fresh.val() || {};
        if (f.status === 'rejected') {
          const updatedTx = { ...tx, status: 'rejected', wallet_applied: 0, wallet_uid: userInfo.uid };
          return { ok: true, tx: updatedTx };
        }
        return { ok: false, reason: 'already-decided' };
      }
      const updatedTx = { ...tx, status: 'rejected', reason: String(reason || '').slice(0, 256), wallet_applied: 0, wallet_uid: userInfo.uid };
      await sendWallet(userInfo.uid);
      await notifyTransaction(userInfo.uid, updatedTx);
      return { ok: true, tx: updatedTx };
    }

    // ---- All other cases: resolve user & apply wallet change ----
    const userInfo = await findUserRefForWallet({
      uid: tx.uid, wallet_uid: tx.wallet_uid, telegram_id: tx.telegram_id,
      telegram_username: tx.telegram_username, username: tx.username
    });
    if (!userInfo) return { ok: false, reason: 'wallet-user-not-found' };
    const userRef = userInfo.ref;
    const userUid = userInfo.uid;

    // Read current state to check preconditions & idempotency
    const freshSnap = await userRef.once('value');
    if (!freshSnap.exists()) return { ok: false, reason: 'wallet-user-not-found' };
    const fresh = freshSnap.val() || {};

    // Idempotency: if the ledger already has this tx, treat as success.
    const existingLedger = fresh.wallet_ledger && fresh.wallet_ledger[id];
    if (existingLedger) {
      if (String(existingLedger.decision) !== decision) {
        return { ok: false, reason: 'conflicting-decision (already ' + existingLedger.decision + ')' };
      }
      await txRef.update({
        status: decision,
        reason: String(reason || '').slice(0, 256),
        wallet_applied: 1,
        wallet_uid: userUid,
        updated_at: now()
      });
      const updatedTx = { ...tx, status: decision, wallet_applied: 1, wallet_uid: userUid, reason: String(reason || '').slice(0, 256) };
      return { ok: true, tx: updatedTx };
    }

    const main = toFiniteNumber(fresh.main_wallet ?? fresh.mainWallet ?? 0, 0);
    const pending = toFiniteNumber(fresh.pending_withdrawal ?? fresh.pendingWithdrawal ?? 0, 0);
    const play = toFiniteNumber(fresh.play_wallet ?? fresh.playWallet ?? 0, 0);

    // Precondition checks with SPECIFIC error messages
    if (tx.type === 'withdraw' && decision === 'approved') {
      if (main < txAmount) return { ok: false, reason: 'insufficient-main (have ' + main + ', need ' + txAmount + ')' };
      if (pending < txAmount) return { ok: false, reason: 'insufficient-pending (have ' + pending + ', need ' + txAmount + ')' };
    }
    if (tx.type === 'withdraw' && decision === 'rejected') {
      if (pending < txAmount) return { ok: false, reason: 'insufficient-pending (have ' + pending + ', need ' + txAmount + ')' };
    }
    // deposit approved → always allowed

    // Apply atomically
    let attemptedChange = false;
    const result = await userRef.transaction(u => {
      if (!u) return u;
      // Idempotency inside the transaction
      const ledger = u.wallet_ledger && u.wallet_ledger[id];
      if (ledger) return u;

      const main2 = toFiniteNumber(u.main_wallet ?? u.mainWallet ?? u.main_balance ?? u.mainBalance ?? 0, 0);
      const pending2 = toFiniteNumber(u.pending_withdrawal ?? u.pendingWithdrawal ?? 0, 0);
      const play2 = toFiniteNumber(u.play_wallet ?? u.playWallet ?? u.play_balance ?? u.playBalance ?? 0, 0);

      if (tx.type === 'deposit') {
        if (decision !== 'approved') return u;
        u.play_wallet = play2 + txAmount;
      } else if (tx.type === 'withdraw') {
        if (decision === 'approved') {
          if (main2 < txAmount || pending2 < txAmount) return u;
          u.main_wallet = main2 - txAmount;
          u.pending_withdrawal = pending2 - txAmount;
        } else {
          if (pending2 < txAmount) return u;
          u.pending_withdrawal = pending2 - txAmount;
        }
      } else {
        return u;
      }

      if (!u.wallet_ledger || typeof u.wallet_ledger !== 'object') u.wallet_ledger = {};
      u.wallet_ledger[id] = {
        type: tx.type,
        decision,
        amount: txAmount,
        applied_at: now()
      };
      u.wallet_updated_at = now();
      u.updated_at = u.wallet_updated_at;
      attemptedChange = true;
      return u;
    });

    // Recover the freshest state regardless of committed flag
    let updatedUser = null;
    if (result && result.snapshot && result.snapshot.exists()) {
      updatedUser = result.snapshot.val() || {};
    } else {
      const recovery = await userRef.once('value');
      updatedUser = recovery.exists() ? (recovery.val() || {}) : null;
    }
    if (!updatedUser) return { ok: false, reason: 'user-not-found-after-write' };

    const ledgerEntry = updatedUser.wallet_ledger && updatedUser.wallet_ledger[id];
    if (!ledgerEntry) {
      // Transaction did NOT apply. Figure out why so we can return a precise reason.
      const u = updatedUser;
      const main3 = toFiniteNumber(u.main_wallet ?? u.mainWallet ?? 0, 0);
      const pending3 = toFiniteNumber(u.pending_withdrawal ?? u.pendingWithdrawal ?? 0, 0);
      if (tx.type === 'withdraw' && decision === 'approved') {
        if (main3 < txAmount) return { ok: false, reason: 'insufficient-main (have ' + main3 + ', need ' + txAmount + ')' };
        if (pending3 < txAmount) return { ok: false, reason: 'insufficient-pending (have ' + pending3 + ', need ' + txAmount + ')' };
      }
      if (tx.type === 'withdraw' && decision === 'rejected') {
        if (pending3 < txAmount) return { ok: false, reason: 'insufficient-pending (have ' + pending3 + ', need ' + txAmount + ')' };
      }
      return { ok: false, reason: 'wallet-write-not-committed' };
    }
    if (String(ledgerEntry.decision) !== decision) {
      return { ok: false, reason: 'conflicting-decision (already ' + ledgerEntry.decision + ')' };
    }

    const newPlay = toFiniteNumber(updatedUser.play_wallet ?? updatedUser.playWallet ?? 0, 0);
    const newMain = toFiniteNumber(updatedUser.main_wallet ?? updatedUser.mainWallet ?? 0, 0);
    const newPending = toFiniteNumber(updatedUser.pending_withdrawal ?? updatedUser.pendingWithdrawal ?? 0, 0);

    await txRef.update({
      status: decision,
      reason: String(reason || '').slice(0, 256),
      wallet_applied: 1,
      wallet_uid: userUid,
      updated_at: now()
    });

    const updatedTx = {
      ...tx,
      status: decision,
      reason: String(reason || '').slice(0, 256),
      wallet_applied: 1,
      wallet_uid: userUid
    };
    console.log(`[ADMIN] ${decision} ${tx.type} id=${id} user=${tx.username} walletUid=${userUid} amount=${txAmount} → play=${newPlay} main=${newMain} pending=${newPending}`);
    await sendWallet(userUid);
    await notifyTransaction(userUid, updatedTx);
    return { ok: true, tx: updatedTx };
  } catch (e) {
    console.error('[ADMIN] decision failed', e);
    return { ok: false, reason: e.message || 'admin-decision-failed' };
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
    u.main_wallet = toFiniteNumber(u.main_wallet ?? u.mainWallet ?? u.main_balance ?? u.mainBalance ?? 0, 0) + amount;
    u.wallet_updated_at = now();
    u.updated_at = u.wallet_updated_at;
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
    u.main_wallet = toFiniteNumber(u.main_wallet ?? u.mainWallet ?? u.main_balance ?? u.mainBalance ?? 0, 0) + amount;
    u.referral_earnings = toFiniteNumber(u.referral_earnings, 0) + amount;
    u.wallet_updated_at = now();
    u.updated_at = u.wallet_updated_at;
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
    const activePlayers = [...this.players.values()].filter(p => p.active);
    const totalPicks = activePlayers.reduce((sum, p) => sum + p.picks.length, 0);
    const totalBets = totalPicks * this.bet;
    const humanBets = activePlayers
      .filter(p => !p.isBot)
      .reduce((sum, p) => sum + p.picks.length * this.bet, 0);
    return {
      gameState: this.phase, round: this.round, betAmount: this.bet,
      selectionEndsAt: this.endsAt, endsAt: this.endsAt,
      numbersTaken: Object.fromEntries([...this.taken.keys()].map(n => [n, true])),
      players,
      winners: [...this.winners].map(uid => this.players.get(uid)?.displayName || uid),
      winningNumber: this.winningNumber,
      prizePool: this.prizePool, potAmount: this.prizePool,
      totalBets,
      humanBets,
      totalPicks,
      displayedPrizePool: Math.floor(totalBets * PAYOUT_RATE),
      onlineCount: activePlayers.length,
      botCount: activePlayers.filter(p => p.isBot).length,
      realPlayerCount: activePlayers.filter(p => !p.isBot).length,
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
  const amt = toFiniteNumber(amount, 0);
  if (amt <= 0) return { ok: false, reason: 'bad-amount' };
  try {
    const result = await db.ref('users/' + uid).transaction(u => {
      if (!u) return u;
      const balance = toFiniteNumber(u.play_wallet ?? u.playWallet ?? u.play_balance ?? u.playBalance ?? 0, 0);
      if (balance < amt) return u;
      u.play_wallet = balance - amt;
      u.updated_at = now();
      u.wallet_updated_at = u.updated_at;
      return u;
    });
    const committed = !!(result && result.committed);
    if (committed) {
      const newPlay = toFiniteNumber(result.snapshot.val()?.play_wallet, 0);
      console.log(`[DEBIT] uid=${uid} amount=${amt} committed=true newPlay=${newPlay} room=${roomId} round=${round}`);
      await sendWallet(uid);
      return { ok: true, balance: newPlay };
    }
    const refSnap = await db.ref('users/' + uid).once('value');
    if (!refSnap.exists()) {
      console.warn(`[DEBIT] uid=${uid} → user-not-found`);
      return { ok: false, reason: 'user-not-found' };
    }
    const live = toFiniteNumber(refSnap.val()?.play_wallet, 0);
    console.log(`[DEBIT] uid=${uid} amount=${amt} committed=false livePlay=${live} → insufficient`);
    await sendWallet(uid);
    return { ok: false, reason: 'insufficient', balance: live };
  } catch (e) {
    console.error(`[DEBIT] uid=${uid} amount=${amt} ERROR`, e);
    return { ok: false, reason: 'debit-error' };
  }
}

async function refundWager(uid, amount, roomId, round) {
  const amt = toFiniteNumber(amount, 0);
  if (amt <= 0) return false;
  try {
    let committed = false;
    const result = await db.ref('users/' + uid).transaction(u => {
      if (!u) return u;
      u.play_wallet = toFiniteNumber(u.play_wallet ?? u.playWallet ?? u.play_balance ?? u.playBalance ?? 0, 0) + amt;
      u.updated_at = now();
      u.wallet_updated_at = u.updated_at;
      committed = true;
      return u;
    });
    const ok = committed && !!(result && result.committed);
    if (ok) {
      const newPlay = toFiniteNumber(result.snapshot.val()?.play_wallet, 0);
      console.log(`[REFUND] uid=${uid} amount=${amt} newPlay=${newPlay} room=${roomId} round=${round}`);
      await sendWallet(uid);
    }
    return ok;
  } catch (e) {
    console.error(`[REFUND] uid=${uid} amount=${amt} ERROR`, e);
    return false;
  }
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

    const debitResult = await debit(uid, room.bet, room.id, room.round);
    if (!debitResult.ok) return { ok: false, reason: debitResult.reason || 'insufficient' };

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
  const amount = room.bet * p.picks.length;
  await refundWager(p.uid, amount, room.id, room.round);
  for (const n of p.picks) if (room.taken.get(n) === p.uid) room.taken.delete(n);
  p.picks = []; p.active = false; room.dirty = true;
}

async function toSpinning(room) {
  if (room.phase !== 'selection') return;
  const allWithPicks = [...room.players.values()]
    .filter(p => p.picks.length > 0 && (p.isBot || p.active));
  const humans = allWithPicks.filter(p => !p.isBot);
  const bots = allWithPicks.filter(p => p.isBot);
  if (!allWithPicks.length) return resetRound(room, 'no-picks');

  let winningNumber = null;
  let winners = [];
  let prizePool = 0;

  const allPlayers = [...humans, ...bots];
  const allNums = [...new Set(allPlayers.flatMap(p => p.picks))];
  if (!allNums.length) return resetRound(room, 'no-picks');
  
  winningNumber = allNums[Math.floor(Math.random() * allNums.length)];
  winners = allPlayers.filter(p => p.picks.includes(winningNumber)).map(p => p.uid);

  const totalPicksInRoom = allPlayers.reduce((s, p) => s + p.picks.length, 0);
  prizePool = Math.floor(totalPicksInRoom * room.bet * PAYOUT_RATE);

  room.winningNumber = winningNumber;
  room.winners = winners;
  room.prizePool = prizePool;
  room.phase = 'spinning';
  room.endsAt = now() + SPINNING_SECONDS * 1000;
  room.dirty = true;

  if (winners.length && prizePool > 0) {
    const share = Math.floor(prizePool / winners.length);
    for (const uid of winners) {
      if (share <= 0) break;
      if (!(await claimPayout(uid, share, room.id, room.round, 'win'))) continue;
      if (!room.players.get(uid)?.isBot) {
          await addNotification(uid, { message: `🏆 You won ${share} Birr in round ${room.round}!`, type: 'notification' });
          const uSnap = await db.ref('users/' + uid).once('value');
          const ref = uSnap.val()?.referred_by;
          if (ref) await creditReferral(ref, Math.floor(share * REFERRAL_RATE), room.id, room.round, uid);
      }
    }
  }

  room.lastResult = {
    round: room.round,
    winningNumber,
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
  console.log(`[${room.id}] reset (${why}) → round ${room.round}`);
  scheduleBots(room);
}

function botRoster(count, round) {
  const pool = [...BOT_NAMES]; const off = ((round - 1) * 7) % pool.length;
  const rot = pool.slice(off).concat(pool.slice(0, off));
  return rot.slice(0, Math.min(count, pool.length));
}
function botUid(displayName) { return `__bot_${safeUser(displayName)}`; }
function clearBotTimers(room) { room.botTimers.forEach(clearTimeout); room.botTimers = []; }
function currentBotRoster(room) {
  const count = BOT_MIN + (room.round % (BOT_MAX - BOT_MIN + 1));
  return botRoster(count, room.round);
}

function normalizeRoundBots(room) {
  const roster = new Set(currentBotRoster(room));
  const activeBotUids = new Set([...roster].map(botUid));
  for (const [uid, p] of room.players) {
    if (!p.isBot) continue;
    if (!activeBotUids.has(uid)) {
      for (const n of p.picks || []) if (room.taken.get(n) === uid) room.taken.delete(n);
      p.picks = [];
      p.active = false;
    } else {
      p.active = true;
      p.displayName = p.displayName || uid.replace(/^__bot_/, '');
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
  for (let pickNo = 0; pickNo < MAX_PICKS; pickNo++) {
    const pass = [...names];
    for (let i = pass.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [pass[i], pass[j]] = [pass[j], pass[i]];
    }
    for (const name of pass) order.push(name);
  }

  const START_DELAY_MS = 350;
  const BOT_PICK_GAP_MS = 300;
  let index = 0;

  const runNextPick = async () => {
    if (room.phase !== 'selection' || index >= order.length) return;
    if (now() >= room.endsAt - 1000) return;
    const displayName = order[index++];
    const uid = botUid(displayName);
    try {
      await room.mutex.run(async () => {
        if (room.phase !== 'selection') return;
        await ensureUser(uid, null, true);
        let p = room.players.get(uid);
        if (!p) {
          p = { uid, picks: [], isBot: true, active: true, displayName };
          room.players.set(uid, p);
        } else {
          p.active = true;
          p.isBot = true;
          p.displayName = displayName;
        }
        if (p.picks.length >= MAX_PICKS) return;
        let num = null;
        for (let attempt = 0; attempt < 12; attempt++) {
          const candidate = 1 + Math.floor(Math.random() * 200);
          if (!room.taken.has(candidate) && !p.picks.includes(candidate)) {
            num = candidate;
            break;
          }
        }
        if (num == null) {
          for (let candidate = 1; candidate <= 200; candidate++) {
            if (!room.taken.has(candidate)) { num = candidate; break; }
          }
        }
        if (num == null) return;
        room.taken.set(num, uid);
        p.picks.push(num);
        room.dirty = true;
        room.broadcast();
      });
    } catch (err) {
      console.error('[BOT-PICK]', err);
    }
    if (room.phase !== 'selection' || index >= order.length) return;
    const t = setTimeout(runNextPick, BOT_PICK_GAP_MS);
    room.botTimers.push(t);
  };

  const first = setTimeout(runNextPick, START_DELAY_MS);
  room.botTimers.push(first);
}

setInterval(async () => {
  for (const room of Object.values(rooms)) {
    await room.mutex.run(async () => {
      const t = now();
      if (room.phase === 'selection' && t >= room.endsAt) await toSpinning(room);
      else if (room.phase === 'spinning' && t >= room.endsAt) await toResults(room);
      else if (room.phase === 'results' && t >= room.endsAt) await resetRound(room, 'results-done');
      if (room.dirty) {
        await room.persist();
        room.broadcast();
        room.dirty = false;
      }
    });
  }
}, 1000);

const conns = new Map();
const wss = new WebSocketServer({ noServer: true });

function send(ws, obj) { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); }
function sendRoomState(room) { if (room) room.broadcast(); }

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.authenticatedAt = null;
  ws.authTimer = setTimeout(() => { if (!ws.authed) { try { ws.close(4001, 'authentication timeout'); } catch {} } }, 10000);
  ws.uid = null; ws.roomId = null; ws.authed = false; ws.isAdmin = false;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', async raw => {
    let m; try { m = JSON.parse(raw.toString()); } catch { return; }
    try { await handle(ws, m); } catch (e) { console.error('[WS-MSG]', e); send(ws, { type: 'error', message: 'server-error' }); }
  });
  ws.on('close', async () => {
    clearTimeout(ws.authTimer);
    if (!ws.uid) return;
    const set = conns.get(ws.uid);
    if (set) {
      set.delete(ws);
      if (set.size) return;
      conns.delete(ws.uid);
    }
    if (ws.roomId) {
      const room = rooms[ws.roomId];
      if (!room) return;
      await room.mutex.run(async () => {
        const p = room.players.get(ws.uid);
        if (!p || p.isBot) return;
        if (room.phase === 'selection') {
          await refundDisconnectedPlayer(room, p);
        }
        p.active = false;
        room.dirty = true;
        await room.persist();
        room.broadcast();
      }).catch(err => console.error('[WS-CLOSE] disconnect refund failed', err));
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
      resolved = { uid, user: await ensureUser(uid, m.ref || null, false, { display_name: uid }) };
    }
    if (!resolved?.uid) {
      send(ws, { type: 'error', message: 'auth-failed', code: 'AUTH_REQUIRED' });
      try { ws.close(4001, 'authentication required'); } catch {}
      return;
    }
    const uid = resolved.uid;
    await ensureUser(uid, m.ref || null, false, { telegram_id: user?.id, telegram_username: user ? safeUser(user.username || '') : null, display_name: user ? ([user.first_name, user.last_name].filter(Boolean).join(' ') || user.username || uid) : uid });
    clearTimeout(ws.authTimer); ws.authenticatedAt = now();
    ws.uid = uid;
    ws.telegramId = user ? Number(user.id) : null;
    ws.telegramUsername = user ? safeUser(user.username || '') : '';
    ws.displayName = user ? ([user.first_name, user.last_name].filter(Boolean).join(' ') || user.username || uid) : uid;
    ws.authed = true;
    let set = conns.get(uid); if (!set) { set = new Set(); conns.set(uid, set); } set.add(ws);
    send(ws, { type: 'authed', uid, displayName: ws.displayName, telegramId: ws.telegramId, telegramUsername: ws.telegramUsername || null });
    send(ws, { type: 'wallet', ...(await walletFor(uid)) });
    console.log(`[AUTH] uid=${uid} → wallet sent`);

    const txSnap = await db.ref('transactions').orderByChild('wallet_uid').equalTo(uid).once('value');
    const pending = [];
    txSnap.forEach(child => {
      const d = child.val();
      if (d.status === 'pending') pending.push({ id: child.key, type: d.type, amount: d.amount, status: d.status, timestamp: Number(d.timestamp || 0) });
    });
    pending.sort((a, b) => b.timestamp - a.timestamp);
    send(ws, { type: 'pending-transactions', transactions: pending.slice(0, 10) });
    return;
  }

  if (m.type === 'admin-auth') {
    if (!ADMIN_KEY || typeof m.key !== 'string' || !constantTimeEqual(m.key, ADMIN_KEY)) {
      send(ws, { type: 'admin-auth-fail' });
      try { ws.close(4003, 'admin authentication failed'); } catch {}
      return;
    }
    ws.authed = true; ws.isAdmin = true; send(ws, { type: 'admin-authed' }); return;
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
    if (ws.roomId && rooms[ws.roomId] && ws.roomId !== m.room) { const oldPlayer = rooms[ws.roomId].players.get(ws.uid); if (oldPlayer) oldPlayer.active = false; }
    ws.roomId = m.room;
    const existing = room.players.get(ws.uid);
    if (existing && existing.picks.length && !existing.active) { await refundDisconnectedPlayer(room, existing); }
    if (!room.players.has(ws.uid)) room.players.set(ws.uid, { uid: ws.uid, picks: [], isBot: false, active: true, displayName: ws.displayName || ws.uid });
    else { const p = room.players.get(ws.uid); p.active = true; p.displayName = ws.displayName || p.displayName || ws.uid; }
    room.dirty = true; await saveRoom(room);
    await sendWallet(ws.uid);
    return;
  }
  if (m.type === 'leave') {
    const room = rooms[ws.roomId]; const p = room?.players.get(ws.uid);
    if (room && p && !p.isBot) { await refundDisconnectedPlayer(room, p); room.players.delete(ws.uid); room.dirty = true; await saveRoom(room); }
    ws.roomId = null; return;
  }
  if (m.type === 'pick') {
    const room = rooms[ws.roomId]; if (!room) return;
    const r = await pickIntent(room, ws.uid, Number(m.number));
    send(ws, r.ok ? { type: 'pick-ok', number: Number(m.number) } : { type: 'pick-fail', number: Number(m.number), reason: r.reason });
    if (r.ok && room.dirty) {
      await room.persist();
      room.broadcast();
      room.dirty = false;
    }
    send(ws, { type: 'wallet', ...(await walletFor(ws.uid)) });
    return;
  }
  if (m.type === 'unpick') {
    const room = rooms[ws.roomId]; if (!room) return;
    const r = await unpickIntent(room, ws.uid, Number(m.number));
    send(ws, r.ok ? { type: 'unpick-ok', number: Number(m.number) } : { type: 'unpick-fail', number: Number(m.number), reason: r.reason });
    if (r.ok && room.dirty) {
      await room.persist();
      room.broadcast();
      room.dirty = false;
    }
    send(ws, { type: 'wallet', ...(await walletFor(ws.uid)) });
    return;
  }
  if (m.type === 'transfer') {
    const amount = Number(m.amount);
    if (!Number.isFinite(amount) || amount <= 0 || amount > 1000000) return send(ws, { type: 'error', message: 'bad-amount' });
    let success = false;
    await db.ref('users/' + ws.uid).transaction(u => {
      if (!u) return u;
      const main = toFiniteNumber(u.main_wallet ?? u.mainWallet ?? u.main_balance ?? u.mainBalance ?? 0, 0);
      const pending = toFiniteNumber(u.pending_withdrawal ?? u.pendingWithdrawal ?? 0, 0);
      const play = toFiniteNumber(u.play_wallet ?? u.playWallet ?? u.play_balance ?? u.playBalance ?? 0, 0);
      if (main - pending < amount) return u;
      u.main_wallet = main - amount;
      u.play_wallet = play + amount;
      u.updated_at = now();
      success = true;
      return u;
    });
    await sendWallet(ws.uid);
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
  if (m.type === 'transaction-image') {
    await transactionImage(ws.uid, String(m.id || ''), String(m.imageData || '')); return;
  }
  if (m.type === 'notification-read') {
    const id = String(m.id || '');
    const snap = await db.ref('notifications/' + id).once('value');
    if (snap.exists() && snap.val()?.username === ws.uid) await markNotificationRead(ws.uid, id);
    return;
  }
}

setInterval(() => { for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; try { ws.ping(); } catch {} } }, 25000);

const app = express();

app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, X-Admin-Key');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

function isAdmin(req) {
  const suppliedKey = String(req.get('x-admin-key') || '');
  return ADMIN_KEY && constantTimeEqual(suppliedKey, ADMIN_KEY);
}

app.get('/admin/pending', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  try {
    const txs = await adminListTransactions();
    res.json({ ok: true, transactions: txs });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/admin/decision', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  const id = String(req.query.id || '');
  const decision = String(req.query.decision || '');
  if (!id || !decision) return res.status(400).json({ ok: false, error: 'missing-params' });
  try {
    const result = await adminDecision(id, decision, 'Processed via HTTP admin panel');
    if (!result.ok) return res.status(400).json({ ok: false, error: result.reason });
    res.json({ ok: true, transaction: result.tx });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/admin/repair-tx-ids', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  try {
    const snap = await db.ref('transactions').orderByChild('status').equalTo('pending').once('value');
    const repaired = [];
    const skipped = [];
    const promises = [];
    snap.forEach(child => {
      const tx = child.val();
      const id = child.key;
      const telegramId = Number(tx.telegram_id || 0);
      const username = safeUser(tx.telegram_username || tx.username || '');
      if (telegramId > 0 || username) {
        promises.push((async () => {
          const userInfo = await findUserRefForWallet({
            uid: tx.uid, wallet_uid: tx.wallet_uid, telegram_id: telegramId,
            telegram_username: username, username: tx.username
          });
          if (userInfo && userInfo.uid !== tx.wallet_uid) {
            await db.ref('transactions/' + id).update({ wallet_uid: userInfo.uid, updated_at: now() });
            repaired.push({ id, old: tx.wallet_uid, new: userInfo.uid });
          } else {
            skipped.push(id);
          }
        })());
      } else {
        skipped.push(id);
      }
    });
    await Promise.all(promises);
    res.json({ ok: true, repaired, skipped });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/admin/diagnose', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  try {
    const txs = await adminListTransactions();
    const diagnostics = txs.map(tx => ({ ...tx, resolved_uid: tx.walletUid }));
    res.json({ ok: true, transactions: diagnostics });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/health', async (_, res) => {
  const roomInfo = {};
  for (const room of Object.values(rooms)) {
    roomInfo[room.id] = { phase: room.phase, round: room.round, players: Object.keys(room.snapshot().players).length, taken: room.taken.size };
  }
  try {
    await Promise.race([
      db.ref('rooms').limitToFirst(1).once('value'),
      new Promise((_, reject) => setTimeout(() => reject(new Error('database-timeout')), 2500))
    ]);
    res.json({ ok: true, service: 'winzo-authoritative-server', database: 'firebase-realtime-database', websocket: true, uptime: process.uptime(), origins: ALLOWED_ORIGINS, rooms: roomInfo });
  } catch (e) {
    res.status(503).json({ ok: false, service: 'winzo-authoritative-server', database: 'unavailable', websocket: true, error: e.message || 'database-unavailable' });
  }
});

app.get('/identity/:telegramId', async (req, res) => {
  const suppliedKey = String(req.get('x-admin-key') || '');
  if (!ADMIN_KEY || !constantTimeEqual(suppliedKey, ADMIN_KEY)) return res.status(403).json({ ok: false, error: 'forbidden' });
  const telegramId = Number(req.params.telegramId);
  if (!Number.isSafeInteger(telegramId) || telegramId <= 0) return res.status(400).json({ ok: false, error: 'bad-telegram-id' });
  try {
    const mapped = await db.ref('telegram_users/' + telegramId).once('value');
    const byTelegram = await findUserByTelegramId(telegramId);
    const canonical = await readUserByUid(`tg_${telegramId}`);
    res.json({
      ok: true,
      telegramId,
      mapping: mapped.exists() ? mapped.val() : null,
      byTelegram: byTelegram ? { uid: byTelegram.uid, username: byTelegram.data.username, displayName: byTelegram.data.display_name, telegramId: byTelegram.data.telegram_id, main: toFiniteNumber(byTelegram.data.main_wallet), play: toFiniteNumber(byTelegram.data.play_wallet), pending: toFiniteNumber(byTelegram.data.pending_withdrawal) } : null,
      canonical: canonical ? { uid: canonical.uid, username: canonical.data.username, displayName: canonical.data.display_name, telegramId: canonical.data.telegram_id, main: toFiniteNumber(canonical.data.main_wallet), play: toFiniteNumber(canonical.data.play_wallet), pending: toFiniteNumber(canonical.data.pending_withdrawal) } : null
    });
  } catch (e) {
    res.status(503).json({ ok: false, error: e.message || 'identity-check-failed' });
  }
});

app.get('/config', (_, res) => res.json({
  ok: true,
  service: 'winzo-authoritative-server',
  database: 'firebase-realtime-database',
  walletSchema: 'users/<uid>/{main_wallet,play_wallet,pending_withdrawal,referral_earnings}',
  websocket: true
}));

app.get('/', (_, res) => res.type('text').send('Winzo authoritative Firebase server is running.'));

const server = http.createServer(app);

server.on('upgrade', (req, socket, head) => {
  const origin = req.headers.origin || '';
  const allowed = !origin || ALLOWED_ORIGINS.includes('*') || ALLOWED_ORIGINS.includes(origin);
  if (!allowed) {
    console.warn(`[WS-REJECT] origin="${origin}"`);
    socket.write('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n');
    socket.destroy();
    return;
  }
  console.log(`[WS-ACCEPT] origin="${origin || '(none)'}"`);
  wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
});

async function gracefulShutdown(signal) {
  console.log(`[SHUTDOWN] ${signal}`);
  try {
    for (const room of Object.values(rooms)) {
      await room.mutex.run(async () => { if (room.dirty) await room.persist(); });
      clearBotTimers(room);
    }
  } catch (e) { console.error('[SHUTDOWN] persist failed', e); }
  for (const ws of wss.clients) { try { ws.close(1001, 'server shutting down'); } catch {} }
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000).unref();
}
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

loadRooms().then(() => {
  server.listen(PORT, '0.0.0.0', () => console.log(`Winzo Realtime Database server listening on :${PORT}`));
  for (const room of Object.values(rooms)) {
    if (room.phase === 'selection' && room.endsAt <= now()) { room.endsAt = now() + SELECTION_SECONDS * 1000; room.dirty = true; }
    if (room.phase === 'selection') normalizeRoundBots(room);
    room.persist();
    scheduleBots(room);
  }
  console.log(`Realtime Database: ${FIREBASE_DATABASE_URL}`);
  console.log(`Bot pool size: ${BOT_NAMES.length}`);
  console.log(`Bots per round: ${BOT_MIN}–${BOT_MAX}`);
  console.log(`Results phase length: ${RESULTS_SECONDS}s`);
  console.log(`Allowed WebSocket origins: ${ALLOWED_ORIGINS.join(', ')}`);
}).catch(err => {
  console.error("Failed to load rooms from Realtime Database. Check your credentials and database URL.", err);
  process.exit(1);
});
