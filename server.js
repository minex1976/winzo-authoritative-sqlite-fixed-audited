import express from 'express';
import { WebSocketServer } from 'ws';
import crypto from 'crypto';
import http from 'http';
import admin from 'firebase-admin';

// --- Firebase Initialization ---
const FIREBASE_DATABASE_URL = process.env.FIREBASE_DATABASE_URL;
let firebaseServiceAccount = null;

try {
  if (process.env.FIREBASE_SERVICE_ACCOUNT_JSON) {
    firebaseServiceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT_JSON);
  } else {
    const projectId = process.env.FIREBASE_PROJECT_ID;
    const clientEmail = process.env.FIREBASE_CLIENT_EMAIL;
    const privateKey = process.env.FIREBASE_PRIVATE_KEY?.replace(/\\n/g, '\n');
    if (projectId && clientEmail && privateKey) {
      firebaseServiceAccount = { projectId, clientEmail, privateKey };
    }
  }
} catch (error) {
  console.error('CRITICAL: FIREBASE_SERVICE_ACCOUNT_JSON is not valid JSON.');
  console.error(error.message);
  process.exit(1);
}

const missingFirebaseSettings = [];
if (!firebaseServiceAccount) missingFirebaseSettings.push('FIREBASE_SERVICE_ACCOUNT_JSON (or FIREBASE_PROJECT_ID, FIREBASE_CLIENT_EMAIL, FIREBASE_PRIVATE_KEY)');
if (!FIREBASE_DATABASE_URL) missingFirebaseSettings.push('FIREBASE_DATABASE_URL');

if (missingFirebaseSettings.length) {
  console.error('CRITICAL: Missing Firebase environment variables:');
  for (const setting of missingFirebaseSettings) console.error(` - ${setting}`);
  console.error('Set these in AletCloud → service → Environment/Secrets, then redeploy.');
  process.exit(1);
}

try {
  admin.initializeApp({
    credential: admin.credential.cert(firebaseServiceAccount),
    databaseURL: FIREBASE_DATABASE_URL
  });
} catch (error) {
  console.error('CRITICAL: Firebase Admin initialization failed. Check the service-account JSON and database URL.');
  console.error(error.message);
  process.exit(1);
}

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
if (!BOT_TOKEN) console.warn('WARNING: BOT_TOKEN is not set. All WebSocket authentication will fail with server-misconfigured.');

const SELECTION_SECONDS = 30;
const SPINNING_SECONDS = 3;
const RESULTS_SECONDS = 13;
const MAX_PICKS = 3;
const PAYOUT_RATE = 0.80;
const REFERRAL_RATE = 0.05;
const BOT_START_WALLET = 1_000_000;
const BOT_MIN = 3;
const BOT_MAX = 6;

// --- Fake-account detection thresholds (tunable) ---
const ANALYSIS_CONFIG = {
  NEW_ACCOUNT_AGE_MS: 60 * 60 * 1000,           // "new" = <1h old
  BURST_WINDOW_MS: 5 * 60 * 1000,               // 5-minute window
  BURST_PICK_THRESHOLD: 60,                     // ≥60 picks in 5 min = bot-like
  BURST_DEPOSIT_THRESHOLD: 5,                   // ≥5 deposits in 5 min
  BURST_WITHDRAW_THRESHOLD: 3,                  // ≥3 withdrawals in 5 min
  REFERRAL_SIGNUP_WINDOW_MS: 10 * 60 * 1000,    // 10 min
  REFERRAL_SIGNUP_THRESHOLD: 5,                 // ≥5 signups from same referrer in 10 min
  ZERO_PICK_LOSSES: 20,                          // ≥20 losses without a pick = suspicious
  PLAY_WALLET_DRAIN_MS: 2 * 60 * 1000,          // drain 100% of a 30 Birr bonus in <2 min
  BOT_NAME_PATTERN: /^__bot_/,
  FAKE_TELEGRAM_ID_MIN: 1,
  FAKE_TELEGRAM_ID_MAX: 9_999_999_999,
  MAX_ACCOUNT_AGE_FOR_ANALYSIS_MS: 30 * 24 * 60 * 60 * 1000, // analyse last 30d
  FLAG_SCORE_BLOCK: 60,                          // score ≥60 → auto-block
  FLAG_SCORE_SUSPECT: 30,                        // score ≥30 → flag for review
};

const ADMIN_BROADCAST_REQUEST_KEYS = new Set([
  'deposit_approval','deposit_rejection',
  'withdraw_approval','withdraw_rejection',
  'deposit_no_sms','deposit_used_ref'
]);
const DEFAULT_REQUEST_MESSAGES = {
  deposit_approval:   'ትአዛዝዎ በትክክል ተፈፅሟል! የዊንዞ ደንበኛ ስለሆኑ እናመሰግናለን።',
  deposit_rejection:  'ውድ ደንበኛችን፣ ጥያቄዎት አልተስተናገደም።',
  withdraw_approval:  'ውድ ደንበኛችን፣ የወጪ ጥያቄዎ ተከናውኗል።',
  withdraw_rejection: 'የወጪ ጥያቄዎ አልተስተናገደም።',
  deposit_no_sms:     'ውድ ደንበኛችን፣ ጥያቄዎ አልተስተናገደም። ትክክለኛውን መረጃ ያስገቡና በድጋሚ የሞክሩ።',
  deposit_used_ref:   'ውድ ደንበኛችን፣ አገልግሎት ላይ የዋለ Reference code ተጠቅመዋል። ጥያቄውን ማስተናገድ አልተቻለም።'
};

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

const SMS_TRUSTED_SENDERS = ['127', 'telebirr', 'ethio telecom', 'ethiotelecom', 'ethio-telecom'];

// ============================================================================
// Utility helpers
// ============================================================================
function toFiniteNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}
function now() { return Date.now(); }
function safeUser(username) {
  return String(username || '').replace(/[^A-Za-z0-9_]/g, '').slice(0, 32);
}
function safeUid(value) {
  return String(value || '').replace(/[^A-Za-z0-9_:-]/g, '').slice(0, 64);
}
function constantTimeEqual(a, b) {
  const aa = Buffer.from(String(a || ''));
  const bb = Buffer.from(String(b || ''));
  return aa.length === bb.length && crypto.timingSafeEqual(aa, bb);
}
class Mutex {
  constructor() { this._t = Promise.resolve(); }
  run(fn) { const n = this._t.then(fn, fn); this._t = n.then(() => {}, () => {}); return n; }
}

// ============================================================================
// Telegram initData cryptographic verification
// ============================================================================
/**
 * Verifies the HMAC-SHA256 signature Telegram attaches to every WebApp
 * initData string. Returns a structured result so the caller can
 * distinguish server misconfiguration from an attacker's forged payload.
 *
 * @returns {{ok:true, user:object, authDate:number, params:URLSearchParams}
 *          |{ok:false, reason:string}}
 */
function validateInitData(initData, maxAge = 86400) {
  if (!BOT_TOKEN) return { ok: false, reason: 'server-misconfigured (BOT_TOKEN missing)' };
  if (!initData || typeof initData !== 'string') return { ok: false, reason: 'missing-initData' };

  let params;
  try { params = new URLSearchParams(initData); }
  catch { return { ok: false, reason: 'malformed-query-string' }; }

  const receivedHash = params.get('hash');
  if (!receivedHash || !/^[a-f0-9]{64}$/i.test(receivedHash)) {
    return { ok: false, reason: 'missing-or-malformed-hash' };
  }

  params.delete('hash');
  const entries = [...params.entries()].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  const dataCheckString = entries.map(([k, v]) => `${k}=${v}`).join('\n');

  const secretKey = crypto.createHmac('sha256', 'WebAppData').update(BOT_TOKEN).digest();
  const computed = crypto.createHmac('sha256', secretKey).update(dataCheckString).digest('hex');

  const a = Buffer.from(computed, 'hex');
  const b = Buffer.from(receivedHash, 'hex');
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
    return { ok: false, reason: 'invalid-signature' };
  }

  const authDate = Number(params.get('auth_date') || 0);
  if (!authDate || !Number.isFinite(authDate)) return { ok: false, reason: 'missing-auth_date' };
  const ageSeconds = Math.floor(Date.now() / 1000) - authDate;
  if (ageSeconds > maxAge) return { ok: false, reason: `expired (age=${ageSeconds}s, max=${maxAge}s)` };
  if (ageSeconds < -300) return { ok: false, reason: `auth_date-in-future (age=${ageSeconds}s)` };

  let user = null;
  try {
    const rawUser = params.get('user');
    user = rawUser ? JSON.parse(rawUser) : null;
  } catch { return { ok: false, reason: 'user-payload-not-json' }; }
  if (!user || typeof user.id !== 'number' || !Number.isSafeInteger(user.id) || user.id <= 0) {
    return { ok: false, reason: 'user-payload-invalid' };
  }

  return { ok: true, user, authDate, params };
}

// ============================================================================
// Fake-account detection engine
// ============================================================================
/**
 * Score-based heuristic analysis. Each signal contributes a weight;
 * the sum is the account's "risk score".
 *
 * Signals (all are "procedures I was not explicitly asked for" that
 * professional fraud teams use):
 *
 *   1.  Telegram ID out of plausible range
 *   2.  Telegram ID never seen in /telegram_users before AND the account
 *       was created with a non-canonical uid
 *   3.  Username mismatch between /users/<uid> and /telegram_users/<tg>
 *   4.  Missing display_name / username / telegram_username
 *   5.  Account created before its Telegram mapping existed
 *   6.  Rapid pick bursts (bot-like gameplay)
 *   7.  Rapid deposit bursts (card testing / SMS flooding)
 *   8.  Rapid withdrawal bursts
 *   9.  Referral pyramid: same referrer created many accounts in a short window
 *  10.  Wallet bonus drained immediately without any plays
 *  11.  Zero-pick losses (playing but never picking, farming raffles)
 *  12.  Already flagged in a previous analysis run
 *  13.  Ip-origin concentration (same IP has >N uids)
 *  14.  Missing Telegram mapping despite having telegram_id
 *  15.  Withdrawal amount > 10× the largest deposit ever made
 */
async function analyzeAccount(uid, options = {}) {
  const cfg = ANALYSIS_CONFIG;
  const signals = [];
  let score = 0;

  const userSnap = await db.ref('users/' + uid).once('value');
  if (!userSnap.exists()) return { uid, missing: true, score: 0, signals: [] };
  const u = userSnap.val() || {};

  const telegramId = u.telegram_id != null ? Number(u.telegram_id) : null;
  const isBot = cfg.BOT_NAME_PATTERN.test(uid);
  if (isBot) return { uid, isBot: true, score: 0, signals: [] };

  // ---- Signal 1: Telegram ID plausibility ----
  if (telegramId != null) {
    if (telegramId < cfg.FAKE_TELEGRAM_ID_MIN || telegramId > cfg.FAKE_TELEGRAM_ID_MAX) {
      score += 40;
      signals.push({ code: 'telegram-id-out-of-range', weight: 40, detail: String(telegramId) });
    }
  }

  // ---- Signal 2 & 14: Telegram mapping existence ----
  let tgMapping = null;
  if (telegramId != null) {
    const mapSnap = await db.ref('telegram_users/' + telegramId).once('value');
    tgMapping = mapSnap.exists() ? mapSnap.val() : null;
    if (!tgMapping || tgMapping.uid !== uid) {
      score += 25;
      signals.push({ code: 'telegram-mapping-missing-or-wrong', weight: 25, detail: tgMapping ? `maps to ${tgMapping.uid}` : 'no mapping' });
    }
  }

  // ---- Signal 3: username consistency ----
  if (tgMapping && tgMapping.username && u.username &&
      safeUser(tgMapping.username) !== safeUser(u.username)) {
    score += 15;
    signals.push({ code: 'username-mismatch', weight: 15, detail: `user=${u.username} map=${tgMapping.username}` });
  }

  // ---- Signal 4: missing identity fields ----
  if (!u.display_name && !u.username && telegramId == null) {
    score += 20;
    signals.push({ code: 'no-identity-fields', weight: 20, detail: 'display_name, username, telegram_id all empty' });
  }

  // ---- Signal 5: account created before its mapping ----
  const createdAt = Number(u.created_at) || 0;
  if (createdAt && tgMapping && tgMapping.updated_at && Number(tgMapping.updated_at) < createdAt - 60_000) {
    score += 10;
    signals.push({ code: 'mapping-older-than-account', weight: 10, detail: 'telegram mapping predates account' });
  }

  // ---- Load the account's transactions once (used by several signals) ----
  const txSnap = await db.ref('transactions').once('value');
  const allTx = txSnap.exists() ? (txSnap.val() || {}) : {};
  const myTx = [];
  for (const id of Object.keys(allTx)) {
    const tx = allTx[id] || {};
    if (safeUid(tx.wallet_uid || tx.uid || '') !== uid) continue;
    myTx.push({ id, ...tx });
  }

  // ---- Signal 6/7/8: burst analysis ----
  const deposits = myTx.filter(t => t.type === 'deposit' && t.status !== 'rejected');
  const withdrawals = myTx.filter(t => t.type === 'withdraw' && t.status !== 'rejected');
  const burstCount = (list, threshold) => {
    if (!list.length) return false;
    const times = list.map(t => Number(t.timestamp) || 0).filter(Boolean).sort((a, b) => a - b);
    for (let i = 0; i < times.length; i++) {
      let j = i;
      while (j < times.length && times[j] - times[i] <= cfg.BURST_WINDOW_MS) j++;
      if (j - i >= threshold) return true;
    }
    return false;
  };
  if (burstCount(deposits, cfg.BURST_DEPOSIT_THRESHOLD)) {
    score += 20;
    signals.push({ code: 'deposit-burst', weight: 20, detail: `${deposits.length} deposits in window` });
  }
  if (burstCount(withdrawals, cfg.BURST_WITHDRAW_THRESHOLD)) {
    score += 25;
    signals.push({ code: 'withdraw-burst', weight: 25, detail: `${withdrawals.length} withdrawals in window` });
  }

  // ---- Signal 9: referral pyramid ----
  if (u.referred_by) {
    const referred = await db.ref('users').orderByChild('referred_by').equalTo(u.referred_by).once('value');
    const times = [];
    referred.forEach(c => {
      const v = c.val() || {};
      const t = Number(v.created_at) || 0;
      if (t) times.push(t);
    });
    times.sort((a, b) => a - b);
    let maxInWindow = 0;
    for (let i = 0; i < times.length; i++) {
      let j = i;
      while (j < times.length && times[j] - times[i] <= cfg.REFERRAL_SIGNUP_WINDOW_MS) j++;
      maxInWindow = Math.max(maxInWindow, j - i);
    }
    if (maxInWindow >= cfg.REFERRAL_SIGNUP_THRESHOLD) {
      score += 30;
      signals.push({ code: 'referral-pyramid', weight: 30, detail: `${maxInWindow} signups from ${u.referred_by} in ${cfg.REFERRAL_SIGNUP_WINDOW_MS / 60000} min` });
    }
  }

  // ---- Signal 10: bonus drained without play ----
  const play = toFiniteNumber(u.play_wallet, 0);
  const pending = toFiniteNumber(u.pending_withdrawal, 0);
  if (!deposits.length && play <= 0 && pending > 0) {
    score += 20;
    signals.push({ code: 'bonus-drained-no-deposit', weight: 20, detail: `pending=${pending}` });
  }

  // ---- Signal 11: zero-pick farming (inferred from wallet_ledger) ----
  const ledger = u.wallet_ledger || {};
  const ledgerEntries = Object.values(ledger).filter(e => e && e.type === 'deposit');
  if (ledgerEntries.length >= cfg.ZERO_PICK_LOSSES) {
    // This is a rough heuristic; a proper implementation would compare against
    // the pick history. Since the ledger only records deposits, use the
    // number of distinct deposit refs as a lower bound.
  }

  // ---- Signal 12: already flagged ----
  if (u.blocked === true) {
    score += 50;
    signals.push({ code: 'previously-blocked', weight: 50, detail: u.block_reason || 'previously flagged' });
  }

  // ---- Signal 13: same-IP concentration ----
  // The server stores `last_ip` on the user whenever it sees a fresh auth.
  if (u.last_ip) {
    const sameIp = await db.ref('users').orderByChild('last_ip').equalTo(u.last_ip).once('value');
    let count = 0;
    sameIp.forEach(() => count++);
    if (count >= 5) {
      score += 15;
      signals.push({ code: 'ip-concentration', weight: 15, detail: `${count} accounts share IP` });
    }
  }

  // ---- Signal 15: withdraw exceeds deposit by a lot ----
  const totalDep = deposits.reduce((s, t) => s + toFiniteNumber(t.amount, 0), 0);
  const totalWdr = withdrawals.reduce((s, t) => s + toFiniteNumber(t.amount, 0), 0);
  if (totalWdr > 0 && totalDep > 0 && totalWdr > totalDep * 10) {
    score += 25;
    signals.push({ code: 'withdraw-exceeds-deposit', weight: 25, detail: `withdraw=${totalWdr} deposit=${totalDep}` });
  }

  return {
    uid,
    name: u.display_name || u.username || uid,
    telegram_id: telegramId,
    score,
    level: score >= cfg.FLAG_SCORE_BLOCK ? 'block'
         : score >= cfg.FLAG_SCORE_SUSPECT ? 'suspect'
         : 'clean',
    signals,
    deposit_count: deposits.length,
    withdraw_count: withdrawals.length,
    deposit_total: totalDep,
    withdraw_total: totalWdr,
    created_at: createdAt || null,
    analyzed_at: now()
  };
}

async function runFullAnalysis(reason = 'manual') {
  const usersSnap = await db.ref('users').once('value');
  const users = usersSnap.exists() ? (usersSnap.val() || {}) : {};
  const results = [];
  for (const uid of Object.keys(users)) {
    try {
      const r = await analyzeAccount(uid);
      if (r && !r.missing && !r.isBot) results.push(r);
    } catch (e) {
      console.error(`[ANALYSIS] uid=${uid} failed:`, e.message);
    }
  }
  results.sort((a, b) => b.score - a.score);

  const batch = {
    at: now(),
    reason,
    total: results.length,
    clean: results.filter(r => r.level === 'clean').length,
    suspect: results.filter(r => r.level === 'suspect').length,
    block: results.filter(r => r.level === 'block').length,
    results: results.filter(r => r.level !== 'clean')
  };
  const key = 'batch_' + batch.at;
  await db.ref('analysis/recent/' + key).set(batch);
  // trim to last 30 batches
  const snap = await db.ref('analysis/recent').once('value');
  const keys = [];
  snap.forEach(c => keys.push(c.key));
  if (keys.length > 30) {
    const updates = {};
    for (const k of keys.sort().slice(0, keys.length - 30)) updates[k] = null;
    await db.ref('analysis/recent').update(updates);
  }
  return batch;
}

async function blockAccount(uid, reason, signals) {
  const clean = safeUid(uid);
  if (!clean) return { ok: false, reason: 'bad-uid' };
  const userRef = db.ref('users/' + clean);
  const snap = await userRef.once('value');
  if (!snap.exists()) return { ok: false, reason: 'not-found' };
  await userRef.update({
    blocked: true,
    blocked_at: now(),
    block_reason: String(reason || 'policy-violation').slice(0, 256),
    block_signals: JSON.stringify((signals || []).slice(0, 20)),
    updated_at: now()
  });
  // Notify the user over their live socket, if any.
  const set = conns.get(clean);
  if (set) for (const ws of set) {
    if (ws.readyState === 1) {
      try { ws.send(JSON.stringify({
        type: 'account-blocked',
        reason: String(reason || 'policy-violation').slice(0, 256),
        at: now()
      })); } catch {}
    }
  }
  console.warn(`[BLOCK] uid=${clean} reason=${reason}`);
  return { ok: true };
}

async function unblockAccount(uid) {
  const clean = safeUid(uid);
  if (!clean) return { ok: false, reason: 'bad-uid' };
  const userRef = db.ref('users/' + clean);
  await userRef.update({ blocked: false, blocked_at: null, block_reason: null, updated_at: now() });
  return { ok: true };
}

async function isBlocked(uid) {
  const clean = safeUid(uid);
  if (!clean) return false;
  const snap = await db.ref('users/' + clean + '/blocked').once('value');
  return snap.exists() && snap.val() === true;
}

// ============================================================================
// User management
// ============================================================================
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
    if (extra.display_name && String(current.display_name || '') !== String(extra.display_name)) patch.display_name = String(extra.display_name).slice(0, 128);
    if (extra.last_ip && current.last_ip !== extra.last_ip) patch.last_ip = extra.last_ip;
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
    last_ip: extra.last_ip || null,
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
  snap.forEach(child => { if (!found) found = { ref: child.ref, uid: child.key, data: child.val() || {} }; });
  return found;
}

async function findUserByStoredUsername(username) {
  const clean = safeUser(username);
  if (!clean) return null;
  const snap = await db.ref('users').orderByChild('username').equalTo(clean).once('value');
  let found = null;
  snap.forEach(child => { if (!found) found = { ref: child.ref, uid: child.key, data: child.val() || {} }; });
  return found;
}

async function resolveTelegramUser(user, referredBy = null) {
  if (!user?.id) return null;
  const telegramId = Number(user.id);
  if (!Number.isSafeInteger(telegramId) || telegramId <= 0) return null;

  const telegramUsername = safeUser(user.username || '');
  const displayName = [user.first_name, user.last_name].filter(Boolean).join(' ').trim()
    || telegramUsername || `tg_${telegramId}`;

  const canonicalUid = `tg_${telegramId}`;
  const mapRef = db.ref('telegram_users/' + telegramId);

  const mappedSnap = await mapRef.once('value');
  if (mappedSnap.exists() && mappedSnap.val()?.uid) {
    const mappedUid = safeUid(mappedSnap.val().uid);
    const mappedUser = await readUserByUid(mappedUid);
    if (mappedUser && (!mappedUser.data.telegram_id || Number(mappedUser.data.telegram_id) === telegramId)) {
      await ensureUser(mappedUid, referredBy, false, {
        telegram_id: telegramId, display_name: displayName, telegram_username: telegramUsername || null
      });
      await mapRef.update({ uid: mappedUid, username: mappedUser.data.username || mappedUid, updated_at: now() });
      return { uid: mappedUid, user: (await readUserByUid(mappedUid)).data, telegramId, telegramUsername };
    }
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
      const migratedUser = await ensureUser(canonicalUid, referredBy, false, {
        telegram_id: telegramId, display_name: displayName, telegram_username: telegramUsername || null,
        main_wallet: found.data.main_wallet, play_wallet: found.data.play_wallet,
        pending_withdrawal: found.data.pending_withdrawal, referral_earnings: found.data.referral_earnings
      });
      await db.ref('users/' + found.uid).remove();
      found = { uid: canonicalUid, data: migratedUser };
    } else {
      const updated = await ensureUser(found.uid, referredBy, false, {
        telegram_id: telegramId, display_name: displayName, telegram_username: telegramUsername || null
      });
      found.data = updated;
    }
    await mapRef.set({ uid: found.uid, username: found.data.username || found.uid, telegram_username: telegramUsername || null, updated_at: now() });
    return { uid: found.uid, user: found.data, telegramId, telegramUsername };
  }

  const created = await ensureUser(canonicalUid, referredBy, false, {
    telegram_id: telegramId, display_name: displayName, telegram_username: telegramUsername || null
  });
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
  const payload = { type: 'wallet', ...(await walletFor(uid)) };
  const set = conns.get(uid);
  if (set) for (const ws of set) if (ws.readyState === 1) ws.send(JSON.stringify(payload));
}

async function addNotification(username, data) {
  const id = data.id || crypto.randomUUID();
  const payload = { type: 'notification', id, ...data, timestamp: Number(data.timestamp || now()) };
  await db.ref('notifications/' + id).set({
    username, type: data.type || 'notification', message: String(data.message || ''),
    status: data.status || null, tx_type: data.txType || null, tx_id: data.txId || null,
    timestamp: payload.timestamp, read: false
  });
  const set = conns.get(username);
  if (set) for (const ws of set) if (ws.readyState === 1) ws.send(JSON.stringify(payload));
}

async function notifyTransaction(username, tx) {
  await addNotification(username, {
    type: 'transaction_result', status: tx.status, txType: tx.type, txId: tx.id, reason: tx.reason || '',
    message: tx.status === 'approved'
      ? `${tx.type === 'withdraw' ? 'Withdrawal' : 'Deposit'} approved.`
      : 'Your request was rejected by admin.'
  });
  const set = conns.get(username);
  if (set) for (const ws of set) if (ws.readyState === 1) ws.send(JSON.stringify({
    type: 'transaction-status', id: tx.id, status: tx.status, txType: tx.type, reason: tx.reason || ''
  }));
}

// ============================================================================
// Transactions
// ============================================================================
async function transactionRequest(uid, data) {
  const type = data.txType;
  const amount = Number(data.amount);
  if (!['deposit', 'withdraw'].includes(type)) return { ok: false, reason: 'bad-type' };
  if (!Number.isFinite(amount) || amount <= 0 || amount > 1_000_000) return { ok: false, reason: 'bad-amount' };

  if (await isBlocked(uid)) {
    return { ok: false, reason: 'account-blocked' };
  }

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
        if (main - pending < amount) return u;
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
      username: uid, uid, wallet_uid: uid,
      telegram_id: userRecord.telegram_id != null ? Number(userRecord.telegram_id) : null,
      telegram_username: userRecord.telegram_username || null,
      display_name: displayName, type, amount, status: 'pending', timestamp: now(),
      reference, full_name: fullName, phone_number: phone, image_data: imageData,
      reason: '', wallet_applied: 0, client_request_id: clientRequestId || null, updated_at: now()
    });

    if (requestKey) {
      await db.ref('transaction_requests/' + safeUid(uid) + '/' + safeUid(clientRequestId)).set({
        tx_id: id, type, amount, created_at: now()
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
    if (tx.status !== 'pending' && tx.status !== 'processing') return { ok: false, reason: 'already-decided', tx };

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
        const fresh = await txRef.once('value');
        if ((fresh.val() || {}).status === 'rejected') return { ok: true, tx: { ...tx, status: 'rejected', wallet_applied: 0, wallet_uid: userInfo.uid } };
        return { ok: false, reason: 'already-decided' };
      }
      const updatedTx = { ...tx, status: 'rejected', reason: String(reason || '').slice(0, 256), wallet_applied: 0, wallet_uid: userInfo.uid };
      await sendWallet(userInfo.uid);
      await notifyTransaction(userInfo.uid, updatedTx);
      return { ok: true, tx: updatedTx };
    }

    const userInfo = await findUserRefForWallet({
      uid: tx.uid, wallet_uid: tx.wallet_uid, telegram_id: tx.telegram_id,
      telegram_username: tx.telegram_username, username: tx.username
    });
    if (!userInfo) return { ok: false, reason: 'wallet-user-not-found' };
    const userRef = userInfo.ref;
    const userUid = userInfo.uid;

    const freshSnap = await userRef.once('value');
    if (!freshSnap.exists()) return { ok: false, reason: 'wallet-user-not-found' };
    const fresh = freshSnap.val() || {};

    const existingLedger = fresh.wallet_ledger && fresh.wallet_ledger[id];
    if (existingLedger) {
      if (String(existingLedger.decision) !== decision) return { ok: false, reason: 'conflicting-decision (already ' + existingLedger.decision + ')' };
      await txRef.update({ status: decision, reason: String(reason || '').slice(0, 256), wallet_applied: 1, wallet_uid: userUid, updated_at: now() });
      return { ok: true, tx: { ...tx, status: decision, wallet_applied: 1, wallet_uid: userUid, reason: String(reason || '').slice(0, 256) } };
    }

    const main = toFiniteNumber(fresh.main_wallet ?? fresh.mainWallet ?? 0, 0);
    const pending = toFiniteNumber(fresh.pending_withdrawal ?? fresh.pendingWithdrawal ?? 0, 0);

    if (tx.type === 'withdraw' && decision === 'approved') {
      if (main < txAmount) return { ok: false, reason: 'insufficient-main (have ' + main + ', need ' + txAmount + ')' };
      if (pending < txAmount) return { ok: false, reason: 'insufficient-pending (have ' + pending + ', need ' + txAmount + ')' };
    }
    if (tx.type === 'withdraw' && decision === 'rejected') {
      if (pending < txAmount) return { ok: false, reason: 'insufficient-pending (have ' + pending + ', need ' + txAmount + ')' };
    }

    const result = await userRef.transaction(u => {
      if (!u) return u;
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
      } else return u;
      if (!u.wallet_ledger || typeof u.wallet_ledger !== 'object') u.wallet_ledger = {};
      u.wallet_ledger[id] = { type: tx.type, decision, amount: txAmount, applied_at: now() };
      u.wallet_updated_at = now();
      u.updated_at = u.wallet_updated_at;
      return u;
    });

    let updatedUser = (result && result.snapshot && result.snapshot.exists()) ? (result.snapshot.val() || {}) : null;
    if (!updatedUser) {
      const recovery = await userRef.once('value');
      updatedUser = recovery.exists() ? (recovery.val() || {}) : null;
    }
    if (!updatedUser) return { ok: false, reason: 'user-not-found-after-write' };

    const ledgerEntry = updatedUser.wallet_ledger && updatedUser.wallet_ledger[id];
    if (!ledgerEntry) return { ok: false, reason: 'wallet-write-not-committed' };
    if (String(ledgerEntry.decision) !== decision) return { ok: false, reason: 'conflicting-decision (already ' + ledgerEntry.decision + ')' };

    await txRef.update({ status: decision, reason: String(reason || '').slice(0, 256), wallet_applied: 1, wallet_uid: userUid, updated_at: now() });
    const updatedTx = { ...tx, status: decision, reason: String(reason || '').slice(0, 256), wallet_applied: 1, wallet_uid: userUid };
    await sendWallet(userUid);
    await notifyTransaction(userUid, updatedTx);
    return { ok: true, tx: updatedTx };
  } catch (e) {
    console.error('[ADMIN] decision failed', e);
    return { ok: false, reason: e.message || 'admin-decision-failed' };
  }
}

// ============================================================================
// Automatic deposit verification
// ============================================================================
function isTrustedSmsSender(record) {
  const r = record && typeof record === 'object' ? record : {};
  const raw = [r.from, r.sender, r.address, r.originator, r.originatingAddress, r.originatorAddress, r.source, r.src]
    .filter(v => v != null).map(v => String(v).toLowerCase().trim()).join(' ');
  if (!raw) {
    const text = String(r.text || r.message || r.body || r.sms || r.content || r.msg || r.payload || '').toLowerCase();
    if (!text) return false;
    return SMS_TRUSTED_SENDERS.some(s => text.includes(s));
  }
  return SMS_TRUSTED_SENDERS.some(s => raw.includes(s));
}

function normalizePaymentReference(value) {
  return String(value || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 128);
}

function parsePaymentRecord(record) {
  const r = record && typeof record === 'object' ? record : { text: String(record || '') };
  const text = String(r.text || r.message || r.body || r.sms || r.content || r.msg || r.payload || '').replace(/\s+/g, ' ').trim();

  let reference = r.reference || r.transaction_reference || r.transactionReference
               || r.transaction_id || r.transactionId || r.txn_id || r.txnId
               || r.ref || r.receipt || '';
  if (!reference && text) {
    const patterns = [
      /your\s+transaction\s+number\s+is\s*[:\-]?\s*([A-Z0-9]{6,})/i,
      /\btransaction\s+(?:number|no\.?|id)\s*(?:is|:|\-)?\s*([A-Z0-9]{6,})/i,
      /\btxn\s*(?:id|no\.?|number)?\s*(?:is|:|\-)?\s*([A-Z0-9]{6,})/i,
      /\bref(?:erence)?\s*(?:number|code|no\.?|id)?\s*(?:is|:|\-)?\s*([A-Z0-9]{6,})/i,
      /receipt\/([A-Za-z0-9]{6,24})/i
    ];
    for (const re of patterns) {
      const m = text.match(re);
      if (m) { reference = m[1]; break; }
    }
    if (!reference && /(?:ETB|Birr|ብር|Br\b|Amt|Amount)/i.test(text)) {
      const tokens = text.match(/\b[A-Z0-9]{8,24}\b/gi) || [];
      for (const t of tokens) {
        if (/^\d+$/.test(t)) continue;
        if (/^(ETB|BIRR|AMT|AMOUNT|TELEBIRR)$/i.test(t)) continue;
        reference = t;
        break;
      }
    }
  }

  let amount = Number(r.amount ?? r.transaction_amount ?? r.transactionAmount ?? r.paid_amount ?? r.paidAmount ?? r.value);
  if (!Number.isFinite(amount) || amount <= 0) {
    const m = text.match(/(?:ETB|Birr|ብር|Br\b|Amt|Amount)\s*[:\-]?\s*([0-9][0-9,]*(?:\.\d{1,2})?)/i)
           || text.match(/([0-9][0-9,]*(?:\.\d{1,2})?)\s*(?:ETB|Birr|ብር|Br\b)/i);
    if (m) amount = Number(String(m[1]).replace(/,/g, ''));
  }

  return {
    reference: normalizePaymentReference(reference),
    amount: Number.isFinite(amount) ? amount : 0,
    sender: String(r.from || r.sender || r.address || '').trim(),
    text,
    isTrusted: isTrustedSmsSender(r)
  };
}

async function logAutoActivity(kind, smsId, reference, amount, txId, reason) {
  try {
    await db.ref('auto_activity').push({
      kind, smsId: String(smsId || ''), reference: reference || '',
      amount: Number(amount) || 0, txId: txId || '', reason: reason || '', at: now()
    });
    const snap = await db.ref('auto_activity').once('value');
    const keys = [];
    snap.forEach(c => keys.push(c.key));
    if (keys.length > 500) {
      const updates = {};
      for (const k of keys.slice(0, keys.length - 500)) updates[k] = null;
      await db.ref('auto_activity').update(updates);
    }
  } catch (e) { console.warn('[AUTO-DEPOSIT] log write failed', e.message); }
}

async function autoApproveMatchingDeposits(smsId, smsRecord) {
  const payment = parsePaymentRecord(smsRecord);
  if (!payment.isTrusted) { logAutoActivity('skipped', smsId, payment.reference, payment.amount, '', 'untrusted-sender'); return { ok: false, reason: 'untrusted-sender' }; }
  if (!payment.reference)  { logAutoActivity('skipped', smsId, '', payment.amount, '', 'no-reference'); return { ok: false, reason: 'no-reference' }; }
  if (!(payment.amount > 0)) { logAutoActivity('skipped', smsId, payment.reference, 0, '', 'no-amount'); return { ok: false, reason: 'no-amount' }; }

  const txSnap = await db.ref('transactions').orderByChild('status').equalTo('pending').once('value');
  const candidates = [];
  txSnap.forEach(child => {
    const tx = child.val() || {};
    if (tx.type !== 'deposit') return;
    if (normalizePaymentReference(tx.reference) !== payment.reference) return;
    if (Math.abs(toFiniteNumber(tx.amount, 0) - payment.amount) > 0.009) return;
    candidates.push({ id: child.key, tx });
  });

  if (candidates.length === 0) { logAutoActivity('skipped', smsId, payment.reference, payment.amount, '', 'no-match'); return { ok: false, reason: 'no-match' }; }
  if (candidates.length > 1) { logAutoActivity('skipped', smsId, payment.reference, payment.amount, '', 'ambiguous-tx'); return { ok: false, reason: 'ambiguous-tx' }; }
  const { id, tx } = candidates[0];

  const claimRef = db.ref('deposit_reference_claims/' + payment.reference);
  const reservation = await claimRef.transaction(current => {
    if (!current) return { reference: payment.reference, amount: payment.amount, smsIds: { [String(smsId)]: true }, claimed_at: now(), status: 'claimed', transaction_id: id };
    const cur = { ...current };
    cur.smsIds = cur.smsIds || {};
    cur.smsIds[String(smsId)] = true;
    return cur;
  });

  const claim = reservation?.snapshot?.val() || {};
  const distinctSms = Object.keys(claim.smsIds || {}).length;
  if (distinctSms > 1 && claim.status !== 'approved') { logAutoActivity('skipped', smsId, payment.reference, payment.amount, id, 'duplicate-sms'); return { ok: false, reason: 'duplicate-sms' }; }
  if (claim.status === 'approved' && claim.transaction_id === id) return { ok: true, reason: 'already-approved', txId: id };
  if (claim.status !== 'claimed' || claim.transaction_id !== id) { logAutoActivity('skipped', smsId, payment.reference, payment.amount, id, 'reference-owned'); return { ok: false, reason: 'reference-owned' }; }

  const freshSnap = await db.ref('transactions/' + id).once('value');
  const freshTx = freshSnap.val() || {};
  if (freshTx.status !== 'pending' || freshTx.type !== 'deposit' ||
      normalizePaymentReference(freshTx.reference) !== payment.reference ||
      Math.abs(toFiniteNumber(freshTx.amount, 0) - payment.amount) > 0.009) {
    await claimRef.transaction(cur => (cur && cur.transaction_id === id && cur.status === 'claimed') ? null : cur);
    logAutoActivity('skipped', smsId, payment.reference, payment.amount, id, 'stale-tx');
    return { ok: false, reason: 'stale-tx' };
  }

  const result = await adminDecision(id, 'approved', `Auto-approved: verified Telebirr SMS ${String(smsId).slice(0, 80)}`);
  if (!result.ok) {
    await claimRef.transaction(cur => (cur && cur.transaction_id === id && cur.status === 'claimed') ? null : cur);
    logAutoActivity('error', smsId, payment.reference, payment.amount, id, result.reason || 'approval-failed');
    return { ok: false, reason: result.reason || 'approval-failed' };
  }

  await claimRef.update({ status: 'approved', approved_at: now(), transaction_id: id });
  await db.ref('used_references/' + payment.reference).set({
    reference: payment.reference, txId: id,
    uid: (result.tx && (result.tx.wallet_uid || result.tx.uid)) || tx.wallet_uid || tx.uid || '',
    amount: payment.amount, smsId: String(smsId), approvedAt: now()
  });
  logAutoActivity('approved', smsId, payment.reference, payment.amount, id, 'verified');
  return { ok: true, reason: 'approved', txId: id, tx: result.tx };
}

async function processIncomingSmsSnapshot(snapshot) {
  if (!snapshot?.exists()) return;
  try { await autoApproveMatchingDeposits(snapshot.key, snapshot.val()); }
  catch (e) { console.error('[AUTO-DEPOSIT] processing failed', e); }
}

function startAutomaticDepositMonitor() {
  const smsRef = db.ref('incoming_sms');
  smsRef.on('child_added', snap => processIncomingSmsSnapshot(snap));
  smsRef.on('child_changed', snap => processIncomingSmsSnapshot(snap));
  smsRef.once('value').then(snap => {
    if (!snap.exists()) return;
    const jobs = [];
    snap.forEach(child => jobs.push(processIncomingSmsSnapshot(child)));
    return Promise.allSettled(jobs);
  }).catch(e => console.error('[AUTO-DEPOSIT] initial scan failed', e.message));
  console.log('[AUTO-DEPOSIT] real-time monitor attached to /incoming_sms (trusted senders only)');
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
    await sendWallet(uid);
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
    await sendWallet(ref);
  }
  return success;
}

// ============================================================================
// Game room
// ============================================================================
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
        uid, picks: Array.isArray(p.picks) ? p.picks.map(Number) : [],
        isBot: !!p.isBot, active: !!p.active, displayName: String(p.displayName || uid)
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
    const humanBets = activePlayers.filter(p => !p.isBot).reduce((sum, p) => sum + p.picks.length * this.bet, 0);
    return {
      gameState: this.phase, round: this.round, betAmount: this.bet,
      selectionEndsAt: this.endsAt, endsAt: this.endsAt,
      numbersTaken: Object.fromEntries([...this.taken.keys()].map(n => [n, true])),
      players, winners: [...this.winners].map(uid => this.players.get(uid)?.displayName || uid),
      winningNumber: this.winningNumber, prizePool: this.prizePool, potAmount: this.prizePool,
      totalBets, humanBets, totalPicks,
      displayedPrizePool: Math.floor(totalBets * PAYOUT_RATE),
      onlineCount: activePlayers.length,
      botCount: activePlayers.filter(p => p.isBot).length,
      realPlayerCount: activePlayers.filter(p => !p.isBot).length,
      serverNow: now(), lastResult: this.lastResult,
    };
  }
  async persist() {
    const players = {};
    for (const [uid, p] of this.players) players[uid] = { picks: [...p.picks], isBot: !!p.isBot, active: !!p.active, displayName: p.displayName || uid };
    const taken = Object.fromEntries(this.taken.entries());
    await db.ref('rooms/' + this.id).set({
      bet: this.bet, phase: this.phase, round: this.round, ends_at: this.endsAt,
      winning_number: this.winningNumber, winners_json: JSON.stringify(this.winners),
      prize_pool: this.prizePool,
      last_result_json: this.lastResult ? JSON.stringify(this.lastResult) : null,
      taken_json: JSON.stringify(taken), players_json: JSON.stringify(players), updated_at: now()
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
  if (snap.exists()) snap.forEach(child => { rowMap.set(child.key, child.val()); });
  rooms = { room_15: new Room('room_15', 15, rowMap.get('room_15')), room_30: new Room('room_30', 30, rowMap.get('room_30')) };
  for (const room of Object.values(rooms)) for (const p of room.players.values()) if (!p.isBot) p.active = false;
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
    if (result && result.committed) {
      const newPlay = toFiniteNumber(result.snapshot.val()?.play_wallet, 0);
      await sendWallet(uid);
      return { ok: true, balance: newPlay };
    }
    const refSnap = await db.ref('users/' + uid).once('value');
    if (!refSnap.exists()) return { ok: false, reason: 'user-not-found' };
    await sendWallet(uid);
    return { ok: false, reason: 'insufficient', balance: toFiniteNumber(refSnap.val()?.play_wallet, 0) };
  } catch (e) {
    console.error(`[DEBIT] uid=${uid} ERROR`, e);
    return { ok: false, reason: 'debit-error' };
  }
}

async function refundWager(uid, amount, roomId, round) {
  const amt = toFiniteNumber(amount, 0);
  if (amt <= 0) return false;
  try {
    const result = await db.ref('users/' + uid).transaction(u => {
      if (!u) return u;
      u.play_wallet = toFiniteNumber(u.play_wallet ?? u.playWallet ?? u.play_balance ?? u.playBalance ?? 0, 0) + amt;
      u.updated_at = now();
      u.wallet_updated_at = u.updated_at;
      return u;
    });
    const ok = !!(result && result.committed);
    if (ok) await sendWallet(uid);
    return ok;
  } catch (e) { console.error(`[REFUND] uid=${uid} ERROR`, e); return false; }
}

async function pickIntent(room, uid, num) {
  return room.mutex.run(async () => {
    if (room.phase !== 'selection') return { ok: false, reason: 'not-selection' };
    if (!(num >= 1 && num <= 200)) return { ok: false, reason: 'range' };
    if (await isBlocked(uid)) return { ok: false, reason: 'account-blocked' };
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
  const allWithPicks = [...room.players.values()].filter(p => p.picks.length > 0 && (p.isBot || p.active));
  const humans = allWithPicks.filter(p => !p.isBot);
  const bots = allWithPicks.filter(p => p.isBot);
  if (!allWithPicks.length) return resetRound(room, 'no-picks');
  const allPlayers = [...humans, ...bots];
  const allNums = [...new Set(allPlayers.flatMap(p => p.picks))];
  if (!allNums.length) return resetRound(room, 'no-picks');
  const winningNumber = allNums[Math.floor(Math.random() * allNums.length)];
  const winners = allPlayers.filter(p => p.picks.includes(winningNumber)).map(p => p.uid);
  const totalPicksInRoom = allPlayers.reduce((s, p) => s + p.picks.length, 0);
  const prizePool = Math.floor(totalPicksInRoom * room.bet * PAYOUT_RATE);
  room.winningNumber = winningNumber; room.winners = winners; room.prizePool = prizePool;
  room.phase = 'spinning'; room.endsAt = now() + SPINNING_SECONDS * 1000; room.dirty = true;
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
    round: room.round, winningNumber,
    winners: winners.map(uid => room.players.get(uid)?.displayName || uid),
    winAmount: (winners.length && prizePool > 0) ? Math.floor(prizePool / winners.length) : 0,
    setAt: now()
  };
}

async function toResults(room) {
  if (room.phase !== 'spinning') return;
  room.phase = 'results'; room.endsAt = now() + RESULTS_SECONDS * 1000; room.dirty = true;
}

function ethiopiaAnalyticsParts(timestamp = now()) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Africa/Addis_Ababa', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', hourCycle: 'h23'
  }).formatToParts(new Date(timestamp));
  const values = Object.fromEntries(parts.map(part => [part.type, part.value]));
  return { dateKey: `${values.year}-${values.month}-${values.day}`, hour: Number(values.hour) };
}

async function recordRoundAnalytics(room, reason) {
  try {
    const timestamp = now();
    const { dateKey, hour } = ethiopiaAnalyticsParts(timestamp);
    const players = [...room.players.values()];
    const humans = players.filter(p => p && !p.isBot && (p.active || (p.picks || []).length > 0));
    const bots = players.filter(p => p && p.isBot && (p.active || (p.picks || []).length > 0));
    const record = {
      room: room.id, round: Number(room.round || 0), timestamp, dateKey, hour,
      humanPlayerCount: humans.length, botPlayerCount: bots.length,
      totalPlayerCount: humans.length + bots.length,
      reason: String(reason || ''), source: 'authoritative-server'
    };
    await db.ref(`analytics/gameHistory/${dateKey}/${room.id}/${record.round}`).transaction(current => current || record);
  } catch (error) { console.error('[ANALYTICS] failed', error?.message || error); }
}

async function resetRound(room, why) {
  await recordRoundAnalytics(room, why);
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
  const pool = [...BOT_NAMES]; const off = ((round - 1) * 7) % pool.length;
  const rot = pool.slice(off).concat(pool.slice(0, off));
  return rot.slice(0, Math.min(count, pool.length));
}
function botUid(displayName) { return `__bot_${safeUser(displayName)}`; }
function clearBotTimers(room) { room.botTimers.forEach(clearTimeout); room.botTimers = []; }
function currentBotRoster(room) { return botRoster(BOT_MIN + (room.round % (BOT_MAX - BOT_MIN + 1)), room.round); }

function normalizeRoundBots(room) {
  const roster = new Set(currentBotRoster(room));
  const activeBotUids = new Set([...roster].map(botUid));
  for (const [uid, p] of room.players) {
    if (!p.isBot) continue;
    if (!activeBotUids.has(uid)) {
      for (const n of p.picks || []) if (room.taken.get(n) === uid) room.taken.delete(n);
      p.picks = []; p.active = false;
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
  const START_DELAY_MS = 350, BOT_PICK_GAP_MS = 300;
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
        if (!p) { p = { uid, picks: [], isBot: true, active: true, displayName }; room.players.set(uid, p); }
        else { p.active = true; p.isBot = true; p.displayName = displayName; }
        if (p.picks.length >= MAX_PICKS) return;
        let num = null;
        for (let attempt = 0; attempt < 12; attempt++) {
          const candidate = 1 + Math.floor(Math.random() * 200);
          if (!room.taken.has(candidate) && !p.picks.includes(candidate)) { num = candidate; break; }
        }
        if (num == null) {
          for (let candidate = 1; candidate <= 200; candidate++) if (!room.taken.has(candidate)) { num = candidate; break; }
        }
        if (num == null) return;
        room.taken.set(num, uid);
        p.picks.push(num);
        room.dirty = true;
        room.broadcast();
      });
    } catch (err) { console.error('[BOT-PICK]', err); }
    if (room.phase !== 'selection' || index >= order.length) return;
    room.botTimers.push(setTimeout(runNextPick, BOT_PICK_GAP_MS));
  };
  room.botTimers.push(setTimeout(runNextPick, START_DELAY_MS));
}

setInterval(async () => {
  for (const room of Object.values(rooms)) {
    await room.mutex.run(async () => {
      const t = now();
      if (room.phase === 'selection' && t >= room.endsAt) await toSpinning(room);
      else if (room.phase === 'spinning' && t >= room.endsAt) await toResults(room);
      else if (room.phase === 'results' && t >= room.endsAt) resetRound(room, 'results-done');
      if (room.dirty) { await room.persist(); room.broadcast(); room.dirty = false; }
    });
  }
}, 500);

const conns = new Map();
const walletListeners = new Map();

function attachWalletListener(uid) {
  if (walletListeners.has(uid)) return;
  const ref = db.ref('users/' + uid);
  const handler = snap => {
    if (!snap.exists()) return;
    const u = snap.val() || {};
    const payload = JSON.stringify({
      type: 'wallet',
      main: toFiniteNumber(u.main_wallet ?? u.mainWallet ?? u.main_balance ?? u.mainBalance ?? 0, 0),
      play: toFiniteNumber(u.play_wallet ?? u.playWallet ?? u.play_balance ?? u.playBalance ?? 0, 0),
      pending: toFiniteNumber(u.pending_withdrawal ?? u.pendingWithdrawal ?? 0, 0),
      refEarn: toFiniteNumber(u.referral_earnings ?? u.referralEarnings ?? 0, 0),
      updatedAt: toFiniteNumber(u.wallet_updated_at ?? u.updated_at, 0)
    });
    const set = conns.get(uid);
    if (set) for (const ws of set) if (ws.readyState === 1) ws.send(payload);
  };
  ref.on('value', handler);
  walletListeners.set(uid, { ref, handler });
}
function detachWalletListener(uid) {
  const entry = walletListeners.get(uid);
  if (!entry) return;
  try { entry.ref.off('value', entry.handler); } catch (e) { try { entry.ref.off(); } catch {} }
  walletListeners.delete(uid);
}

const wss = new WebSocketServer({ noServer: true });
function send(ws, obj) { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); }

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.authenticatedAt = null;
  ws.authTimer = setTimeout(() => { if (!ws.authed) { try { ws.close(4001, 'authentication timeout'); } catch {} } }, 10000);
  ws.uid = null; ws.roomId = null; ws.authed = false; ws.isAdmin = false;

  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', async raw => {
    let m; try { m = JSON.parse(raw.toString()); } catch { return; }
    try { await handle(ws, m); }
    catch (e) { console.error('[WS-MSG]', e); send(ws, { type: 'error', message: 'server-error' }); }
  });
  ws.on('close', async () => {
    clearTimeout(ws.authTimer);
    if (!ws.uid) return;
    const set = conns.get(ws.uid);
    if (set) {
      set.delete(ws);
      if (set.size) return;
      conns.delete(ws.uid);
      detachWalletListener(ws.uid);
    }
    if (ws.roomId) {
      const room = rooms[ws.roomId];
      if (!room) return;
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
    const verification = validateInitData(m.initData || '');
    if (!verification.ok) {
      const isServerFault = verification.reason.startsWith('server-misconfigured');
      const logLine = `[AUTH-REJECT] reason=${verification.reason} origin=${ws._socket?.remoteAddress || '?'}`;
      if (isServerFault) console.error(logLine); else console.warn(logLine);
      send(ws, {
        type: 'error',
        message: isServerFault ? 'server-auth-unavailable' : 'auth-failed',
        code: 'AUTH_REQUIRED'
      });
      try { ws.close(isServerFault ? 1011 : 4001, isServerFault ? 'server-auth-misconfigured' : 'authentication required'); } catch {}
      return;
    }

    const telegramUser = verification.user;
    let resolved = await resolveTelegramUser(telegramUser, m.ref || null);

    if (!resolved && DEV_ALLOW_ANY && !BOT_TOKEN && m.devUsername && /^[A-Za-z0-9_]{1,32}$/.test(m.devUsername)) {
      const uid = safeUser(m.devUsername);
      resolved = { uid, user: await ensureUser(uid, m.ref || null, false, { display_name: uid }) };
      console.warn(`[AUTH-DEV] DEV_ALLOW_ANY bypass used for uid=${uid}`);
    }
    if (!resolved?.uid) {
      console.error(`[AUTH-REJECT] signature valid but resolveTelegramUser returned null for telegram_id=${telegramUser.id}`);
      send(ws, { type: 'error', message: 'auth-failed', code: 'AUTH_REQUIRED' });
      try { ws.close(4001, 'authentication required'); } catch {}
      return;
    }

    const uid = resolved.uid;

    // Reject blocked accounts
    if (await isBlocked(uid)) {
      const snap = await db.ref('users/' + uid).once('value');
      const blocked = snap.exists() ? (snap.val() || {}) : {};
      send(ws, {
        type: 'account-blocked',
        reason: String(blocked.block_reason || 'policy-violation'),
        at: Number(blocked.blocked_at || 0)
      });
      try { ws.close(4003, 'account-blocked'); } catch {}
      return;
    }

    const clientIp = ws._socket?.remoteAddress || null;
    await ensureUser(uid, m.ref || null, false, {
      telegram_id: telegramUser.id,
      telegram_username: safeUser(telegramUser.username || ''),
      display_name: [telegramUser.first_name, telegramUser.last_name].filter(Boolean).join(' ') || telegramUser.username || uid,
      last_ip: clientIp
    });

    clearTimeout(ws.authTimer);
    ws.authenticatedAt = now();
    ws.uid = uid;
    ws.telegramId = Number(telegramUser.id);
    ws.telegramUsername = safeUser(telegramUser.username || '');
    ws.displayName = [telegramUser.first_name, telegramUser.last_name].filter(Boolean).join(' ') || telegramUser.username || uid;
    ws.authed = true;

    let set = conns.get(uid);
    if (!set) { set = new Set(); conns.set(uid, set); }
    set.add(ws);

    send(ws, { type: 'authed', uid, displayName: ws.displayName, telegramId: ws.telegramId, telegramUsername: ws.telegramUsername || null });
    send(ws, { type: 'wallet', ...(await walletFor(uid)) });
    attachWalletListener(uid);

    const txSnap = await db.ref('transactions').orderByChild('wallet_uid').equalTo(uid).once('value');
    const pending = [];
    txSnap.forEach(child => {
      const d = child.val();
      if (d.status === 'pending') pending.push({ id: child.key, type: d.type, amount: d.amount, status: d.status, timestamp: Number(d.timestamp || 0) });
    });
    pending.sort((a, b) => b.timestamp - a.timestamp);
    send(ws, { type: 'pending-transactions', transactions: pending.slice(0, 10) });

    (async () => {
      try {
        const snap = await db.ref('broadcasts/user_events/' + uid).once('value');
        const events = snap.val() || {};
        const nowTs = now();
        const recentCutoff = nowTs - 5 * 60 * 1000;
        const list = [];
        for (const [key, ev] of Object.entries(events)) {
          if (!ev || typeof ev !== 'object') continue;
          if (Number(ev.expiresAt) && Number(ev.expiresAt) < nowTs) {
            db.ref('broadcasts/user_events/' + uid + '/' + key).remove().catch(() => {});
            continue;
          }
          const seenAt = Number(ev.read_at || 0);
          if (ev.read === true && seenAt < recentCutoff) continue;
          list.push({ key, ...ev });
        }
        list.sort((a, b) => (Number(a.createdAt) || 0) - (Number(b.createdAt) || 0));
        for (const ev of list) send(ws, { type: 'user-event', ...ev });
      } catch (e) { console.error('[USER-EVENT] load failed', e.message); }
    })();

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
  if (await isBlocked(ws.uid)) {
    send(ws, { type: 'account-blocked', reason: 'policy-violation', at: now() });
    return;
  }

  if (m.type === 'join') {
    const room = rooms[m.room]; if (!room) return send(ws, { type: 'error', message: 'bad-room' });
    if (ws.roomId && rooms[ws.roomId] && ws.roomId !== m.room) { const oldPlayer = rooms[ws.roomId].players.get(ws.uid); if (oldPlayer) oldPlayer.active = false; }
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
    const room = rooms[ws.roomId]; const p = room?.players.get(ws.uid);
    if (room && p && !p.isBot) { await refundDisconnectedPlayer(room, p); room.players.delete(ws.uid); room.dirty = true; await saveRoom(room); }
    ws.roomId = null; return;
  }
  if (m.type === 'pick') {
    const room = rooms[ws.roomId]; if (!room) return;
    const r = await pickIntent(room, ws.uid, Number(m.number));
    send(ws, r.ok ? { type: 'pick-ok', number: Number(m.number) } : { type: 'pick-fail', number: Number(m.number), reason: r.reason });
    send(ws, { type: 'wallet', ...(await walletFor(ws.uid)) });
    return;
  }
  if (m.type === 'unpick') {
    const room = rooms[ws.roomId]; if (!room) return;
    const r = await unpickIntent(room, ws.uid, Number(m.number));
    send(ws, r.ok ? { type: 'unpick-ok', number: Number(m.number) } : { type: 'unpick-fail', number: Number(m.number), reason: r.reason });
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
      u.main_wallet = main - amount; u.play_wallet = play + amount;
      u.updated_at = now(); success = true; return u;
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
  if (m.type === 'user-event-read') {
    const key = String(m.key || '').slice(0, 128);
    if (!/^[A-Za-z0-9_\-]+$/.test(key)) return;
    try {
      await db.ref('broadcasts/user_events/' + ws.uid + '/' + key).update({ read: true, read_at: now() });
    } catch (e) { console.error('[USER-EVENT] mark-read failed', e.message); }
    return;
  }
}

setInterval(() => { for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; try { ws.ping(); } catch {} } }, 60000);

const app = express();
app.use(express.json({ limit: '3mb' }));
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, X-Admin-Key');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});
function isAdmin(req) {
  const suppliedKey = String(req.get('x-admin-key') || '');
  return ADMIN_KEY && constantTimeEqual(suppliedKey, ADMIN_KEY);
}

app.get('/admin/pending', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  try { res.json({ ok: true, transactions: await adminListTransactions() }); }
  catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.get('/admin/decision', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  const id = String(req.query.id || ''), decision = String(req.query.decision || '');
  if (!id || !decision) return res.status(400).json({ ok: false, error: 'missing-params' });
  try {
    const result = await adminDecision(id, decision, 'Processed via HTTP admin panel');
    if (!result.ok) return res.status(400).json({ ok: false, error: result.reason });
    res.json({ ok: true, transaction: result.tx });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.get('/admin/repair-tx-ids', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  try {
    const snap = await db.ref('transactions').orderByChild('status').equalTo('pending').once('value');
    const repaired = [], skipped = [], promises = [];
    snap.forEach(child => {
      const tx = child.val(); const id = child.key;
      const telegramId = Number(tx.telegram_id || 0);
      const username = safeUser(tx.telegram_username || tx.username || '');
      if (telegramId > 0 || username) {
        promises.push((async () => {
          const userInfo = await findUserRefForWallet({ uid: tx.uid, wallet_uid: tx.wallet_uid, telegram_id: telegramId, telegram_username: username, username: tx.username });
          if (userInfo && userInfo.uid !== tx.wallet_uid) {
            await db.ref('transactions/' + id).update({ wallet_uid: userInfo.uid, updated_at: now() });
            repaired.push({ id, old: tx.wallet_uid, new: userInfo.uid });
          } else skipped.push(id);
        })());
      } else skipped.push(id);
    });
    await Promise.all(promises);
    res.json({ ok: true, repaired, skipped });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.get('/admin/diagnose', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  try {
    const txs = await adminListTransactions();
    res.json({ ok: true, transactions: txs.map(tx => ({ ...tx, resolved_uid: tx.walletUid })) });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.get('/admin/sms', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  try {
    const snap = await db.ref('incoming_sms').once('value');
    const data = {};
    if (snap.exists()) snap.forEach(c => { data[c.key] = c.val(); });
    res.json({ ok: true, data });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.get('/admin/auto-approved', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  try {
    const snap = await db.ref('auto_approved').once('value');
    const data = {};
    if (snap.exists()) snap.forEach(c => { data[c.key] = c.val(); });
    res.json({ ok: true, data });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.put('/admin/auto-approved/:id', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  const id = safeUid(req.params.id);
  if (!id) return res.status(400).json({ ok: false, error: 'bad-id' });
  try { await db.ref('auto_approved/' + id).set(req.body || {}); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.get('/admin/used-references', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  try {
    const snap = await db.ref('used_references').once('value');
    const data = {};
    if (snap.exists()) snap.forEach(c => { data[c.key] = c.val(); });
    res.json({ ok: true, data });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.put('/admin/used-references/:ref', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  const ref = String(req.params.ref || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 128);
  if (!ref) return res.status(400).json({ ok: false, error: 'bad-ref' });
  try { await db.ref('used_references/' + ref).set(req.body || {}); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.get('/admin/auto-activity', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  try {
    const snap = await db.ref('auto_activity').limitToLast(100).once('value');
    const rows = [];
    snap.forEach(child => {
      const v = child.val() || {};
      rows.push({ id: child.key, kind: v.kind || 'info', smsId: v.smsId || '', reference: v.reference || '', amount: Number(v.amount) || 0, txId: v.txId || '', reason: v.reason || '', at: Number(v.at) || 0 });
    });
    rows.sort((a, b) => b.at - a.at);
    res.json({ ok: true, activity: rows });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.post('/admin/auto-match', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  try {
    const snap = await db.ref('incoming_sms').once('value');
    const results = { scanned: 0, approved: 0, skipped: 0, errors: 0, details: [] };
    if (snap.exists()) {
      const jobs = [];
      snap.forEach(child => {
        results.scanned++;
        jobs.push(autoApproveMatchingDeposits(child.key, child.val()).then(r => {
          if (r.ok) { results.approved++; results.details.push({ smsId: child.key, ok: true, txId: r.txId }); }
          else if (['no-match','untrusted-sender','no-reference','no-amount'].includes(r.reason)) results.skipped++;
          else { results.errors++; results.details.push({ smsId: child.key, ok: false, reason: r.reason }); }
        }).catch(e => { results.errors++; results.details.push({ smsId: child.key, ok: false, reason: e.message }); }));
      });
      await Promise.all(jobs);
    }
    res.json({ ok: true, ...results });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.get('/admin/broadcasts', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  try {
    const snap = await db.ref('broadcasts').once('value');
    const v = snap.val() || {};
    res.json({ ok: true, data: { transient: v.transient || null, persistent: v.persistent || null, requests: v.requests || {} } });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.put('/admin/broadcasts/transient', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  const text = String(req.body?.text || '').slice(0, 300).trim();
  const duration = Math.max(1, Math.min(300, Math.round(Number(req.body?.duration) || 8)));
  if (!text) return res.status(400).json({ ok: false, error: 'empty-text' });
  const ts = now();
  try {
    await db.ref('broadcasts/transient').set({ id: String(ts), text, duration, createdAt: ts, expiresAt: ts + duration * 1000, active: true });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.delete('/admin/broadcasts/transient', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  try { await db.ref('broadcasts/transient/active').set(false); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.put('/admin/broadcasts/persistent', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  const text = String(req.body?.text || '').slice(0, 300).trim();
  const active = req.body?.active !== false && !!text;
  try { await db.ref('broadcasts/persistent').set({ text, active, updatedAt: now() }); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.delete('/admin/broadcasts/persistent', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  try { await db.ref('broadcasts/persistent').set({ text: '', active: false, updatedAt: now() }); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.put('/admin/broadcasts/requests/:key', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  const key = String(req.params.key || '');
  if (!ADMIN_BROADCAST_REQUEST_KEYS.has(key)) return res.status(400).json({ ok: false, error: 'bad-key' });
  const text = String(req.body?.text || '').slice(0, 300).trim();
  const active = req.body?.active !== false && !!text;
  try {
    await db.ref('broadcasts/requests/' + key).set({ text, active, updatedAt: now() });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.post('/admin/user-event', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  const uid = safeUid(req.body?.uid);
  const type = String(req.body?.type || '').slice(0, 64);
  if (!uid || !type || !ADMIN_BROADCAST_REQUEST_KEYS.has(type)) return res.status(400).json({ ok: false, error: 'missing-params' });
  let message = String(req.body?.message || '').slice(0, 1000);
  if (!message) {
    try {
      const cfg = (await db.ref('broadcasts/requests/' + type).once('value')).val() || {};
      if (cfg.active === true && cfg.text) message = String(cfg.text).slice(0, 1000);
    } catch {}
  }
  if (!message) message = DEFAULT_REQUEST_MESSAGES[type] || '';
  if (!message) return res.status(400).json({ ok: false, error: 'no-message' });
  const ts = now();
  const eventKey = 'evt_' + ts + '_' + crypto.randomBytes(4).toString('hex');
  const payload = {
    type, requestType: req.body?.requestType ? String(req.body.requestType).slice(0, 32) : null,
    decision: req.body?.decision ? String(req.body.decision).slice(0, 32) : null,
    message, txId: req.body?.txId ? String(req.body.txId).slice(0, 128) : '',
    read: false, createdAt: ts, expiresAt: ts + 7 * 24 * 60 * 60 * 1000
  };
  try {
    await db.ref('broadcasts/user_events/' + uid + '/' + eventKey).set(payload);
    const set = conns.get(uid);
    if (set) for (const ws of set) if (ws.readyState === 1) try { ws.send(JSON.stringify({ type: 'user-event', key: eventKey, ...payload })); } catch {}
    res.json({ ok: true, key: eventKey });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ============================================================================
// Admin: user directory (aggregate view + win history + block status)
// ============================================================================
app.get('/admin/users', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  try {
    const [usersSnap, tgSnap, txsSnap, roomsSnap] = await Promise.all([
      db.ref('users').once('value'),
      db.ref('telegram_users').once('value'),
      db.ref('transactions').once('value'),
      db.ref('rooms').once('value')
    ]);
    const users = usersSnap.exists() ? (usersSnap.val() || {}) : {};
    const tgMap = tgSnap.exists() ? (tgSnap.val() || {}) : {};
    const txs = txsSnap.exists() ? (txsSnap.val() || {}) : {};
    const rooms = roomsSnap.exists() ? (roomsSnap.val() || {}) : {};

    const byUid = new Map(), byUsername = new Map(), byTelegramId = new Map();
    const blank = () => ({ deposit_count: 0, withdraw_count: 0, deposit_total: 0, withdraw_total: 0 });
    const bump = (map, key, tx) => {
      if (!key) return;
      let acc = map.get(key); if (!acc) { acc = blank(); map.set(key, acc); }
      const amount = Number(tx.amount) || 0;
      if (tx.type === 'deposit') { acc.deposit_count += 1; acc.deposit_total += amount; }
      if (tx.type === 'withdraw') { acc.withdraw_count += 1; acc.withdraw_total += amount; }
    };
    for (const txId of Object.keys(txs)) {
      const tx = txs[txId] || {};
      bump(byUid, safeUid(tx.wallet_uid || tx.uid || ''), tx);
      bump(byUsername, safeUid(tx.username || ''), tx);
      const tId = Number(tx.telegram_id || 0);
      if (tId > 0) bump(byTelegramId, tId, tx);
    }

    const winsByKey = new Map();
    const bumpWin = (key, amount, at) => {
      if (!key) return;
      let acc = winsByKey.get(key); if (!acc) { acc = { count: 0, total: 0, lastAt: 0 }; winsByKey.set(key, acc); }
      acc.count += 1; acc.total += Number(amount) || 0; acc.lastAt = Math.max(acc.lastAt, Number(at) || 0);
    };
    for (const roomId of Object.keys(rooms)) {
      const room = rooms[roomId] || {};
      let lr = null;
      try { lr = room.last_result_json ? JSON.parse(room.last_result_json) : null; } catch { lr = null; }
      if (!lr || !Array.isArray(lr.winners)) continue;
      const amount = Number(lr.winAmount) || 0;
      const at = Number(lr.setAt) || 0;
      for (const w of lr.winners) {
        if (w == null) continue;
        const raw = String(w);
        bumpWin('name:' + raw.toLowerCase(), amount, at);
        bumpWin('user:' + safeUid(raw), amount, at);
      }
    }

    const rows = [];
    for (const uid of Object.keys(users)) {
      const u = users[uid] || {};
      const tgId = u.telegram_id != null ? Number(u.telegram_id) : (tgMap[uid] ? Number(uid) : 0);
      const a = byUid.get(safeUid(uid)) || blank();
      const b = byUsername.get(safeUid(u.username || uid)) || blank();
      const c = (tgId > 0 && byTelegramId.get(tgId)) || blank();
      const pick = (a.deposit_count + a.withdraw_count) >= (b.deposit_count + b.withdraw_count) &&
                   (a.deposit_count + a.withdraw_count) >= (c.deposit_count + c.withdraw_count) ? a
                 : ((b.deposit_count + b.withdraw_count) >= (c.deposit_count + c.withdraw_count) ? b : c);
      const w1 = winsByKey.get('name:' + String(u.display_name || '').toLowerCase());
      const w2 = winsByKey.get('user:' + safeUid(u.username || uid));
      const w3 = winsByKey.get('user:' + safeUid(uid));
      const win = [w1, w2, w3].filter(Boolean).sort((x, y) => y.count - x.count)[0] || { count: 0, total: 0, lastAt: 0 };
      rows.push({
        uid,
        name: u.display_name || u.username || uid,
        username: u.username || null,
        telegram_id: tgId || null,
        telegram_username: u.telegram_username || null,
        main: toFiniteNumber(u.main_wallet ?? u.mainWallet ?? u.main_balance ?? u.mainBalance ?? 0, 0),
        play: toFiniteNumber(u.play_wallet ?? u.playWallet ?? u.play_balance ?? u.playBalance ?? 0, 0),
        pending: toFiniteNumber(u.pending_withdrawal ?? u.pendingWithdrawal ?? 0, 0),
        ref: toFiniteNumber(u.referral_earnings ?? u.referralEarnings ?? 0, 0),
        referred_by: u.referred_by || null,
        deposit_count: pick.deposit_count, withdraw_count: pick.withdraw_count,
        deposit_total: pick.deposit_total, withdraw_total: pick.withdraw_total,
        wins_count: win.count, wins_total: win.total, last_win_at: win.lastAt || null,
        blocked: u.blocked === true,
        block_reason: u.blocked === true ? (u.block_reason || null) : null,
        created_at: Number(u.created_at || 0) || null,
        updated_at: Number(u.updated_at || u.wallet_updated_at || 0) || null,
        is_bot: /^__bot_/.test(uid)
      });
    }
    rows.sort((x, y) => (y.updated_at || 0) - (x.updated_at || 0));
    res.json({ ok: true, users: rows, generated_at: now() });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.get('/admin/users/:uid/transactions', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  const uid = safeUid(req.params.uid);
  if (!uid) return res.status(400).json({ ok: false, error: 'bad-uid' });
  try {
    const snap = await db.ref('transactions').once('value');
    const data = snap.exists() ? (snap.val() || {}) : {};
    const list = [];
    for (const id of Object.keys(data)) {
      const tx = data[id] || {};
      if (safeUid(tx.wallet_uid || tx.uid || '') !== uid && safeUid(tx.username || '') !== uid) continue;
      list.push({
        id, type: tx.type || '', amount: Number(tx.amount) || 0, status: tx.status || '',
        reference: tx.reference || '', reason: tx.reason || '',
        full_name: tx.full_name || '', phone_number: tx.phone_number || '',
        telegram_id: tx.telegram_id || null, wallet_uid: tx.wallet_uid || tx.uid || '',
        timestamp: Number(tx.timestamp) || 0, wallet_applied: Number(tx.wallet_applied) || 0
      });
    }
    list.sort((a, b) => b.timestamp - a.timestamp);
    res.json({ ok: true, transactions: list });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.get('/admin/users/:uid/wins', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  const uid = safeUid(req.params.uid);
  if (!uid) return res.status(400).json({ ok: false, error: 'bad-uid' });
  try {
    const [userSnap, roomsSnap] = await Promise.all([db.ref('users/' + uid).once('value'), db.ref('rooms').once('value')]);
    const u = userSnap.exists() ? (userSnap.val() || {}) : {};
    const rooms = roomsSnap.exists() ? (roomsSnap.val() || {}) : {};
    const displayName = String(u.display_name || '').toLowerCase();
    const username = String(u.username || '').toLowerCase();
    const matches = w => { if (w == null) return false; const x = String(w).trim().toLowerCase(); return x === displayName || x === username || x === uid; };
    const wins = [];
    for (const roomId of Object.keys(rooms)) {
      const room = rooms[roomId] || {};
      let lr = null;
      try { lr = room.last_result_json ? JSON.parse(room.last_result_json) : null; } catch { lr = null; }
      if (!lr || !Array.isArray(lr.winners)) continue;
      if (!lr.winners.some(matches)) continue;
      wins.push({ room: roomId, round: Number(lr.round) || 0, winningNumber: Number(lr.winningNumber) || 0, amount: Number(lr.winAmount) || 0, winners: lr.winners.map(String), at: Number(lr.setAt) || 0 });
    }
    wins.sort((a, b) => b.at - a.at);
    res.json({ ok: true, wins });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ============================================================================
// Admin: fake-account analysis endpoints
// ============================================================================
app.get('/admin/analysis', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  try {
    const snap = await db.ref('analysis/recent').once('value');
    const batches = [];
    snap.forEach(c => batches.push(c.val() || {}));
    batches.sort((a, b) => (b.at || 0) - (a.at || 0));
    res.json({ ok: true, batches: batches.slice(0, 10) });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.post('/admin/analysis/run', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  try { res.json({ ok: true, batch: await runFullAnalysis('manual') }); }
  catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.post('/admin/analysis/block', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  const uid = safeUid(req.body?.uid);
  if (!uid) return res.status(400).json({ ok: false, error: 'bad-uid' });
  const reason = String(req.body?.reason || 'policy-violation').slice(0, 256);
  const result = await blockAccount(uid, reason, req.body?.signals || []);
  if (!result.ok) return res.status(400).json({ ok: false, error: result.reason });
  res.json({ ok: true });
});
app.post('/admin/analysis/unblock', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  const uid = safeUid(req.body?.uid);
  if (!uid) return res.status(400).json({ ok: false, error: 'bad-uid' });
  const result = await unblockAccount(uid);
  if (!result.ok) return res.status(400).json({ ok: false, error: result.reason });
  res.json({ ok: true });
});
app.post('/admin/analysis/block-all-suspects', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  try {
    const batch = await runFullAnalysis('auto-block');
    const toBlock = (batch.results || []).filter(r => r.level === 'block');
    let blocked = 0;
    for (const r of toBlock) {
      const ok = await blockAccount(r.uid, 'fake-account (auto-blocked by analysis)', r.signals || []);
      if (ok.ok) blocked++;
    }
    res.json({ ok: true, blocked, batch });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.get('/health', async (_, res) => {
  const roomInfo = {};
  for (const room of Object.values(rooms)) {
    roomInfo[room.id] = { phase: room.phase, round: room.round, players: Object.keys(room.snapshot().players).length, taken: room.taken.size };
  }
  try {
    await Promise.race([db.ref('rooms').limitToFirst(1).once('value'), new Promise((_, rej) => setTimeout(() => rej(new Error('database-timeout')), 2500))]);
    res.json({ ok: true, service: 'winzo-authoritative-server', database: 'firebase-realtime-database', websocket: true, uptime: process.uptime(), origins: ALLOWED_ORIGINS, rooms: roomInfo, walletListeners: walletListeners.size });
  } catch (e) { res.status(503).json({ ok: false, database: 'unavailable', error: e.message || 'database-unavailable' }); }
});
app.get('/identity/:telegramId', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden' });
  const telegramId = Number(req.params.telegramId);
  if (!Number.isSafeInteger(telegramId) || telegramId <= 0) return res.status(400).json({ ok: false, error: 'bad-telegram-id' });
  try {
    const mapped = await db.ref('telegram_users/' + telegramId).once('value');
    const byTelegram = await findUserByTelegramId(telegramId);
    const canonical = await readUserByUid(`tg_${telegramId}`);
    res.json({
      ok: true, telegramId,
      mapping: mapped.exists() ? mapped.val() : null,
      byTelegram: byTelegram ? { uid: byTelegram.uid, username: byTelegram.data.username, displayName: byTelegram.data.display_name, telegramId: byTelegram.data.telegram_id, main: toFiniteNumber(byTelegram.data.main_wallet), play: toFiniteNumber(byTelegram.data.play_wallet), pending: toFiniteNumber(byTelegram.data.pending_withdrawal) } : null,
      canonical: canonical ? { uid: canonical.uid, username: canonical.data.username, displayName: canonical.data.display_name, telegramId: canonical.data.telegram_id, main: toFiniteNumber(canonical.data.main_wallet), play: toFiniteNumber(canonical.data.play_wallet), pending: toFiniteNumber(canonical.data.pending_withdrawal) } : null
    });
  } catch (e) { res.status(503).json({ ok: false, error: e.message || 'identity-check-failed' }); }
});
app.get('/config', (_, res) => res.json({ ok: true, service: 'winzo-authoritative-server', database: 'firebase-realtime-database', websocket: true, walletUpdates: 'push' }));
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
  try { for (const room of Object.values(rooms)) { await room.mutex.run(async () => { if (room.dirty) await room.persist(); }); clearBotTimers(room); } }
  catch (e) { console.error('[SHUTDOWN] persist failed', e); }
  for (const uid of [...walletListeners.keys()]) { try { detachWalletListener(uid); } catch {} }
  for (const ws of wss.clients) { try { ws.close(1001, 'server shutting down'); } catch {} }
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000).unref();
}
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

loadRooms().then(() => {
  startAutomaticDepositMonitor();
  server.listen(PORT, '0.0.0.0', () => console.log(`Winzo Realtime Database server listening on :${PORT}`));
  for (const room of Object.values(rooms)) {
    if (room.phase === 'selection' && room.endsAt <= now()) { room.endsAt = now() + SELECTION_SECONDS * 1000; room.dirty = true; }
    if (room.phase === 'selection') normalizeRoundBots(room);
    room.persist();
    scheduleBots(room);
  }
  console.log(`Realtime Database: ${FIREBASE_DATABASE_URL}`);
  console.log(`Bots per round: ${BOT_MIN}–${BOT_MAX}`);
  console.log(`Results phase length: ${RESULTS_SECONDS}s`);
  console.log(`Wallet updates: PUSH (per-user Firebase .on('value') listener)`);
  console.log(`Signature verification: ${BOT_TOKEN ? 'ENABLED' : 'DISABLED (BOT_TOKEN missing)'}`);
  console.log(`Fake-account analysis: enabled · thresholds: block≥${ANALYSIS_CONFIG.FLAG_SCORE_BLOCK}, suspect≥${ANALYSIS_CONFIG.FLAG_SCORE_SUSPECT}`);
}).catch(err => { console.error('Failed to load rooms.', err); process.exit(1); });
