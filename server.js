import express from 'express';
import { WebSocketServer } from 'ws';
import crypto from 'crypto';
import http from 'http';
import pg from 'pg';
import fs from 'fs';
import admin from 'firebase-admin';

const { Pool } = pg;

// ============================================================================
// ⚠️ FIREBASE MIGRATION CONFIG ⚠️
// ============================================================================
const FIREBASE_DB_URL = "https://render-e9e2d-default-rtdb.firebaseio.com"; 
// ============================================================================

// --- Database Initialization ---
const pool = new Pool({
  host: process.env.DB_HOST || 'winzo-sql-rw.db-956.svc.cluster.local',
  port: Number(process.env.DB_PORT || 5432),
  database: process.env.DB_NAME || 'winzo_sql',
  user: process.env.DB_USER || 'admin_winzo_sql',
  password: process.env.DB_PASSWORD || 'avR9@t+ItYe-Mqa3Ulmb2Wk7U:D:Qsla',
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

// --- Configuration ---
const PORT = Number(process.env.PORT || 10000);
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const ADMIN_KEY = process.env.ADMIN_KEY || '';
const NODE_ENV = process.env.NODE_ENV || 'production';
const ALLOWED_ORIGINS = String(process.env.ALLOWED_ORIGINS || '*')
  .split(',').map(v => v.trim()).filter(Boolean);

if (!ADMIN_KEY) console.warn('WARNING: ADMIN_KEY is not set. Admin approval will be disabled.');
if (!BOT_TOKEN) console.warn('WARNING: BOT_TOKEN is not set. All WebSocket authentication will fail.');

const SELECTION_SECONDS = 30;
const SPINNING_SECONDS = 3;
const RESULTS_SECONDS = 13;
const MAX_PICKS = 3;
const PAYOUT_RATE = 0.80;
const REFERRAL_RATE = 0.05;
const BOT_START_WALLET = 1_000_000;
const BOT_MIN = 3;
const BOT_MAX = 6;

const SMS_TRUSTED_SENDERS = ['127', 'telebirr', 'ethio telecom', 'ethiotelecom', 'ethio-telecom'];
const DAY = 86400000;

// ============================================================================
// Database Schema Bootstrap
// ============================================================================
async function initDb() {
  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS users (
          username VARCHAR(64) PRIMARY KEY, display_name VARCHAR(128), telegram_id BIGINT, telegram_username VARCHAR(64),
          main_wallet NUMERIC DEFAULT 0, play_wallet NUMERIC DEFAULT 0, signup_bonus_remaining NUMERIC DEFAULT 0, signup_bonus_original NUMERIC DEFAULT 0,
          withdrawal_hold_until BIGINT DEFAULT 0, withdrawal_hold_reason VARCHAR(64), pending_withdrawal NUMERIC DEFAULT 0, referral_earnings NUMERIC DEFAULT 0,
          wallet_updated_at BIGINT, referred_by VARCHAR(64), last_ip VARCHAR(64), last_ip_hash VARCHAR(128), blocked BOOLEAN DEFAULT FALSE,
          blocked_at BIGINT, block_reason VARCHAR(256), block_signals TEXT, device_fingerprint VARCHAR(128), fraud_status VARCHAR(64),
          account_status VARCHAR(64), created_at BIGINT, updated_at BIGINT, wallet_ledger JSONB DEFAULT '{}'::jsonb
      );
      CREATE TABLE IF NOT EXISTS transactions (
          id VARCHAR(128) PRIMARY KEY, username VARCHAR(64), uid VARCHAR(64), wallet_uid VARCHAR(64), telegram_id BIGINT, telegram_username VARCHAR(64),
          display_name VARCHAR(128), type VARCHAR(32), amount NUMERIC, status VARCHAR(32), timestamp BIGINT, reference VARCHAR(128),
          full_name VARCHAR(128), phone_number VARCHAR(64), image_data TEXT, reason VARCHAR(256), wallet_applied INT DEFAULT 0,
          client_request_id VARCHAR(128), updated_at BIGINT
      );
      CREATE TABLE IF NOT EXISTS rooms (
          id VARCHAR(64) PRIMARY KEY, bet NUMERIC, phase VARCHAR(32), round INT, ends_at BIGINT, winning_number INT,
          winners_json TEXT, prize_pool NUMERIC, last_result_json TEXT, taken_json TEXT, players_json TEXT, updated_at BIGINT
      );
      CREATE TABLE IF NOT EXISTS telegram_users (
          telegram_id BIGINT PRIMARY KEY, uid VARCHAR(64), username VARCHAR(64), telegram_username VARCHAR(64), updated_at BIGINT
      );
      CREATE TABLE IF NOT EXISTS notifications (
          id VARCHAR(128) PRIMARY KEY, username VARCHAR(64), type VARCHAR(32), message TEXT, status VARCHAR(32),
          tx_type VARCHAR(32), tx_id VARCHAR(128), timestamp BIGINT, read BOOLEAN DEFAULT FALSE
      );
      CREATE TABLE IF NOT EXISTS incoming_sms (
          id VARCHAR(128) PRIMARY KEY, sender VARCHAR(64), text TEXT, timestamp BIGINT, processed BOOLEAN DEFAULT FALSE
      );
      CREATE TABLE IF NOT EXISTS auto_activity (
          id VARCHAR(128) PRIMARY KEY, kind VARCHAR(32), sms_id VARCHAR(128), reference VARCHAR(128), amount NUMERIC,
          tx_id VARCHAR(128), reason VARCHAR(256), at BIGINT
      );
      CREATE TABLE IF NOT EXISTS transaction_requests (
          uid VARCHAR(64), client_request_id VARCHAR(128), tx_id VARCHAR(128), type VARCHAR(32), amount NUMERIC, created_at BIGINT,
          PRIMARY KEY (uid, client_request_id)
      );
    `);
    console.log('✅ Database Schema Verified.');
  } catch (e) {
    console.error('❌ Schema init failed:', e);
  } finally {
    client.release();
  }
}

// ============================================================================
// Utility helpers
// ============================================================================
function toFiniteNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}
function now() { return Date.now(); }
function safeUser(username) { return String(username || '').replace(/[^A-Za-z0-9_]/g, '').slice(0, 32); }
function safeUid(value) { return String(value || '').replace(/[^A-Za-z0-9_:-]/g, '').slice(0, 64); }
function constantTimeEqual(a, b) {
  const aa = Buffer.from(String(a || ''));
  const bb = Buffer.from(String(b || ''));
  return aa.length === bb.length && crypto.timingSafeEqual(aa, bb);
}
function hashText(v) { return crypto.createHash('sha256').update(String(v)).digest('hex'); }
function normalizedClientIp(ip) {
  let value = String(ip || '').trim();
  if (value.startsWith('::ffff:')) value = value.slice(7);
  if (value === '::1') return '127.0.0.1';
  return value;
}
function hashWithRotatingSalt(value, dayOffset = 0) {
  const d = new Date(Date.now() - dayOffset * DAY).toISOString().slice(0, 10);
  const salt = `${process.env.IP_HASH_SALT || 'winzo-ip-salt'}|${d}`;
  return hashText(`${salt}|${String(value || '')}`);
}
function websocketClientIp(ws) {
  if (process.env.TRUST_PROXY_HEADERS === 'true') {
    const forwarded = String(ws?._req?.headers?.['x-forwarded-for'] || '').split(',')[0].trim();
    if (forwarded) return normalizedClientIp(forwarded);
  }
  return normalizedClientIp(ws?._socket?.remoteAddress || '');
}

class Mutex {
  constructor() { this._t = Promise.resolve(); }
  run(fn) { const n = this._t.then(fn, fn); this._t = n.then(() => {}, () => {}); return n; }
}

// ============================================================================
// Telegram initData cryptographic verification
// ============================================================================
function validateInitData(initData, maxAge = 300) {
  if (!BOT_TOKEN) return { ok: false, reason: 'server-misconfigured' };
  if (!initData || typeof initData !== 'string') return { ok: false, reason: 'missing-initData' };

  let params;
  try { params = new URLSearchParams(initData); }
  catch { return { ok: false, reason: 'malformed-query-string' }; }

  const receivedHash = params.get('hash');
  if (!receivedHash || !/^[a-f0-9]{64}$/i.test(receivedHash)) return { ok: false, reason: 'missing-hash' };

  params.delete('hash');
  const entries = [...params.entries()].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  const dataCheckString = entries.map(([k, v]) => `${k}=${v}`).join('\n');

  const secretKey = crypto.createHmac('sha256', 'WebAppData').update(BOT_TOKEN).digest();
  const computed = crypto.createHmac('sha256', secretKey).update(dataCheckString).digest('hex');

  const a = Buffer.from(computed, 'hex');
  const b = Buffer.from(receivedHash, 'hex');
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return { ok: false, reason: 'invalid-signature' };

  const authDate = Number(params.get('auth_date') || 0);
  if (!authDate || !Number.isFinite(authDate)) return { ok: false, reason: 'missing-auth_date' };
  const ageSeconds = Math.floor(Date.now() / 1000) - authDate;
  if (ageSeconds > maxAge) return { ok: false, reason: `expired` };
  if (ageSeconds < -300) return { ok: false, reason: `future-date` };

  let user = null;
  try {
    const rawUser = params.get('user');
    user = rawUser ? JSON.parse(rawUser) : null;
  } catch { return { ok: false, reason: 'user-not-json' }; }
  
  if (!user || typeof user.id !== 'number' || !Number.isSafeInteger(user.id) || user.id <= 0) return { ok: false, reason: 'invalid-user' };

  return { ok: true, user, authDate, params };
}

// ============================================================================
// User management
// ============================================================================
async function ensureUser(username, referredBy = null, bot = false, extra = {}) {
  const clean = safeUid(username);
  if (!clean) return null;
  
  const client = await pool.connect();
  try {
    const res = await client.query('SELECT * FROM users WHERE username = $1', [clean]);
    if (res.rows.length > 0) {
      const current = res.rows[0];
      const patch = {};
      if (extra.telegram_id != null && current.telegram_id !== Number(extra.telegram_id)) patch.telegram_id = Number(extra.telegram_id);
      if (extra.display_name && current.display_name !== String(extra.display_name)) patch.display_name = String(extra.display_name).slice(0, 128);
      if (extra.last_ip && current.last_ip_hash !== hashWithRotatingSalt(normalizedClientIp(extra.last_ip))) {
        patch.last_ip_hash = hashWithRotatingSalt(normalizedClientIp(extra.last_ip));
      }

      if (Object.keys(patch).length) {
        const setClauses = Object.keys(patch).map((key, i) => `${key} = $${i + 2}`).join(', ');
        const values = [clean, ...Object.values(patch)];
        await client.query(`UPDATE users SET ${setClauses}, updated_at = $${values.length + 1} WHERE username = $1`, [...values, now()]);
      }
      return { ...current, ...patch };
    }
    
    const t = now();
    const newUser = {
      username: clean, display_name: extra.display_name || clean, telegram_id: extra.telegram_id || null,
      telegram_username: extra.telegram_username || null, main_wallet: 0, play_wallet: bot ? BOT_START_WALLET : 30,
      signup_bonus_remaining: bot ? 0 : 30, signup_bonus_original: bot ? 0 : 30,
      withdrawal_hold_until: bot ? 0 : t + DAY, withdrawal_hold_reason: bot ? null : 'new-account-24h-hold',
      pending_withdrawal: 0, referral_earnings: 0, wallet_updated_at: t, referred_by: referredBy && referredBy !== clean ? safeUser(referredBy) : null,
      last_ip_hash: extra.last_ip ? hashWithRotatingSalt(normalizedClientIp(extra.last_ip)) : (extra.last_ip_hash || null),
      created_at: t, updated_at: t
    };
    
    await client.query(`
      INSERT INTO users (username, display_name, telegram_id, telegram_username, main_wallet, play_wallet, signup_bonus_remaining, signup_bonus_original, withdrawal_hold_until, withdrawal_hold_reason, pending_withdrawal, referral_earnings, wallet_updated_at, referred_by, last_ip_hash, created_at, updated_at) 
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
    `, Object.values(newUser));
    
    return newUser;
  } finally {
    client.release();
  }
}

async function resolveTelegramUser(user, referredBy = null) {
  if (!user?.id) return null;
  const telegramId = Number(user.id);
  const telegramUsername = safeUser(user.username || '');
  const displayName = [user.first_name, user.last_name].filter(Boolean).join(' ').trim() || telegramUsername || `tg_${telegramId}`;
  const canonicalUid = `tg_${telegramId}`;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const mapRes = await client.query('SELECT * FROM telegram_users WHERE telegram_id = $1 FOR UPDATE', [telegramId]);
    if (mapRes.rows.length > 0 && mapRes.rows[0].uid) {
      const mappedUid = safeUid(mapRes.rows[0].uid);
      const mappedUser = await client.query('SELECT * FROM users WHERE username = $1', [mappedUid]);
      if (mappedUser.rows.length > 0 && (!mappedUser.rows[0].telegram_id || Number(mappedUser.rows[0].telegram_id) === telegramId)) {
        await client.query('UPDATE telegram_users SET username = $1, telegram_username = $2, updated_at = $3 WHERE telegram_id = $4', [mappedUser.rows[0].username, telegramUsername, now(), telegramId]);
        await client.query('COMMIT');
        return { uid: mappedUid, user: (await ensureUser(mappedUid, referredBy, false, { telegram_id: telegramId, display_name: displayName, telegram_username: telegramUsername || null })) };
      }
    }

    let found = await client.query('SELECT * FROM users WHERE username = $1 OR telegram_id = $2 LIMIT 1', [canonicalUid, telegramId]);
    let finalUid = canonicalUid;

    if (found.rows.length > 0) {
      const row = found.rows[0];
      if (row.telegram_id && Number(row.telegram_id) !== telegramId) throw new Error('conflicting-user');
      if (row.username !== canonicalUid) {
        await client.query('UPDATE users SET username = $1, telegram_id = $2, updated_at = $3 WHERE username = $4', [canonicalUid, telegramId, now(), row.username]);
        finalUid = canonicalUid;
      }
    }
    
    await client.query('COMMIT');
    const created = await ensureUser(finalUid, referredBy, false, { telegram_id: telegramId, display_name: displayName, telegram_username: telegramUsername || null });
    await pool.query('INSERT INTO telegram_users (telegram_id, uid, username, telegram_username, updated_at) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (telegram_id) DO UPDATE SET uid=EXCLUDED.uid, username=EXCLUDED.username, updated_at=EXCLUDED.updated_at', 
      [telegramId, finalUid, created.username, telegramUsername, now()]);
    return { uid: finalUid, user: created, telegramId, telegramUsername };
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('[IDENTITY] error', e.message);
    return null;
  } finally {
    client.release();
  }
}

async function walletFor(username) {
  const clean = safeUid(username);
  if (!clean) return { main: 0, play: 0, pending: 0, refEarn: 0, updatedAt: 0 };
  const res = await pool.query('SELECT main_wallet, play_wallet, pending_withdrawal, referral_earnings, wallet_updated_at FROM users WHERE username = $1', [clean]);
  if (res.rows.length === 0) return { main: 0, play: 0, pending: 0, refEarn: 0, updatedAt: 0 };
  const u = res.rows[0];
  return { main: Number(u.main_wallet), play: Number(u.play_wallet), pending: Number(u.pending_withdrawal), refEarn: Number(u.referral_earnings), updatedAt: Number(u.wallet_updated_at) };
}

async function sendWallet(uid) {
  const payload = { type: 'wallet', ...(await walletFor(uid)) };
  const set = conns.get(uid);
  if (set) for (const ws of set) if (ws.readyState === 1) ws.send(JSON.stringify(payload));
}

async function addNotification(username, data) {
  const id = data.id || crypto.randomUUID();
  const payload = { type: 'notification', id, ...data, timestamp: Number(data.timestamp || now()) };
  await pool.query('INSERT INTO notifications (id, username, type, message, status, tx_type, tx_id, timestamp, read) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,false)', 
    [id, username, data.type || 'notification', String(data.message || ''), data.status || null, data.txType || null, data.txId || null, payload.timestamp]);
  const set = conns.get(username);
  if (set) for (const ws of set) if (ws.readyState === 1) ws.send(JSON.stringify(payload));
}

async function notifyTransaction(username, tx) {
  await addNotification(username, {
    type: 'transaction_result', status: tx.status, txType: tx.type, txId: tx.id, reason: tx.reason || '',
    message: tx.status === 'approved' ? `${tx.type === 'withdraw' ? 'Withdrawal' : 'Deposit'} approved.` : 'Your request was rejected by admin.'
  });
  const set = conns.get(username);
  if (set) for (const ws of set) if (ws.readyState === 1) ws.send(JSON.stringify({ type: 'transaction-status', id: tx.id, status: tx.status, txType: tx.type, reason: tx.reason || '' }));
}

// ============================================================================
// Transactions
// ============================================================================
async function transactionRequest(uid, data) {
  const type = data.txType;
  const amount = Number(data.amount);
  if (!['deposit', 'withdraw'].includes(type)) return { ok: false, reason: 'bad-type' };
  if (!Number.isFinite(amount) || amount <= 0 || amount > 1_000_000) return { ok: false, reason: 'bad-amount' };

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const userRes = await client.query('SELECT * FROM users WHERE username = $1 FOR UPDATE', [uid]);
    if (userRes.rows.length === 0) throw new Error('user-not-found');
    const user = userRes.rows[0];
    if (user.blocked) throw new Error('account-blocked');

    if (type === 'withdraw') {
      if (Number(user.withdrawal_hold_until) > now()) throw new Error(`withdrawal-hold:${user.withdrawal_hold_reason}`);
      const main = Number(user.main_wallet);
      const pending = Number(user.pending_withdrawal);
      if (main - pending < amount) throw new Error('insufficient-main');
      await client.query('UPDATE users SET pending_withdrawal = $1, updated_at = $2 WHERE username = $3', [pending + amount, now(), uid]);
    }

    const id = crypto.randomUUID();
    const clientRequestId = String(data.clientRequestId || '').slice(0, 128);
    
    await client.query(`
      INSERT INTO transactions (id, username, uid, wallet_uid, telegram_id, telegram_username, display_name, type, amount, status, timestamp, reference, full_name, phone_number, image_data, reason, wallet_applied, client_request_id, updated_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
    `, [id, uid, uid, uid, user.telegram_id, user.telegram_username, user.display_name || uid, type, amount, 'pending', now(), 
        String(data.reference || ''), String(data.fullName || ''), String(data.phoneNumber || ''), String(data.imageData || '').slice(0, 2500000), '', 0, clientRequestId, now()]);

    if (clientRequestId) {
      await client.query('INSERT INTO transaction_requests (uid, client_request_id, tx_id, type, amount, created_at) VALUES ($1,$2,$3,$4,$5,$6)', 
        [uid, clientRequestId, id, type, amount, now()]);
    }

    await client.query('COMMIT');
    await sendWallet(uid);
    return { ok: true, id, clientRequestId };
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('[TX-REQUEST]', e.message);
    return { ok: false, reason: e.message };
  } finally {
    client.release();
  }
}

async function transactionImage(uid, id, imageData) {
  if (!id || !imageData || String(imageData).length > 2_500_000) return false;
  const res = await pool.query('UPDATE transactions SET image_data = $1, updated_at = $2 WHERE id = $3 AND username = $4 AND status = $5 RETURNING id', 
    [String(imageData), now(), id, uid, 'pending']);
  return res.rowCount > 0;
}

async function adminListTransactions() {
  const res = await pool.query("SELECT * FROM transactions WHERE status = 'pending' ORDER BY timestamp DESC");
  return res.rows.map(d => ({
    id: d.id, username: d.username, uid: d.uid || d.wallet_uid, walletUid: d.wallet_uid,
    telegramId: d.telegram_id, telegramUsername: d.telegram_username, displayName: d.display_name,
    type: d.type, amount: Number(d.amount), status: d.status, timestamp: Number(d.timestamp), reference: d.reference,
    fullName: d.full_name, phoneNumber: d.phone_number, imageData: d.image_data, reason: d.reason, walletApplied: Number(d.wallet_applied)
  }));
}

async function adminDecision(id, decision, reason = '') {
  if (decision === 'approve') decision = 'approved';
  if (decision === 'reject') decision = 'rejected';
  if (!['approved', 'rejected'].includes(decision)) return { ok: false, reason: 'bad-decision' };

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const txRes = await client.query('SELECT * FROM transactions WHERE id = $1 FOR UPDATE', [id]);
    if (txRes.rows.length === 0) throw new Error('not-found');
    const tx = txRes.rows[0];
    if (tx.status !== 'pending') throw new Error('already-decided');
    
    const userRes = await client.query('SELECT * FROM users WHERE username = $1 FOR UPDATE', [tx.wallet_uid || tx.uid]);
    if (userRes.rows.length === 0) throw new Error('wallet-user-not-found');
    const user = userRes.rows[0];

    let newMain = Number(user.main_wallet);
    let newPlay = Number(user.play_wallet);
    let newPending = Number(user.pending_withdrawal);
    const amount = Number(tx.amount);

    if (tx.type === 'deposit' && decision === 'approved') {
      newPlay += amount;
    } else if (tx.type === 'withdraw' && decision === 'approved') {
      if (newMain < amount || newPending < amount) throw new Error('insufficient-funds');
      newMain -= amount;
      newPending -= amount;
    } else if (tx.type === 'withdraw' && decision === 'rejected') {
      if (newPending < amount) throw new Error('insufficient-pending');
      newPending -= amount;
    }

    await client.query('UPDATE users SET main_wallet = $1, play_wallet = $2, pending_withdrawal = $3, updated_at = $4 WHERE username = $5', 
      [newMain, newPlay, newPending, now(), tx.wallet_uid || tx.uid]);

    await client.query('UPDATE transactions SET status = $1, reason = $2, wallet_applied = 1, updated_at = $3 WHERE id = $4', 
      [decision, String(reason).slice(0, 256), now(), id]);

    await client.query('COMMIT');
    
    const updatedTx = { ...tx, status: decision, reason };
    await sendWallet(tx.wallet_uid || tx.uid);
    await notifyTransaction(tx.wallet_uid || tx.uid, updatedTx);
    return { ok: true, tx: updatedTx };
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('[ADMIN] decision failed', e.message);
    return { ok: false, reason: e.message };
  } finally {
    client.release();
  }
}

// ============================================================================
// Automatic deposit verification (Polling)
// ============================================================================
function isTrustedSmsSender(record) {
  const raw = [record.from, record.sender, record.address, record.originator, record.source].filter(Boolean).map(v => String(v).toLowerCase()).join(' ');
  if (!raw) {
    const text = String(record.text || record.message || '').toLowerCase();
    return SMS_TRUSTED_SENDERS.some(s => text.includes(s));
  }
  return SMS_TRUSTED_SENDERS.some(s => raw.includes(s));
}

function normalizePaymentReference(value) { return String(value || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 128); }

function parsePaymentRecord(r) {
  const text = String(r.text || r.message || '').replace(/\s+/g, ' ').trim();
  let reference = r.reference || '';
  if (!reference && text) {
    const m = text.match(/(?:transaction|txn|ref(?:erence)?)\s*(?:number|code|no\.?|id)?\s*(?:is|:|\-)?\s*([A-Z0-9]{6,})/i) 
           || text.match(/receipt\/([A-Za-z0-9]{6,24})/i);
    if (m) reference = m[1];
  }
  let amount = Number(r.amount);
  if (!Number.isFinite(amount) || amount <= 0) {
    const m = text.match(/(?:ETB|Birr|ብር|Br\b|Amt|Amount)\s*[:\-]?\s*([0-9][0-9,]*(?:\.\d{1,2})?)/i);
    if (m) amount = Number(String(m[1]).replace(/,/g, ''));
  }
  return { reference: normalizePaymentReference(reference), amount: Number.isFinite(amount) ? amount : 0, isTrusted: isTrustedSmsSender(r) };
}

async function logAutoActivity(kind, smsId, reference, amount, txId, reason) {
  await pool.query('INSERT INTO auto_activity (id, kind, sms_id, reference, amount, tx_id, reason, at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)', 
    [crypto.randomUUID(), kind, String(smsId||''), reference || '', Number(amount)||0, txId || '', reason || '', now()]);
}

async function autoApproveMatchingDeposits(smsId, smsRecord) {
  const payment = parsePaymentRecord(smsRecord);
  if (!payment.isTrusted) return { ok: false, reason: 'untrusted-sender' };
  if (!payment.reference) return { ok: false, reason: 'no-reference' };
  if (!(payment.amount > 0)) return { ok: false, reason: 'no-amount' };

  const txRes = await pool.query("SELECT id, * FROM transactions WHERE status = 'pending' AND type = 'deposit'");
  const candidates = txRes.rows.filter(tx => normalizePaymentReference(tx.reference) === payment.reference && Math.abs(Number(tx.amount) - payment.amount) < 0.01);

  if (candidates.length !== 1) {
    await logAutoActivity('skipped', smsId, payment.reference, payment.amount, '', candidates.length === 0 ? 'no-match' : 'ambiguous-tx');
    return { ok: false };
  }
  
  const tx = candidates[0];
  const result = await adminDecision(tx.id, 'approved', `Auto-approved: SMS ${String(smsId).slice(0, 80)}`);
  if (!result.ok) {
    await logAutoActivity('error', smsId, payment.reference, payment.amount, tx.id, result.reason);
    return { ok: false, reason: result.reason };
  }
  
  await logAutoActivity('approved', smsId, payment.reference, payment.amount, tx.id, 'verified');
  return { ok: true, txId: tx.id, tx: result.tx };
}

async function processIncomingSmsSnapshot(row) {
  if (row.processed) return;
  try {
    const res = await autoApproveMatchingDeposits(row.id, row);
    await pool.query('UPDATE incoming_sms SET processed = true WHERE id = $1', [row.id]);
  } catch (e) { console.error('[AUTO-DEPOSIT]', e); }
}

function startAutomaticDepositMonitor() {
  setInterval(async () => {
    try {
      const res = await pool.query('SELECT * FROM incoming_sms WHERE processed = false LIMIT 10');
      for (const row of res.rows) {
        await processIncomingSmsSnapshot(row);
      }
    } catch (e) { console.error('[SMS-MONITOR]', e.message); }
  }, 5000);
  console.log('[AUTO-DEPOSIT] polling monitor started');
}

async function markNotificationRead(uid, id) {
  await pool.query('UPDATE notifications SET read = true WHERE id = $1 AND username = $2', [id, uid]);
}

async function claimPayout(uid, amount, roomId, round, kind) {
  const key = `payout:${roomId}_${round}_${uid}_${kind}`;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const marker = await client.query('SELECT id FROM notifications WHERE id = $1', [key]);
    if (marker.rows.length > 0) { await client.query('ROLLBACK'); return false; }
    
    await client.query('UPDATE users SET main_wallet = main_wallet + $1, wallet_updated_at = $2, updated_at = $2 WHERE username = $3', [amount, now(), uid]);
    await client.query('INSERT INTO notifications (id, username, type, read, timestamp) VALUES ($1,$2,$3,true,$4)', [key, uid, 'payout_marker', now()]);
    await client.query('COMMIT');
    await sendWallet(uid);
    return true;
  } catch (e) {
    await client.query('ROLLBACK');
    return false;
  } finally {
    client.release();
  }
}

async function creditReferral(ref, amount, roomId, round, winnerUid) {
  if (!ref || ref === winnerUid || amount <= 0) return false;
  const key = `ref:${roomId}_${round}_${winnerUid}_${ref}`;
  
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const marker = await client.query('SELECT id FROM notifications WHERE id = $1', [key]);
    if (marker.rows.length > 0) { await client.query('ROLLBACK'); return false; }

    await client.query('INSERT INTO users (username, display_name, created_at, updated_at) VALUES ($1,$1,$2,$2) ON CONFLICT (username) DO NOTHING', [ref, now()]);
    await client.query('UPDATE users SET main_wallet = main_wallet + $1, referral_earnings = referral_earnings + $1, wallet_updated_at = $2, updated_at = $2 WHERE username = $3', [amount, now(), ref]);
    await client.query('INSERT INTO notifications (id, username, type, read, timestamp) VALUES ($1,$2,$3,true,$4)', [key, ref, 'referral_marker', now()]);
    
    await client.query('COMMIT');
    await addNotification(ref, { message: `🎁 You earned ${amount} Birr commission from a win.` });
    await sendWallet(ref);
    return true;
  } catch (e) {
    await client.query('ROLLBACK');
    return false;
  } finally {
    client.release();
  }
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
        uid, picks: Array.isArray(p.picks) ? p.picks.map(Number) : [], isBot: !!p.isBot, active: !!p.active, displayName: String(p.displayName || uid)
      });
    }
  }
  snapshot() {
    const players = {};
    for (const [uid, p] of this.players) if (p.active || p.isBot) players[uid] = { name: p.displayName || uid, picks: [...p.picks], isBot: !!p.isBot, bet: this.bet };
    const activePlayers = [...this.players.values()].filter(p => p.active);
    const totalPicks = activePlayers.reduce((sum, p) => sum + p.picks.length, 0);
    const totalBets = totalPicks * this.bet;
    const humanBets = activePlayers.filter(p => !p.isBot).reduce((sum, p) => sum + p.picks.length * this.bet, 0);
    return {
      gameState: this.phase, round: this.round, betAmount: this.bet, selectionEndsAt: this.endsAt, endsAt: this.endsAt,
      numbersTaken: Object.fromEntries([...this.taken.keys()].map(n => [n, true])), players, 
      winners: [...this.winners].map(uid => this.players.get(uid)?.displayName || uid),
      winningNumber: this.winningNumber, prizePool: this.prizePool, potAmount: this.prizePool,
      totalBets, humanBets, totalPicks, displayedPrizePool: Math.floor(totalBets * PAYOUT_RATE),
      onlineCount: activePlayers.length, botCount: activePlayers.filter(p => p.isBot).length, realPlayerCount: activePlayers.filter(p => !p.isBot).length,
      serverNow: now(), lastResult: this.lastResult,
    };
  }
  async persist() {
    const players = {};
    for (const [uid, p] of this.players) players[uid] = { picks: [...p.picks], isBot: !!p.isBot, active: !!p.active, displayName: p.displayName || uid };
    const taken = Object.fromEntries(this.taken.entries());
    await pool.query(`
      INSERT INTO rooms (id, bet, phase, round, ends_at, winning_number, winners_json, prize_pool, last_result_json, taken_json, players_json, updated_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
      ON CONFLICT (id) DO UPDATE SET 
        phase = EXCLUDED.phase, round = EXCLUDED.round, ends_at = EXCLUDED.ends_at, winning_number = EXCLUDED.winning_number,
        winners_json = EXCLUDED.winners_json, prize_pool = EXCLUDED.prize_pool, last_result_json = EXCLUDED.last_result_json,
        taken_json = EXCLUDED.taken_json, players_json = EXCLUDED.players_json, updated_at = EXCLUDED.updated_at
    `, [this.id, this.bet, this.phase, this.round, this.endsAt, this.winningNumber, JSON.stringify(this.winners), this.prizePool, 
        this.lastResult ? JSON.stringify(this.lastResult) : null, JSON.stringify(taken), JSON.stringify(players), now()]);
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
  const res = await pool.query('SELECT * FROM rooms');
  const rowMap = new Map();
  res.rows.forEach(row => rowMap.set(row.id, row));
  rooms = { room_15: new Room('room_15', 15, rowMap.get('room_15')), room_30: new Room('room_30', 30, rowMap.get('room_30')) };
  for (const room of Object.values(rooms)) for (const p of room.players.values()) if (!p.isBot) p.active = false;
}
async function saveRoom(room) { await room.persist(); room.broadcast(); }

async function debit(uid, amount, roomId, round) {
  const amt = toFiniteNumber(amount, 0);
  if (amt <= 0) return { ok: false, reason: 'bad-amount' };
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const res = await client.query('SELECT play_wallet FROM users WHERE username = $1 FOR UPDATE', [uid]);
    if (res.rows.length === 0) throw new Error('user-not-found');
    if (Number(res.rows[0].play_wallet) < amt) throw new Error('insufficient');
    
    await client.query('UPDATE users SET play_wallet = play_wallet - $1, updated_at = $2, wallet_updated_at = $2 WHERE username = $3', [amt, now(), uid]);
    await client.query('COMMIT');
    await sendWallet(uid);
    return { ok: true };
  } catch (e) {
    await client.query('ROLLBACK');
    await sendWallet(uid);
    return { ok: false, reason: e.message };
  } finally {
    client.release();
  }
}

async function refundWager(uid, amount, roomId, round) {
  const amt = toFiniteNumber(amount, 0);
  if (amt <= 0) return false;
  try {
    await pool.query('UPDATE users SET play_wallet = play_wallet + $1, updated_at = $2, wallet_updated_at = $2 WHERE username = $3', [amt, now(), uid]);
    await sendWallet(uid);
    return true;
  } catch (e) { return false; }
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
    const d = await debit(uid, room.bet, room.id, room.round);
    if (!d.ok) return d;
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
    if (!p || !p.picks.includes(num) || room.taken.get(num) !== uid) return { ok: false, reason: 'not-picked' };
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
  const allWithPicks = [...room.players.values()].filter(p => p.picks.length > 0 && (p.isBot || p.active));
  if (!allWithPicks.length) return resetRound(room, 'no-picks');
  const allNums = [...new Set(allWithPicks.flatMap(p => p.picks))];
  if (!allNums.length) return resetRound(room, 'no-picks');
  
  const winningNumber = allNums[Math.floor(Math.random() * allNums.length)];
  const winners = allWithPicks.filter(p => p.picks.includes(winningNumber)).map(p => p.uid);
  const totalPicksInRoom = allWithPicks.reduce((s, p) => s + p.picks.length, 0);
  const prizePool = Math.floor(totalPicksInRoom * room.bet * PAYOUT_RATE);
  
  room.winningNumber = winningNumber; room.winners = winners; room.prizePool = prizePool;
  room.phase = 'spinning'; room.endsAt = now() + SPINNING_SECONDS * 1000; room.dirty = true;
  
  if (winners.length && prizePool > 0) {
    const share = Math.floor(prizePool / winners.length);
    for (const uid of winners) {
      if (share <= 0) break;
      if (!(await claimPayout(uid, share, room.id, room.round, 'win'))) continue;
      if (!room.players.get(uid)?.isBot) {
        await addNotification(uid, { message: `🏆 You won ${share} Birr in round ${room.round}!` });
      }
    }
  }
  room.lastResult = { round: room.round, winningNumber, winners: winners.map(uid => room.players.get(uid)?.displayName || uid), winAmount: (winners.length && prizePool > 0) ? Math.floor(prizePool / winners.length) : 0, setAt: now() };
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
  const pool = [...BOT_NAMES]; const off = ((round - 1) * 7) % pool.length;
  return pool.slice(off).concat(pool.slice(0, off)).slice(0, Math.min(count, pool.length));
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
    } else { p.active = true; p.displayName = p.displayName || uid.replace(/^__bot_/, ''); }
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
    for (let i = pass.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [pass[i], pass[j]] = [pass[j], pass[i]]; }
    for (const name of pass) order.push(name);
  }
  let index = 0;
  const runNextPick = async () => {
    if (room.phase !== 'selection' || index >= order.length || now() >= room.endsAt - 1000) return;
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
        if (num == null) { for (let candidate = 1; candidate <= 200; candidate++) if (!room.taken.has(candidate)) { num = candidate; break; } }
        if (num == null) return;
        room.taken.set(num, uid); p.picks.push(num); room.dirty = true; room.broadcast();
      });
    } catch (err) { console.error('[BOT-PICK]', err); }
    if (room.phase !== 'selection' || index >= order.length) return;
    room.botTimers.push(setTimeout(runNextPick, 300));
  };
  room.botTimers.push(setTimeout(runNextPick, 350));
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

// ============================================================================
// WebSocket Setup
// ============================================================================
const conns = new Map();
const wss = new WebSocketServer({ noServer: true });
function send(ws, obj) { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); }

wss.on('connection', (ws) => {
  ws.isAlive = true; ws.authenticatedAt = null;
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
    if (set) { set.delete(ws); if (!set.size) conns.delete(ws.uid); }
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
    if (!verification.ok) return send(ws, { type: 'error', message: 'auth-failed' });
    
    const telegramUser = verification.user;
    const resolved = await resolveTelegramUser(telegramUser, m.ref || null);
    if (!resolved?.uid) return send(ws, { type: 'error', message: 'auth-failed' });

    const uid = resolved.uid;
    const isBlockedRes = await pool.query('SELECT blocked, block_reason, blocked_at FROM users WHERE username = $1', [uid]);
    if (isBlockedRes.rows[0]?.blocked) {
      send(ws, { type: 'account-blocked', reason: isBlockedRes.rows[0].block_reason, at: Number(isBlockedRes.rows[0].blocked_at) });
      try { ws.close(4003, 'account-blocked'); } catch {}
      return;
    }

    await ensureUser(uid, m.ref || null, false, {
      telegram_id: telegramUser.id, display_name: [telegramUser.first_name, telegramUser.last_name].filter(Boolean).join(' ') || uid, last_ip: websocketClientIp(ws)
    });
    
    clearTimeout(ws.authTimer);
    ws.authenticatedAt = now(); ws.uid = uid; ws.authed = true;
    ws.displayName = resolved.user.display_name;

    let set = conns.get(uid);
    if (!set) { set = new Set(); conns.set(uid, set); }
    set.add(ws);

    const holdRes = await pool.query('SELECT withdrawal_hold_until FROM users WHERE username = $1', [uid]);
    send(ws, { type: 'authed', uid, displayName: ws.displayName, telegramId: telegramUser.id, withdrawalHoldUntil: Number(holdRes.rows[0]?.withdrawal_hold_until || 0) });
    send(ws, { type: 'wallet', ...(await walletFor(uid)) });

    const txSnap = await pool.query("SELECT id, type, amount, status, timestamp FROM transactions WHERE wallet_uid = $1 AND status = 'pending' ORDER BY timestamp DESC LIMIT 10", [uid]);
    send(ws, { type: 'pending-transactions', transactions: txSnap.rows.map(t => ({ ...t, timestamp: Number(t.timestamp), amount: Number(t.amount) })) });
    return;
  }

  if (m.type === 'admin-auth') {
    if (!ADMIN_KEY || typeof m.key !== 'string' || !constantTimeEqual(m.key, ADMIN_KEY)) return ws.close(4003, 'admin auth failed');
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
    ws.roomId = m.room;
    const existing = room.players.get(ws.uid);
    if (existing && existing.picks.length && !existing.active) await refundDisconnectedPlayer(room, existing);
    if (!room.players.has(ws.uid)) room.players.set(ws.uid, { uid: ws.uid, picks: [], isBot: false, active: true, displayName: ws.displayName });
    else { room.players.get(ws.uid).active = true; }
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
    try {
      await pool.query('UPDATE users SET main_wallet = main_wallet - $1, play_wallet = play_wallet + $1, updated_at = $2 WHERE username = $3 AND main_wallet >= $1', [amount, now(), ws.uid]);
      await sendWallet(ws.uid);
    } catch (e) { send(ws, { type: 'error', message: 'transfer-failed' }); }
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
  if (m.type === 'notification-read') { await markNotificationRead(ws.uid, String(m.id || '')); return; }
}

setInterval(() => { for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; try { ws.ping(); } catch {} } }, 60000);

// ============================================================================
// Express API Routes
// ============================================================================
const app = express();
app.use(express.json({ limit: '3mb' }));
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, X-Admin-Key');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

app.get('/', (_, res) => res.send('Winzo Server is running. Database is PostgreSQL.'));
app.get('/health', (_, res) => res.status(200).send('OK')); // AletCloud Health Check

function isAdmin(req) { return ADMIN_KEY && constantTimeEqual(String(req.get('x-admin-key') || ''), ADMIN_KEY); }

app.get('/admin/pending', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  try { res.json({ ok: true, transactions: await adminListTransactions() }); }
  catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.get('/admin/decision', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  const id = String(req.query.id || ''), decision = String(req.query.decision || '');
  try {
    const result = await adminDecision(id, decision, 'Processed via HTTP admin panel');
    res.json({ ok: result.ok, transaction: result.tx, error: result.reason });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.get('/admin/users', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  try {
    const usersRes = await pool.query('SELECT * FROM users ORDER BY updated_at DESC');
    res.json({ ok: true, users: usersRes.rows.map(u => ({ uid: u.username, name: u.display_name, telegram_id: u.telegram_id, main: Number(u.main_wallet), play: Number(u.play_wallet), pending: Number(u.pending_withdrawal), blocked: u.blocked })) });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ============================================================================
// 🚨 TEMPORARY FIREBASE DIRECT MIGRATION ROUTE 🚨
// ============================================================================
app.get('/firebase-direct-migrate', async (req, res) => {
  try {
    if (!fs.existsSync('./serviceAccountKey.json')) {
      return res.send('❌ Error: serviceAccountKey.json not found in repository root.');
    }

    res.write('Initializing Firebase...\n');
    const serviceAccount = JSON.parse(fs.readFileSync('./serviceAccountKey.json', 'utf8'));
    
    if (!admin.apps.length) {
      admin.initializeApp({
        credential: admin.credential.cert(serviceAccount),
        databaseURL: FIREBASE_DB_URL
      });
    }

    res.write('Fetching data from Firebase...\n');
    const snapshot = await admin.database().ref('/').once('value');
    const data = snapshot.val();

    if (!data) return res.end('No data found in Firebase.');

    res.write('Migrating to PostgreSQL...\n');
    const client = await pool.connect();
    await client.query('BEGIN');

    // Migrate Users
    if (data.users) {
      for (const [uid, u] of Object.entries(data.users)) {
        await client.query(`
          INSERT INTO users (username, display_name, telegram_id, main_wallet, play_wallet, pending_withdrawal, referral_earnings, created_at, updated_at)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (username) DO NOTHING
        `, [uid, u.display_name || uid, u.telegram_id || null, Number(u.main_wallet||0), Number(u.play_wallet||0), Number(u.pending_withdrawal||0), Number(u.referral_earnings||0), Number(u.created_at||Date.now()), Number(u.updated_at||Date.now())]);
      }
      res.write(`✅ Users Migrated: ${Object.keys(data.users).length}\n`);
    }

    // Migrate Transactions
    if (data.transactions) {
      for (const [id, t] of Object.entries(data.transactions)) {
        await client.query(`
          INSERT INTO transactions (id, username, wallet_uid, type, amount, status, timestamp, reference)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (id) DO NOTHING
        `, [id, t.username || '', t.wallet_uid || t.uid || t.username || '', t.type || '', Number(t.amount||0), t.status || 'pending', Number(t.timestamp||Date.now()), t.reference || '']);
      }
      res.write(`✅ Transactions Migrated: ${Object.keys(data.transactions).length}\n`);
    }

    await client.query('COMMIT');
    client.release();
    res.end('🎉 SUCCESS! Migration Complete. Go back to GitHub and delete the migration code & serviceAccountKey.json immediately.');
  } catch (error) {
    console.error(error);
    res.status(500).send('❌ Migration Failed: ' + error.message);
  }
});
// ============================================================================

const server = http.createServer(app);
server.on('upgrade', (req, socket, head) => {
  wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
});

// Boot: Listen immediately so AletCloud health check passes
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Server listening on :${PORT}`);
  
  // Initialize DB and load rooms AFTER the server is listening
  initDb().then(() => loadRooms()).then(() => {
    startAutomaticDepositMonitor();
    for (const room of Object.values(rooms)) {
      if (room.phase === 'selection' && room.endsAt <= now()) { room.endsAt = now() + SELECTION_SECONDS * 1000; room.dirty = true; }
      if (room.phase === 'selection') normalizeRoundBots(room);
      room.persist();
      scheduleBots(room);
    }
    console.log('✅ Background initialization complete.');
  }).catch(err => { 
    console.error('Failed to initialize background tasks.', err); 
  });
});

async function gracefulShutdown(signal) {
  console.log(`[SHUTDOWN] ${signal}`);
  for (const room of Object.values(rooms)) { clearBotTimers(room); }
  for (const ws of wss.clients) { try { ws.close(1001, 'server shutting down'); } catch {} }
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000).unref();
}
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));
