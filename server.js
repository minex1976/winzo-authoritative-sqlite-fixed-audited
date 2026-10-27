import express from 'express';
import { WebSocketServer } from 'ws';
import crypto from 'crypto';
import http from 'http';
import pg from 'pg';

const { Pool } = pg;

// ============================================================================
// DATABASE
// ============================================================================
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

// ============================================================================
// CONFIG
// ============================================================================
const PORT = Number(process.env.PORT || 3000);
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const ADMIN_KEY = process.env.ADMIN_KEY || '';
const SMS_WEBHOOK_SECRET = process.env.SMS_WEBHOOK_SECRET || '9f8a7b6c5d4e3f2a1b0c1976';
const NODE_ENV = process.env.NODE_ENV || 'production';

const SIGNUP_BONUS = Number(process.env.SIGNUP_BONUS || 15);

if (!ADMIN_KEY) console.warn('WARNING: ADMIN_KEY is not set. Admin approval will be disabled.');
if (!BOT_TOKEN) console.warn('WARNING: BOT_TOKEN is not set. All WebSocket authentication will fail.');

const SELECTION_SECONDS = 30;
const SPINNING_SECONDS = 3;
const RESULTS_SECONDS = 13;
const MAX_PICKS = 5;
const PAYOUT_RATE = 0.80;
const REFERRAL_RATE = 0.05;
const BOT_START_WALLET = 1_000_000;
const BOT_MIN = 4;
const BOT_MAX = 6;

const AUTO_DEPOSIT_AMOUNT_TOLERANCE = 0.01;
const MIN_REFERENCE_LENGTH = 6;
const MAX_REFERENCE_LENGTH = 128;
const MIN_TELEBIRR_REF_LENGTH = 8;
const SMS_MAX_AGE_MS = 24 * 60 * 60 * 1000;
const DEPOSIT_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000;
const MIN_DEPOSIT_AMOUNT = 15;
const MAX_DEPOSIT_AMOUNT = 1_000_000;
const DEPOSIT_RATE_LIMIT_WINDOW_MS = Number(process.env.DEPOSIT_RATE_WINDOW_MS || 10 * 60 * 1000);
const DEPOSIT_RATE_LIMIT_MAX = Number(process.env.DEPOSIT_RATE_MAX || 15);
const DUPLICATE_SMS_WINDOW_MS = 60 * 1000;
const MAX_IMAGE_BYTES = 2_000_000;
const WS_MAX_PAYLOAD = 5 * 1024 * 1024;

const BOT_NAMES = [
  "Abebe", "Kebede", "Selam", "Hana", "Dawit", "Meron", "Yonas", "Sara",
  "Bereket", "Tigist", "Elias", "Liya", "Samuel", "Bethel", "Nahom", "Rediet",
  "Biniam", "Feven", "Girma", "Mimi", "Henok", "Tsion", "Fikru", "Eden",
  "Dagmawi", "Ruth", "Natnael", "Mahlet", "Yosef", "Rahel", "Tewodros", "Marta",
  "Amanuel", "Sofia", "Zelalem", "Alem", "Beka", "Chala", "Desta", "Eyerus"
];

const SMS_TRUSTED_SENDERS = ['127', 'telebirr', 'ethio telecom', 'ethiotelecom', 'ethio-telecom'];
const DAY = 86400000;

// ============================================================================
// DATABASE SCHEMA + MIGRATIONS
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
          id VARCHAR(128) PRIMARY KEY, sender VARCHAR(64), text TEXT, timestamp BIGINT, processed BOOLEAN DEFAULT FALSE,
          matched_tx_id VARCHAR(128), text_hash VARCHAR(64)
      );
      CREATE TABLE IF NOT EXISTS auto_activity (
          id VARCHAR(128) PRIMARY KEY, kind VARCHAR(32), sms_id VARCHAR(128), reference VARCHAR(128), amount NUMERIC,
          tx_id VARCHAR(128), reason VARCHAR(256), at BIGINT
      );
      CREATE TABLE IF NOT EXISTS transaction_requests (
          uid VARCHAR(64), client_request_id VARCHAR(128), tx_id VARCHAR(128), type VARCHAR(32), amount NUMERIC, created_at BIGINT,
          PRIMARY KEY (uid, client_request_id)
      );
      CREATE TABLE IF NOT EXISTS used_references (
          reference VARCHAR(128) PRIMARY KEY,
          sms_id VARCHAR(128), tx_id VARCHAR(128), amount NUMERIC, sender VARCHAR(64), username VARCHAR(64), consumed_at BIGINT
      );
      CREATE TABLE IF NOT EXISTS deposit_attempts (
          id VARCHAR(128) PRIMARY KEY, username VARCHAR(64), created_at BIGINT
      );

      CREATE TABLE IF NOT EXISTS request_trace (
          trace_id VARCHAR(64) PRIMARY KEY,
          uid VARCHAR(64),
          stage VARCHAR(64) NOT NULL,
          status VARCHAR(32) NOT NULL,
          type VARCHAR(32),
          amount NUMERIC,
          reference VARCHAR(128),
          client_request_id VARCHAR(128),
          tx_id VARCHAR(128),
          reason VARCHAR(256),
          detail JSONB DEFAULT '{}'::jsonb,
          at BIGINT NOT NULL
      );

      -- ✅ persistent record of human winners per round
      CREATE TABLE IF NOT EXISTS game_winners (
          id VARCHAR(128) PRIMARY KEY,
          username VARCHAR(64) NOT NULL,
          display_name VARCHAR(128),
          room_id VARCHAR(64) NOT NULL,
          round INT NOT NULL,
          win_amount NUMERIC NOT NULL,
          winning_number INT,
          prize_pool NUMERIC,
          created_at BIGINT NOT NULL
      );

      -- ✅ NEW: per-round snapshot of human participation (all humans, not just winners)
      CREATE TABLE IF NOT EXISTS round_snapshots (
          id VARCHAR(160) PRIMARY KEY,
          room_id VARCHAR(64) NOT NULL,
          round INT NOT NULL,
          bet NUMERIC NOT NULL,
          human_count INT NOT NULL DEFAULT 0,
          human_picks INT NOT NULL DEFAULT 0,
          bot_count INT NOT NULL DEFAULT 0,
          bot_picks INT NOT NULL DEFAULT 0,
          total_picks INT NOT NULL DEFAULT 0,
          prize_pool NUMERIC NOT NULL DEFAULT 0,
          winning_number INT,
          winner_count INT NOT NULL DEFAULT 0,
          human_winner_count INT NOT NULL DEFAULT 0,
          human_win_amount NUMERIC NOT NULL DEFAULT 0,
          human_player_ids JSONB DEFAULT '[]'::jsonb,
          started_at BIGINT NOT NULL,
          ended_at BIGINT NOT NULL,
          created_at BIGINT NOT NULL
      );
    `);

    await client.query(`
      ALTER TABLE incoming_sms ADD COLUMN IF NOT EXISTS matched_tx_id VARCHAR(128);
      ALTER TABLE incoming_sms ADD COLUMN IF NOT EXISTS text_hash VARCHAR(64);
      ALTER TABLE transactions  ADD COLUMN IF NOT EXISTS reference VARCHAR(128);
      ALTER TABLE transactions  ADD COLUMN IF NOT EXISTS client_request_id VARCHAR(128);
      ALTER TABLE users         ADD COLUMN IF NOT EXISTS fraud_status VARCHAR(64);
      ALTER TABLE users         ADD COLUMN IF NOT EXISTS account_status VARCHAR(64);
      ALTER TABLE users         ADD COLUMN IF NOT EXISTS referral_earnings NUMERIC DEFAULT 0;
    `);

    await client.query(`UPDATE users SET referral_earnings = 0 WHERE referral_earnings IS NULL;`);

    console.log('✅ Migrations applied.');

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_tx_pending_deposit
        ON transactions (status, type) WHERE status = 'pending' AND type = 'deposit';
      CREATE INDEX IF NOT EXISTS idx_tx_reference ON transactions (reference);
      CREATE INDEX IF NOT EXISTS idx_sms_unprocessed ON incoming_sms (processed) WHERE processed = false;
      CREATE INDEX IF NOT EXISTS idx_sms_text_hash ON incoming_sms (text_hash);
      CREATE INDEX IF NOT EXISTS idx_deposit_attempts_user ON deposit_attempts (username, created_at);
      CREATE INDEX IF NOT EXISTS idx_request_trace_uid ON request_trace (uid, at DESC);
      CREATE INDEX IF NOT EXISTS idx_request_trace_tx ON request_trace (tx_id);
      CREATE INDEX IF NOT EXISTS idx_request_trace_stage ON request_trace (stage, status, at DESC);
      CREATE INDEX IF NOT EXISTS idx_game_winners_username ON game_winners (username, created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_game_winners_room ON game_winners (room_id, round);
      CREATE INDEX IF NOT EXISTS idx_users_referred_by ON users (referred_by) WHERE referred_by IS NOT NULL;

      -- ✅ NEW: indexes for round_snapshots
      CREATE INDEX IF NOT EXISTS idx_round_snapshots_time ON round_snapshots (ended_at DESC);
      CREATE INDEX IF NOT EXISTS idx_round_snapshots_room_round ON round_snapshots (room_id, round);
      CREATE INDEX IF NOT EXISTS idx_round_snapshots_room_time ON round_snapshots (room_id, ended_at DESC);
    `);
    console.log('✅ Database Schema Verified.');
  } catch (e) {
    console.error('❌ Schema init failed:', e);
  } finally {
    client.release();
  }
}

// ============================================================================
// UTILITY HELPERS
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
// REQUEST TRACE HELPERS
// ============================================================================
async function trace(traceId, uid, stage, status, opts = {}) {
  try {
    await pool.query(
      `INSERT INTO request_trace (trace_id, uid, stage, status, type, amount, reference, client_request_id, tx_id, reason, detail, at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
      [
        String(traceId).slice(0, 64),
        uid ? String(uid).slice(0, 64) : null,
        String(stage).slice(0, 64),
        String(status).slice(0, 32),
        opts.type ? String(opts.type).slice(0, 32) : null,
        Number.isFinite(Number(opts.amount)) ? Number(opts.amount) : null,
        opts.reference ? String(opts.reference).slice(0, 128) : null,
        opts.clientRequestId ? String(opts.clientRequestId).slice(0, 128) : null,
        opts.txId ? String(opts.txId).slice(0, 128) : null,
        opts.reason ? String(opts.reason).slice(0, 256) : null,
        JSON.stringify(opts.detail || {}),
        now(),
      ]
    );
  } catch (e) {
    console.error('[TRACE] insert failed:', e.message, { traceId, stage, status });
  }
  console.log(`[TRACE] ${traceId} stage=${stage} status=${status}` +
    (opts.reason ? ` reason=${opts.reason}` : '') +
    (opts.txId ? ` tx=${opts.txId}` : '') +
    (opts.amount ? ` amount=${opts.amount}` : '') +
    (opts.reference ? ` ref=${String(opts.reference).slice(0, 20)}` : ''));
}

function newTraceId() { return crypto.randomUUID(); }

// ============================================================================
// TELEGRAM INITDATA VERIFICATION
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
// USER MANAGEMENT
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
      telegram_username: extra.telegram_username || null, main_wallet: 0,
      play_wallet: bot ? BOT_START_WALLET : SIGNUP_BONUS,
      signup_bonus_remaining: bot ? 0 : SIGNUP_BONUS,
      signup_bonus_original: bot ? 0 : SIGNUP_BONUS,
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
  const timestamp = Number(data.timestamp || now());
  const dbType = data.type || 'notification';

  const wsPayload = {
    ...data,
    type: 'notification',
    notificationType: dbType,
    id,
    timestamp
  };

  await pool.query(
    'INSERT INTO notifications (id, username, type, message, status, tx_type, tx_id, timestamp, read) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,false)',
    [id, username, dbType, String(data.message || ''), data.status || null, data.txType || null, data.txId || null, timestamp]
  );

  const set = conns.get(username);
  if (set) for (const ws of set) if (ws.readyState === 1) ws.send(JSON.stringify(wsPayload));
}

async function notifyTransaction(username, tx) {
  let message;
  if (tx.type === 'deposit' && tx.status === 'approved') {
    message = 'ትዕዛዙ ተፈፅሟል። የዊንዞ ቤተሰብ በመሆንዎ እናመሰግናለን። መልካም ጨዋታ!';
  } else if (tx.status === 'approved') {
    message = `${tx.type === 'withdraw' ? 'Withdrawal' : 'Deposit'} approved.`;
  } else {
    message = 'Your request was rejected by admin.';
  }
  await addNotification(username, {
    type: 'transaction_result', status: tx.status, txType: tx.type, txId: tx.id, reason: tx.reason || '',
    message
  });
  const set = conns.get(username);
  if (set) for (const ws of set) if (ws.readyState === 1) ws.send(JSON.stringify({ type: 'transaction-status', id: tx.id, status: tx.status, txType: tx.type, reason: tx.reason || '' }));
}

// ============================================================================
// TRANSACTIONS
// ============================================================================
async function transactionRequest(uid, data) {
  const traceId = String(data.__traceId || newTraceId());
  const type = data.txType;
  const amount = Number(data.amount);
  const rawRef = normalizePaymentReference(data.reference);
  const clientRequestId = String(data.clientRequestId || '').slice(0, 128);
  const imgLen = String(data.imageData || '').length;

  await trace(traceId, uid, 'tx.received', 'ok', {
    type, amount, reference: rawRef, clientRequestId,
    detail: { imgLen, hasImage: imgLen > 0 }
  });

  if (!['deposit', 'withdraw'].includes(type)) {
    await trace(traceId, uid, 'tx.validate', 'fail', { type, reason: 'bad-type' });
    return { ok: false, reason: 'bad-type', traceId };
  }
  if (!Number.isFinite(amount) || amount <= 0 || amount > 1_000_000) {
    await trace(traceId, uid, 'tx.validate', 'fail', { type, amount, reason: 'bad-amount' });
    return { ok: false, reason: 'bad-amount', traceId };
  }

  if (type === 'deposit') {
    if (amount < MIN_DEPOSIT_AMOUNT || amount > MAX_DEPOSIT_AMOUNT) {
      await trace(traceId, uid, 'tx.ratelimit', 'fail', { type, amount, reason: 'amount-out-of-range' });
      return { ok: false, reason: 'amount-out-of-range', traceId };
    }
    const since = now() - DEPOSIT_RATE_LIMIT_WINDOW_MS;
    const recent = await pool.query(
      'SELECT COUNT(*)::int AS c FROM deposit_attempts WHERE username = $1 AND created_at > $2',
      [uid, since]
    );
    if (recent.rows[0].c >= DEPOSIT_RATE_LIMIT_MAX) {
      await trace(traceId, uid, 'tx.ratelimit', 'fail', {
        type, amount, reason: 'rate-limited',
        detail: { count: recent.rows[0].c, max: DEPOSIT_RATE_LIMIT_MAX, windowMs: DEPOSIT_RATE_LIMIT_WINDOW_MS }
      });
      return { ok: false, reason: 'rate-limited', traceId };
    }
    await pool.query('INSERT INTO deposit_attempts (id, username, created_at) VALUES ($1,$2,$3)',
      [crypto.randomUUID(), uid, now()]);
    await trace(traceId, uid, 'tx.ratelimit', 'ok', { type, amount, detail: { count: recent.rows[0].c + 1 } });
  }

  const client = await pool.connect();
  let committed = false;
  let txId = null;
  try {
    await client.query('BEGIN');
    await trace(traceId, uid, 'tx.begin', 'ok', { type, amount });

    const userRes = await client.query('SELECT * FROM users WHERE username = $1 FOR UPDATE', [uid]);
    if (userRes.rows.length === 0) throw new Error('user-not-found');
    const user = userRes.rows[0];
    if (user.blocked) throw new Error('account-blocked');
    if (String(user.fraud_status || '').toLowerCase() === 'frozen') throw new Error('account-frozen');
    await trace(traceId, uid, 'tx.user-locked', 'ok', { type, amount });

    if (type === 'deposit') {
      if (rawRef) {
        const prior = await client.query('SELECT tx_id FROM used_references WHERE reference = $1', [rawRef]);
        if (prior.rows.length > 0) throw new Error('reference-already-used');
        const dup = await client.query(
          "SELECT id FROM transactions WHERE type = 'deposit' AND status IN ('pending','approved') AND UPPER(REGEXP_REPLACE(reference, '[^A-Za-z0-9]', '', 'g')) = $1 LIMIT 1",
          [rawRef]
        );
        if (dup.rows.length > 0) throw new Error('reference-already-used');
        await trace(traceId, uid, 'tx.ref-check', 'ok', { type, reference: rawRef });
      } else {
        await trace(traceId, uid, 'tx.ref-check', 'warn', { type, reason: 'empty-reference' });
      }
    }

    if (type === 'withdraw') {
      if (Number(user.withdrawal_hold_until) > now()) throw new Error(`withdrawal-hold:${user.withdrawal_hold_reason}`);
      const main = Number(user.main_wallet);
      const pending = Number(user.pending_withdrawal);
      if (main - pending < amount) throw new Error('insufficient-main');
      await client.query('UPDATE users SET pending_withdrawal = $1, updated_at = $2 WHERE username = $3', [pending + amount, now(), uid]);
      await trace(traceId, uid, 'tx.withdraw-hold', 'ok', { type, amount });
    }

    const id = crypto.randomUUID();
    txId = id;

    await client.query(`
      INSERT INTO transactions (id, username, uid, wallet_uid, telegram_id, telegram_username, display_name, type, amount, status, timestamp, reference, full_name, phone_number, image_data, reason, wallet_applied, client_request_id, updated_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
    `, [id, uid, uid, uid, user.telegram_id, user.telegram_username, user.display_name || uid, type, amount, 'pending', now(),
        String(data.reference || ''), String(data.fullName || ''), String(data.phoneNumber || ''), String(data.imageData || '').slice(0, 2500000), '', 0, clientRequestId, now()]);
    await trace(traceId, uid, 'tx.insert', 'ok', { type, amount, reference: rawRef, txId: id, clientRequestId });

    if (clientRequestId) {
      await client.query('INSERT INTO transaction_requests (uid, client_request_id, tx_id, type, amount, created_at) VALUES ($1,$2,$3,$4,$5,$6)',
        [uid, clientRequestId, id, type, amount, now()]);
    }

    await client.query('COMMIT');
    committed = true;
    await trace(traceId, uid, 'tx.commit', 'ok', { type, amount, reference: rawRef, txId: id, clientRequestId });
  } catch (e) {
    if (!committed) {
      try { await client.query('ROLLBACK'); } catch (_) {}
    }
    await trace(traceId, uid, 'tx.rollback', 'fail', { type, amount, reference: rawRef, reason: e.message });
    console.error('[TX-REQUEST] FAILED', { traceId, uid, type, amount, reason: e.message });
    return { ok: false, reason: e.message, traceId };
  } finally {
    client.release();
  }

  try {
    await sendWallet(uid);
    await trace(traceId, uid, 'tx.wallet-push', 'ok', { type, amount, txId });
  } catch (e) {
    await trace(traceId, uid, 'tx.wallet-push', 'fail', { type, amount, txId, reason: e.message });
  }

  if (type === 'deposit') {
    rescanPendingSms(traceId)
      .then(async () => {
        try {
          const statusCheck = await pool.query('SELECT status FROM transactions WHERE id = $1', [txId]);
          if (statusCheck.rows[0]?.status === 'pending') {
            await addNotification(uid, {
              type: 'notification',
              message: 'ገንዘብ ገቢ ማደረግዎን የሚያሳይ ማስረጃ አልተገኘም።'
            });
            await trace(traceId, uid, 'tx.wait-msg', 'ok', { txId });
          }
        } catch (e) {
          console.error('[TX-WAIT-MSG]', e.message);
        }
      })
      .catch(err => {
        console.error('[TX-RESCAN]', err.message);
        trace(traceId, uid, 'tx.rescan', 'fail', { txId, reason: err.message }).catch(() => {});
      });
    await trace(traceId, uid, 'tx.rescan', 'ok', { txId });
  }

  return { ok: true, id: txId, traceId, clientRequestId };
}

async function transactionImage(uid, id, imageData) {
  if (!id || !imageData || String(imageData).length > 2_500_000) return false;
  const res = await pool.query('UPDATE transactions SET image_data = $1, updated_at = $2 WHERE id = $3 AND username = $4 AND status = $5 RETURNING id',
    [String(imageData), now(), id, uid, 'pending']);
  return res.rowCount > 0;
}

async function adminListTransactions() {
  const res = await pool.query(
    "SELECT * FROM transactions WHERE status = 'pending' ORDER BY timestamp DESC LIMIT 500"
  );
  console.log(`[ADMIN] Returning ${res.rows.length} pending transactions.`);
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
// AUTO-DEPOSIT STRICT MODE
// ============================================================================
function normalizeSender(value) {
  return String(value || '').toLowerCase().replace(/[^a-z0-9 ]+/g, ' ').replace(/\s+/g, ' ').trim();
}

function isTrustedSmsSender(record) {
  const raw = [record.from, record.sender, record.address, record.originator, record.source]
    .filter(Boolean).map(v => normalizeSender(v)).filter(Boolean);
  if (!raw.length) return false;
  for (const candidate of raw) {
    for (const trusted of SMS_TRUSTED_SENDERS) {
      const t = normalizeSender(trusted);
      if (!t) continue;
      if (candidate === t) return true;
      if (/^[0-9]+$/.test(t) && candidate.split(' ').includes(t)) return true;
    }
  }
  return false;
}

function normalizePaymentReference(value) {
  return String(value || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, MAX_REFERENCE_LENGTH);
}

function isValidReference(ref) {
  if (!ref || typeof ref !== 'string') return false;
  if (ref.length < MIN_REFERENCE_LENGTH || ref.length > MAX_REFERENCE_LENGTH) return false;
  if (!/^[A-Z0-9]+$/.test(ref)) return false;
  if (/^[0-9]+$/.test(ref) && ref.length < MIN_TELEBIRR_REF_LENGTH) return false;
  return true;
}

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
  return {
    reference: normalizePaymentReference(reference),
    amount: Number.isFinite(amount) ? amount : 0,
    isTrusted: isTrustedSmsSender(r),
    text,
  };
}

async function logAutoActivity(kind, smsId, reference, amount, txId, reason) {
  try {
    await pool.query('INSERT INTO auto_activity (id, kind, sms_id, reference, amount, tx_id, reason, at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)',
      [crypto.randomUUID(), kind, String(smsId||''), reference || '', Number(amount)||0, txId || '', reason || '', now()]);
  } catch (e) { console.error('[AUTO-LOG]', e.message); }
}

function smsMatchesDepositIdentity(smsText, tx) {
  const text = String(smsText || '').toLowerCase();
  if (!text) return false;
  const phone = String(tx.phone_number || '').replace(/\D/g, '');
  if (phone.length >= 4) {
    const tail = phone.slice(-4);
    if (!text.includes(tail) && !text.includes(phone)) return false;
  }
  const fullName = String(tx.full_name || '').trim().toLowerCase();
  if (fullName.length >= 3) {
    const tokens = fullName.split(/\s+/).filter(t => t.length >= 3);
    if (tokens.length && !tokens.some(t => text.includes(t))) return false;
  }
  return true;
}

async function autoApproveMatchingDeposits(smsId, smsRecord, parentTraceId = null) {
  const traceId = parentTraceId || newTraceId();
  await trace(traceId, null, 'auto.sms', 'ok', { detail: { smsId, sender: smsRecord.sender || smsRecord.from } });

  const payment = parsePaymentRecord(smsRecord);
  if (!payment.isTrusted) {
    await trace(traceId, null, 'auto.sender', 'fail', { reason: 'untrusted-sender', reference: payment.reference });
    return { ok: false, reason: 'untrusted-sender', traceId };
  }
  if (!payment.reference) {
    await trace(traceId, null, 'auto.ref', 'fail', { reason: 'no-reference' });
    return { ok: false, reason: 'no-reference', traceId };
  }
  if (!isValidReference(payment.reference)) {
    await trace(traceId, null, 'auto.ref', 'fail', { reason: 'invalid-reference', reference: payment.reference });
    return { ok: false, reason: 'invalid-reference', traceId };
  }

  const smsAt = Number(smsRecord.timestamp || now());
  if (!Number.isFinite(smsAt) || (now() - smsAt) > SMS_MAX_AGE_MS) {
    await trace(traceId, null, 'auto.age', 'fail', { reason: 'stale-sms', reference: payment.reference });
    return { ok: false, reason: 'stale-sms', traceId };
  }
  if (!(payment.amount > 0)) {
    await trace(traceId, null, 'auto.amount', 'fail', { reason: 'no-amount', reference: payment.reference });
    return { ok: false, reason: 'no-amount', traceId };
  }
  if (payment.amount < MIN_DEPOSIT_AMOUNT || payment.amount > MAX_DEPOSIT_AMOUNT) {
    await trace(traceId, null, 'auto.amount', 'fail', { reason: 'amount-out-of-range', reference: payment.reference, amount: payment.amount });
    return { ok: false, reason: 'amount-out-of-range', traceId };
  }

  const used = await pool.query('SELECT tx_id FROM used_references WHERE reference = $1', [payment.reference]);
  if (used.rows.length > 0) {
    await logAutoActivity('skipped', smsId, payment.reference, payment.amount, used.rows[0].tx_id, 'reference-reused');
    await trace(traceId, null, 'auto.reuse', 'fail', { reason: 'reference-reused', reference: payment.reference, txId: used.rows[0].tx_id });
    return { ok: false, reason: 'reference-reused', traceId };
  }

  const lockKey = advisoryKeyForReference(payment.reference);
  const lockClient = await pool.connect();
  try {
    const lockRes = await lockClient.query('SELECT pg_try_advisory_lock($1) AS got', [lockKey]);
    if (!lockRes.rows[0]?.got) {
      await trace(traceId, null, 'auto.lock', 'fail', { reason: 'reference-locked', reference: payment.reference });
      return { ok: false, reason: 'reference-locked', traceId };
    }

    try {
      const recheck = await pool.query('SELECT tx_id FROM used_references WHERE reference = $1', [payment.reference]);
      if (recheck.rows.length > 0) {
        await trace(traceId, null, 'auto.reuse-recheck', 'fail', { reason: 'reference-reused', reference: payment.reference });
        return { ok: false, reason: 'reference-reused', traceId };
      }

      const txRes = await pool.query(
        "SELECT * FROM transactions WHERE status = 'pending' AND type = 'deposit' AND timestamp > $1",
        [now() - DEPOSIT_MAX_AGE_MS]
      );

      const candidates = [];
      for (const tx of txRes.rows) {
        const txRef = normalizePaymentReference(tx.reference);
        if (!txRef) continue;
        if (txRef !== payment.reference) continue;
        const txAmt = Number(tx.amount);
        if (!Number.isFinite(txAmt)) continue;
        if (Math.abs(txAmt - payment.amount) > AUTO_DEPOSIT_AMOUNT_TOLERANCE) continue;
        candidates.push(tx);
      }

      if (candidates.length === 0) {
        await logAutoActivity('skipped', smsId, payment.reference, payment.amount, '', 'no-match');
        await trace(traceId, null, 'auto.match', 'fail', { reason: 'no-match', reference: payment.reference, amount: payment.amount, detail: { scanned: txRes.rows.length } });
        return { ok: false, reason: 'no-match', traceId };
      }
      if (candidates.length > 1) {
        await logAutoActivity('skipped', smsId, payment.reference, payment.amount, '', 'ambiguous-tx');
        await trace(traceId, null, 'auto.match', 'fail', { reason: 'ambiguous-tx', reference: payment.reference, amount: payment.amount, detail: { count: candidates.length } });
        return { ok: false, reason: 'ambiguous-tx', traceId };
      }

      const tx = candidates[0];
      await trace(traceId, tx.username, 'auto.candidate', 'ok', { txId: tx.id, reference: payment.reference, amount: payment.amount });

      const owner = await pool.query('SELECT blocked, fraud_status, account_status FROM users WHERE username = $1', [tx.wallet_uid || tx.uid]);
      if (owner.rows.length === 0) {
        await logAutoActivity('skipped', smsId, payment.reference, payment.amount, tx.id, 'owner-missing');
        await trace(traceId, tx.username, 'auto.owner', 'fail', { txId: tx.id, reason: 'owner-missing' });
        return { ok: false, reason: 'owner-missing', traceId };
      }
      const o = owner.rows[0];
      if (o.blocked) {
        await logAutoActivity('skipped', smsId, payment.reference, payment.amount, tx.id, 'owner-blocked');
        await trace(traceId, tx.username, 'auto.owner', 'fail', { txId: tx.id, reason: 'owner-blocked' });
        return { ok: false, reason: 'owner-blocked', traceId };
      }
      if (String(o.fraud_status || '').toLowerCase() === 'frozen') {
        await logAutoActivity('skipped', smsId, payment.reference, payment.amount, tx.id, 'owner-frozen');
        await trace(traceId, tx.username, 'auto.owner', 'fail', { txId: tx.id, reason: 'owner-frozen' });
        return { ok: false, reason: 'owner-frozen', traceId };
      }

      if (!smsMatchesDepositIdentity(payment.text, tx)) {
        await logAutoActivity('skipped', smsId, payment.reference, payment.amount, tx.id, 'identity-mismatch');
        await trace(traceId, tx.username, 'auto.identity', 'fail', { txId: tx.id, reason: 'identity-mismatch' });
        return { ok: false, reason: 'identity-mismatch', traceId };
      }

      const reserve = await pool.query(
        `INSERT INTO used_references (reference, sms_id, tx_id, amount, sender, username, consumed_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7)
         ON CONFLICT (reference) DO NOTHING
         RETURNING reference`,
        [payment.reference, smsId, tx.id, payment.amount, String(smsRecord.sender || smsRecord.from || ''), tx.username, now()]
      );
      if (reserve.rowCount === 0) {
        await logAutoActivity('skipped', smsId, payment.reference, payment.amount, tx.id, 'reference-reused');
        await trace(traceId, tx.username, 'auto.reserve', 'fail', { txId: tx.id, reason: 'reference-reused' });
        return { ok: false, reason: 'reference-reused', traceId };
      }
      await trace(traceId, tx.username, 'auto.reserve', 'ok', { txId: tx.id, reference: payment.reference });

      const result = await adminDecision(tx.id, 'approved', `Auto-approved: SMS ${String(smsId).slice(0, 80)} (ref ${payment.reference})`);
      if (!result.ok) {
        await pool.query('DELETE FROM used_references WHERE reference = $1 AND tx_id = $2', [payment.reference, tx.id]);
        await logAutoActivity('error', smsId, payment.reference, payment.amount, tx.id, result.reason);
        await trace(traceId, tx.username, 'auto.decision', 'fail', { txId: tx.id, reason: result.reason });
        return { ok: false, reason: result.reason, traceId };
      }

      await logAutoActivity('approved', smsId, payment.reference, payment.amount, tx.id, 'verified');
      await trace(traceId, tx.username, 'auto.decision', 'ok', { txId: tx.id, reference: payment.reference, amount: payment.amount });
      return { ok: true, txId: tx.id, tx: result.tx, traceId };
    } finally {
      try { await lockClient.query('SELECT pg_advisory_unlock($1)', [lockKey]); } catch {}
    }
  } finally {
    lockClient.release();
  }
}

function advisoryKeyForReference(ref) {
  const h = crypto.createHash('sha256').update('ref:' + String(ref || '')).digest();
  const n = h.readUIntBE(0, 6);
  return Number(n);
}

async function processIncomingSmsSnapshot(row, parentTraceId = null) {
  if (row.processed) return;
  const traceId = parentTraceId || newTraceId();
  try {
    const res = await autoApproveMatchingDeposits(row.id, row, traceId);
    const definitive = res.ok || ['untrusted-sender', 'no-reference', 'invalid-reference',
      'no-amount', 'amount-out-of-range', 'stale-sms', 'reference-reused'].includes(res.reason);
    if (definitive) {
      try {
        await pool.query(
          'UPDATE incoming_sms SET processed = true, matched_tx_id = COALESCE($1, matched_tx_id) WHERE id = $2',
          [res.ok ? res.txId : null, row.id]
        );
      } catch (updErr) {
        try { await pool.query('UPDATE incoming_sms SET processed = true WHERE id = $1', [row.id]); } catch (_) {}
      }
      await trace(traceId, null, 'auto.sms-processed', res.ok ? 'ok' : 'skipped', {
        detail: { smsId: row.id, reason: res.reason || null }
      });
    } else {
      await trace(traceId, null, 'auto.sms-queued', 'warn', {
        reason: res.reason || 'retry-later',
        detail: { smsId: row.id }
      });
    }
  } catch (e) {
    console.error('[AUTO-DEPOSIT]', e);
    await trace(traceId, null, 'auto.sms-error', 'fail', { reason: e.message, detail: { smsId: row.id } });
  }
}

async function rescanPendingSms(parentTraceId = null) {
  const traceId = parentTraceId || newTraceId();
  try {
    const res = await pool.query('SELECT * FROM incoming_sms WHERE processed = false ORDER BY timestamp ASC LIMIT 50');
    if (res.rows.length > 0) {
      await trace(traceId, null, 'rescan', 'ok', { detail: { queued: res.rows.length } });
    }
    for (const row of res.rows) {
      await processIncomingSmsSnapshot(row, traceId);
    }
  } catch (e) {
    console.error('[RESCAN-SMS]', e.message);
    await trace(traceId, null, 'rescan', 'fail', { reason: e.message });
  }
}

function startAutomaticDepositMonitor() {
  setInterval(async () => {
    try {
      const res = await pool.query('SELECT * FROM incoming_sms WHERE processed = false ORDER BY timestamp ASC LIMIT 10');
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

    await client.query(
      'INSERT INTO users (username, display_name, created_at, updated_at) VALUES ($1,$1,$2,$2) ON CONFLICT (username) DO NOTHING',
      [ref, now()]
    );
    await client.query(
      `UPDATE users
          SET main_wallet = COALESCE(main_wallet,0) + $1,
              referral_earnings = COALESCE(referral_earnings,0) + $1,
              wallet_updated_at = $2,
              updated_at = $2
        WHERE username = $3`,
      [amount, now(), ref]
    );
    await client.query('INSERT INTO notifications (id, username, type, read, timestamp) VALUES ($1,$2,$3,true,$4)', [key, ref, 'referral_marker', now()]);

    await client.query('COMMIT');
    await addNotification(ref, {
      type: 'referral_commission',
      message: `🎁 You earned ${amount} Birr commission from a win.`
    });
    await sendWallet(ref);
    console.log(`[REFERRAL] credited ${amount} Birr to ${ref} from winner ${winnerUid} (room=${roomId} round=${round})`);
    return true;
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('[REFERRAL] error:', e.message, { ref, amount, roomId, round, winnerUid });
    return false;
  } finally {
    client.release();
  }
}

async function recordHumanWinner({ username, displayName, roomId, round, winAmount, winningNumber, prizePool }) {
  try {
    await pool.query(
      `INSERT INTO game_winners (id, username, display_name, room_id, round, win_amount, winning_number, prize_pool, created_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
       ON CONFLICT (id) DO NOTHING`,
      [
        `win_${roomId}_${round}_${username}`,
        username,
        displayName || username,
        roomId,
        round,
        winAmount,
        winningNumber,
        prizePool,
        now()
      ]
    );
    console.log(`[WINNER-RECORD] ${username} (${displayName}) won ${winAmount} in ${roomId} round ${round}`);
  } catch (e) {
    console.error('[WINNER-RECORD] failed:', e.message, { username, roomId, round, winAmount });
  }
}

// ✅ NEW: persist per-round human participation snapshot
async function recordRoundSnapshot({
  roomId, round, bet, humanCount, humanPicks, botCount, botPicks, totalPicks,
  prizePool, winningNumber, winnerCount, humanWinnerCount, humanWinAmount,
  humanPlayerIds, startedAt, endedAt,
}) {
  try {
    const id = `snap_${roomId}_${round}`;
    await pool.query(
      `INSERT INTO round_snapshots (
         id, room_id, round, bet, human_count, human_picks, bot_count, bot_picks,
         total_picks, prize_pool, winning_number, winner_count, human_winner_count,
         human_win_amount, human_player_ids, started_at, ended_at, created_at
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
       ON CONFLICT (id) DO UPDATE SET
         human_count = EXCLUDED.human_count,
         human_picks = EXCLUDED.human_picks,
         bot_count = EXCLUDED.bot_count,
         bot_picks = EXCLUDED.bot_picks,
         total_picks = EXCLUDED.total_picks,
         prize_pool = EXCLUDED.prize_pool,
         winning_number = EXCLUDED.winning_number,
         winner_count = EXCLUDED.winner_count,
         human_winner_count = EXCLUDED.human_winner_count,
         human_win_amount = EXCLUDED.human_win_amount,
         human_player_ids = EXCLUDED.human_player_ids,
         ended_at = EXCLUDED.ended_at`,
      [
        id, roomId, round, bet, humanCount, humanPicks, botCount, botPicks,
        totalPicks, prizePool, winningNumber, winnerCount, humanWinnerCount,
        humanWinAmount, JSON.stringify(humanPlayerIds || []), startedAt, endedAt, now(),
      ]
    );
    console.log(`[ROUND-SNAPSHOT] ${roomId} r${round}: ${humanCount} humans, ${humanPicks} picks, ${winnerCount} winners`);
  } catch (e) {
    console.error('[ROUND-SNAPSHOT] failed:', e.message, { roomId, round });
  }
}

// ============================================================================
// GAME ROOM
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
    this.roundStartedAt = now();
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

  // ✅ NEW: capture human/bot breakdown for this round
  const humans = allWithPicks.filter(p => !p.isBot);
  const bots = allWithPicks.filter(p => p.isBot);
  const humanCount = humans.length;
  const humanPicks = humans.reduce((s, p) => s + p.picks.length, 0);
  const botCount = bots.length;
  const botPicks = bots.reduce((s, p) => s + p.picks.length, 0);
  const humanPlayerIds = humans.map(p => p.uid);
  const humanWinners = winners.filter(uid => !room.players.get(uid)?.isBot);
  const humanWinAmount = winners.length && prizePool > 0
    ? Math.floor(prizePool / winners.length) * humanWinners.length
    : 0;
  const roundStartedAt = room.roundStartedAt || (now() - SELECTION_SECONDS * 1000);

  room.winningNumber = winningNumber; room.winners = winners; room.prizePool = prizePool;
  room.phase = 'spinning'; room.endsAt = now() + SPINNING_SECONDS * 1000; room.dirty = true;

  if (winners.length && prizePool > 0) {
    const share = Math.floor(prizePool / winners.length);
    for (const uid of winners) {
      if (share <= 0) break;
      if (!(await claimPayout(uid, share, room.id, room.round, 'win'))) continue;

      const player = room.players.get(uid);
      if (player?.isBot) continue; // bots: no record, no notification, no referral

      const displayName = player?.displayName || uid;

      await recordHumanWinner({
        username: uid,
        displayName,
        roomId: room.id,
        round: room.round,
        winAmount: share,
        winningNumber,
        prizePool
      });

      try {
        await addNotification(uid, { message: `🏆 You won ${share} Birr in round ${room.round}!` });
      } catch (e) {
        console.error('[WIN-NOTIFY]', e.message, { uid });
      }

      try {
        const refRes = await pool.query('SELECT referred_by FROM users WHERE username = $1', [uid]);
        const refBy = refRes.rows[0]?.referred_by;
        if (refBy && refBy !== uid) {
          const commission = Math.floor(share * REFERRAL_RATE);
          if (commission > 0) {
            await creditReferral(refBy, commission, room.id, room.round, uid);
          }
        }
      } catch (e) {
        console.error('[REFERRAL] credit failed:', e.message, { uid, share });
      }
    }
  }

  // ✅ NEW: persist the round snapshot (all humans, not just winners)
  try {
    await recordRoundSnapshot({
      roomId: room.id,
      round: room.round,
      bet: room.bet,
      humanCount,
      humanPicks,
      botCount,
      botPicks,
      totalPicks: totalPicksInRoom,
      prizePool,
      winningNumber,
      winnerCount: winners.length,
      humanWinnerCount: humanWinners.length,
      humanWinAmount,
      humanPlayerIds,
      startedAt: roundStartedAt,
      endedAt: now(),
    });
  } catch (e) {
    console.error('[ROUND-SNAPSHOT] persist failed:', e.message);
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
  room.roundStartedAt = now(); // ✅ NEW
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
// WEBSOCKET SETUP
// ============================================================================
const conns = new Map();
const wss = new WebSocketServer({ noServer: true, maxPayload: WS_MAX_PAYLOAD });
function send(ws, obj) { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); }

wss.on('connection', (ws) => {
  ws.isAlive = true; ws.authenticatedAt = null;
  ws.authTimer = setTimeout(() => { if (!ws.authed) { try { ws.close(4001, 'authentication timeout'); } catch {} } }, 10000);
  ws.uid = null; ws.roomId = null; ws.authed = false; ws.isAdmin = false;

  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', async raw => {
    const size = raw?.length || raw?.byteLength || 0;
    let m;
    try { m = JSON.parse(raw.toString()); }
    catch (e) {
      console.warn(`[WS-PARSE] uid=${ws.uid || '?'} size=${size} err=${e.message}`);
      return;
    }
    try { await handle(ws, m); }
    catch (e) { console.error('[WS-MSG]', { type: m?.type, err: e.message }); send(ws, { type: 'error', message: 'server-error' }); }
  });

  ws.on('close', async (code, reason) => {
    clearTimeout(ws.authTimer);
    if (ws.uid) console.log(`[WS-CLOSE] uid=${ws.uid} code=${code} reason=${String(reason || '').slice(0, 80)}`);
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
  ws.on('error', (err) => console.warn('[WS-ERROR]', err.message));
});

async function handle(ws, m) {
  if (m.type === 'ping') return send(ws, { type: 'pong', t: now() });

  if (m.type === 'auth') {
    const verification = validateInitData(m.initData || '');
    if (!verification.ok) {
      send(ws, { type: 'error', message: 'auth-failed', reason: verification.reason || 'unknown' });
      if (verification.reason === 'expired') {
        try { ws.close(4001, 'auth-expired'); } catch {}
      }
      return;
    }

    const telegramUser = verification.user;
    const resolved = await resolveTelegramUser(telegramUser, m.ref || null);
    if (!resolved?.uid) return send(ws, { type: 'error', message: 'auth-failed', reason: 'identity-failed' });

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
    const traceId = newTraceId();
    const imgLen = String(m.imageData || '').length;
    console.log(`[WS-TX] trace=${traceId} uid=${ws.uid} type=${m.txType} amount=${m.amount} ref=${String(m.reference||'').slice(0,20)} imgLen=${imgLen} clientRequestId=${String(m.clientRequestId||'').slice(0,32)}`);
    await trace(traceId, ws.uid, 'ws.arrived', 'ok', {
      type: m.txType, amount: m.amount, reference: normalizePaymentReference(m.reference),
      clientRequestId: m.clientRequestId,
      detail: { imgLen }
    });

    if (imgLen > MAX_IMAGE_BYTES) {
      await trace(traceId, ws.uid, 'ws.image-guard', 'fail', { type: m.txType, reason: 'image-too-large', detail: { imgLen, max: MAX_IMAGE_BYTES } });
      send(ws, { type: 'error', message: 'image-too-large' });
      send(ws, { type: 'transaction-rejected', reason: 'image-too-large', traceId });
      return;
    }

    let r;
    try { r = await transactionRequest(ws.uid, { ...m, __traceId: traceId }); }
    catch (e) {
      console.error('[WS-TX] uncaught:', e.message);
      await trace(traceId, ws.uid, 'ws.uncaught', 'fail', { type: m.txType, reason: e.message });
      send(ws, { type: 'error', message: 'server-error' });
      send(ws, { type: 'transaction-rejected', reason: 'server-error', traceId });
      return;
    }

    if (!r.ok) {
      if (r.reason === 'reference-already-used') {
        try {
          await addNotification(ws.uid, {
            type: 'notification',
            message: 'አገልግሎት ላይ የዋለ Reference code ተጠቅመዋል። እባክዎ በድጋሚ ይሞክሩ።'
          });
        } catch (e) {
          console.error('[TX-USED-REF-MSG]', e.message);
        }
      }
      send(ws, { type: 'error', message: r.reason });
      send(ws, { type: 'transaction-rejected', reason: r.reason, traceId });
      return;
    }
    send(ws, { type: 'transaction-created', id: r.id, clientRequestId: r.clientRequestId || null, traceId });
    try { await sendWallet(ws.uid); } catch (_) {}
    return;
  }

  if (m.type === 'transaction-image') {
    const imgLen = String(m.imageData || '').length;
    if (imgLen > MAX_IMAGE_BYTES) return send(ws, { type: 'error', message: 'image-too-large' });
    await transactionImage(ws.uid, String(m.id || ''), String(m.imageData || ''));
    return;
  }
  if (m.type === 'notification-read') { await markNotificationRead(ws.uid, String(m.id || '')); return; }
}

setInterval(() => { for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; try { ws.ping(); } catch {} } }, 60000);

// ============================================================================
// EXPRESS API
// ============================================================================
const app = express();

app.post('/api/sms/incoming', express.json(), async (req, res) => {
  const providedSecret = req.headers['x-sms-secret'];
  if (!providedSecret || providedSecret !== SMS_WEBHOOK_SECRET) {
    console.warn('[SECURITY] Unauthorized SMS attempt. Header received:', providedSecret);
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }

  const traceId = newTraceId();
  try {
    const { from, text } = req.body || {};
    if (!text) {
      await trace(traceId, null, 'sms.validate', 'fail', { reason: 'missing-text' });
      return res.status(400).json({ ok: false, error: 'Missing text' });
    }

    const id = crypto.randomUUID();
    const timestamp = now();
    const textHash = crypto.createHash('sha256').update(String(from || '') + '|' + String(text)).digest('hex');

    await trace(traceId, null, 'sms.received', 'ok', {
      reference: '', detail: { smsId: id, sender: from, textLen: String(text).length }
    });

    try {
      const dupe = await pool.query(
        'SELECT id FROM incoming_sms WHERE text_hash = $1 AND timestamp > $2 LIMIT 1',
        [textHash, timestamp - DUPLICATE_SMS_WINDOW_MS]
      );
      if (dupe.rows.length > 0) {
        await logAutoActivity('skipped', id, '', 0, '', 'duplicate-sms');
        await trace(traceId, null, 'sms.dedupe', 'skipped', { detail: { smsId: id, originalId: dupe.rows[0].id } });
        return res.json({ ok: true, received: true, duplicate: true, matched: false });
      }
    } catch (dedupeErr) {
      console.warn('[SMS] dedupe skipped:', dedupeErr.message);
    }

    try {
      await pool.query(
        'INSERT INTO incoming_sms (id, sender, text, timestamp, processed, text_hash) VALUES ($1,$2,$3,$4,false,$5)',
        [id, from || 'Unknown', text, timestamp, textHash]
      );
    } catch (insertErr) {
      console.warn('[SMS] extended insert failed, falling back:', insertErr.message);
      await pool.query(
        'INSERT INTO incoming_sms (id, sender, text, timestamp, processed) VALUES ($1,$2,$3,$4,false)',
        [id, from || 'Unknown', text, timestamp]
      );
    }
    await trace(traceId, null, 'sms.persisted', 'ok', { detail: { smsId: id } });

    console.log(`[SMS] Received securely from ${from}: ${String(text).substring(0, 30)}...`);

    const smsRecord = { id, sender: from, text, timestamp };
    const matchResult = await autoApproveMatchingDeposits(id, smsRecord, traceId);

    const definitive = matchResult.ok || ['untrusted-sender', 'no-reference', 'invalid-reference',
      'no-amount', 'amount-out-of-range', 'stale-sms', 'reference-reused'].includes(matchResult.reason);
    if (definitive) {
      try {
        await pool.query(
          'UPDATE incoming_sms SET processed = true, matched_tx_id = COALESCE($1, matched_tx_id) WHERE id = $2',
          [matchResult.ok ? matchResult.txId : null, id]
        );
      } catch (updErr) {
        try { await pool.query('UPDATE incoming_sms SET processed = true WHERE id = $1', [id]); } catch (_) {}
      }
    }

    return res.json({
      ok: true,
      received: true,
      matched: !!matchResult.ok,
      reason: matchResult.reason || null,
      traceId,
    });
  } catch (e) {
    console.error('[SMS API ERROR]', e);
    await trace(traceId, null, 'sms.error', 'fail', { reason: e.message });
    return res.status(200).json({ ok: false, error: e.message, traceId });
  }
});

app.use(express.json({ limit: '3mb' }));
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, X-Admin-Key, x-sms-secret');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

app.get('/', (_, res) => res.send('Winzo Server is running. Database is PostgreSQL.'));
app.get('/health', (_, res) => res.status(200).send('OK'));

function isAdmin(req) { return ADMIN_KEY && constantTimeEqual(String(req.get('x-admin-key') || ''), ADMIN_KEY); }

app.get('/user/notifications', async (req, res) => {
  const uid = String(req.query.uid || '').trim();
  if (!uid) return res.status(400).json({ ok: false, error: 'uid required' });
  try {
    const r = await pool.query(
      'SELECT id, type, message, status, tx_type, tx_id, timestamp, read FROM notifications WHERE username = $1 ORDER BY timestamp DESC LIMIT 50',
      [uid]
    );
    res.json({
      ok: true,
      notifications: r.rows.map(n => ({
        id: n.id,
        type: n.type,
        message: n.message,
        status: n.status,
        txType: n.tx_type,
        txId: n.tx_id,
        timestamp: Number(n.timestamp),
        read: n.read
      }))
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

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
    await logAutoActivity(decision === 'approved' ? 'admin-approved' : 'admin-rejected', '', '', 0, id, 'http-admin');
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

app.get('/admin/user-details', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  const uid = String(req.query.uid || '').trim();
  if (!uid) return res.status(400).json({ ok: false, error: 'uid required' });
  try {
    const userRes = await pool.query('SELECT * FROM users WHERE username = $1', [uid]);
    if (userRes.rows.length === 0) return res.status(404).json({ ok: false, error: 'User not found' });
    const user = userRes.rows[0];

    const refsRes = await pool.query('SELECT username, display_name FROM users WHERE referred_by = $1', [uid]);

    res.json({
      ok: true,
      user: {
        uid: user.username,
        name: user.display_name,
        telegram_id: user.telegram_id,
        main: Number(user.main_wallet),
        play: Number(user.play_wallet),
        pending: Number(user.pending_withdrawal),
        referral_earnings: Number(user.referral_earnings),
        referred_by: user.referred_by,
        blocked: user.blocked,
        block_reason: user.block_reason,
        created_at: Number(user.created_at),
        referrals: refsRes.rows.map(r => ({ uid: r.username, name: r.display_name }))
      }
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post('/admin/block-user', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  const { userId, blocked, reason } = req.body;
  if (!userId) return res.status(400).json({ ok: false, error: 'userId required' });
  try {
    const isBlocked = Boolean(blocked);
    const blockReason = String(reason || 'Blocked by admin').slice(0, 256);
    const blockAt = isBlocked ? Date.now() : null;
    await pool.query(
      'UPDATE users SET blocked = $1, block_reason = $2, blocked_at = $3, updated_at = $4 WHERE username = $5',
      [isBlocked, isBlocked ? blockReason : null, blockAt, Date.now(), userId]
    );
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post('/admin/send-message', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  const { userId, message } = req.body;
  if (!userId || !message) return res.status(400).json({ ok: false, error: 'userId and message required' });
  try {
    await addNotification(userId, {
      type: 'admin_message',
      message: String(message),
      status: 'info',
      from: 'admin'
    });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/admin/user-transactions', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  const uid = String(req.query.uid || '').trim();
  if (!uid) return res.status(400).json({ ok: false, error: 'uid required' });
  try {
    const r = await pool.query(
      `SELECT id, type, amount, status, timestamp, reference, reason, wallet_applied
       FROM transactions WHERE username = $1 OR wallet_uid = $1
       ORDER BY timestamp DESC LIMIT 100`,
      [uid]
    );
    res.json({
      ok: true,
      transactions: r.rows.map(t => ({
        id: t.id,
        type: t.type,
        amount: Number(t.amount),
        status: t.status,
        timestamp: Number(t.timestamp),
        reference: t.reference,
        reason: t.reason,
        wallet_applied: Number(t.wallet_applied)
      }))
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/admin/winners', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  try {
    const limit = Math.min(Number(req.query.limit) || 100, 500);
    const username = String(req.query.username || '').trim();
    const roomId = String(req.query.roomId || '').trim();

    const conds = [];
    const vals = [];
    let i = 1;
    if (username) { conds.push(`username = $${i++}`); vals.push(username); }
    if (roomId) { conds.push(`room_id = $${i++}`); vals.push(roomId); }

    vals.push(limit);
    const sql = `
      SELECT id, username, display_name, room_id, round, win_amount, winning_number, prize_pool, created_at
      FROM game_winners
      ${conds.length ? 'WHERE ' + conds.join(' AND ') : ''}
      ORDER BY created_at DESC
      LIMIT $${i}
    `;
    const r = await pool.query(sql, vals);
    res.json({
      ok: true,
      count: r.rows.length,
      winners: r.rows.map(w => ({
        id: w.id,
        username: w.username,
        displayName: w.display_name,
        roomId: w.room_id,
        round: Number(w.round),
        winAmount: Number(w.win_amount),
        winningNumber: w.winning_number == null ? null : Number(w.winning_number),
        prizePool: Number(w.prize_pool || 0),
        createdAt: Number(w.created_at)
      }))
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post('/admin/auto-match', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  try {
    const smsRes = await pool.query('SELECT * FROM incoming_sms WHERE processed = false ORDER BY timestamp ASC');
    let approved = 0, skipped = 0, errors = 0;
    for (const row of smsRes.rows) {
      try {
        const before = await pool.query('SELECT processed FROM incoming_sms WHERE id = $1', [row.id]);
        await processIncomingSmsSnapshot(row);
        const after = await pool.query('SELECT processed, matched_tx_id FROM incoming_sms WHERE id = $1', [row.id]);
        if (after.rows[0]?.matched_tx_id) approved++;
        else if (after.rows[0]?.processed && before.rows[0]?.processed === false) skipped++;
      } catch (e) {
        errors++;
        await logAutoActivity('error', row.id, '', 0, '', e.message);
      }
    }
    res.json({ ok: true, approved, skipped, errors, scanned: smsRes.rows.length });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/admin/sms', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  try {
    const smsRes = await pool.query('SELECT * FROM incoming_sms ORDER BY timestamp DESC LIMIT 50');
    const data = {};
    smsRes.rows.forEach(r => {
      data[r.id] = { sender: r.sender, text: r.text, timestamp: Number(r.timestamp) };
    });
    res.json({ ok: true, data });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/admin/used-references', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  try {
    const r = await pool.query('SELECT * FROM used_references ORDER BY consumed_at DESC LIMIT 100');
    res.json({ ok: true, references: r.rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/admin/activity', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  try {
    const r = await pool.query('SELECT * FROM auto_activity ORDER BY at DESC LIMIT 100');
    res.json({ ok: true, activity: r.rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/admin/all-transactions', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  try {
    const limit = Math.min(Number(req.query.limit) || 100, 500);
    const r = await pool.query(
      `SELECT id, username, type, amount, status, timestamp, reference, reason, client_request_id, updated_at
       FROM transactions ORDER BY timestamp DESC LIMIT $1`,
      [limit]
    );
    res.json({
      ok: true,
      transactions: r.rows.map(t => ({
        ...t,
        amount: Number(t.amount),
        timestamp: Number(t.timestamp),
        updated_at: Number(t.updated_at || 0),
      }))
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/admin/trace', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  try {
    const traceId = String(req.query.traceId || '').trim();
    const uid = String(req.query.uid || '').trim();
    const clientRequestId = String(req.query.clientRequestId || '').trim();
    const txId = String(req.query.txId || '').trim();
    const limit = Math.min(Number(req.query.limit) || 200, 1000);

    const conds = [];
    const vals = [];
    let i = 1;
    if (traceId) { conds.push(`trace_id = $${i++}`); vals.push(traceId); }
    if (uid) { conds.push(`uid = $${i++}`); vals.push(uid); }
    if (clientRequestId) { conds.push(`client_request_id = $${i++}`); vals.push(clientRequestId); }
    if (txId) { conds.push(`tx_id = $${i++}`); vals.push(txId); }

    if (!conds.length) {
      return res.status(400).json({ ok: false, error: 'must supply traceId, uid, clientRequestId, or txId' });
    }
    vals.push(limit);
    const r = await pool.query(
      `SELECT * FROM request_trace WHERE ${conds.join(' OR ')} ORDER BY at ASC LIMIT $${i}`,
      vals
    );
    res.json({
      ok: true,
      count: r.rows.length,
      trace: r.rows.map(t => ({
        ...t,
        amount: t.amount == null ? null : Number(t.amount),
        at: Number(t.at),
      })),
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ============================================================================
// ANALYTICS ENDPOINTS
// ============================================================================

function dayRangeToMs(fromDate, toDate) {
  const startMs = Date.parse(fromDate + 'T00:00:00Z');
  const endMs   = Date.parse(toDate   + 'T00:00:00Z') + 24 * 60 * 60 * 1000;
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || startMs >= endMs) return null;
  return { startMs, endMs };
}

app.get('/admin/analytics/games', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  try {
    const from = String(req.query.from || '').trim();
    const to   = String(req.query.to   || '').trim();
    const room = String(req.query.room || 'both').trim();
    const rng  = dayRangeToMs(from, to);
    if (!rng) return res.status(400).json({ ok: false, error: 'invalid-date-range' });

    const roomsWanted = room === 'both' ? ['room_15', 'room_30'] : [room];
    if (!roomsWanted.every(r => r === 'room_15' || r === 'room_30')) {
      return res.status(400).json({ ok: false, error: 'invalid-room' });
    }

    // Pull round snapshots in range
    const snapRes = await pool.query(
      `SELECT room_id, round, bet, human_count, human_picks, bot_count, bot_picks,
              total_picks, prize_pool, winning_number, winner_count, human_winner_count,
              human_win_amount, started_at, ended_at
         FROM round_snapshots
        WHERE ended_at >= $1 AND ended_at < $2
          AND room_id = ANY($3::text[])
        ORDER BY ended_at ASC`,
      [rng.startMs, rng.endMs, roomsWanted]
    );

    // Hourly buckets (UTC, same as server-side timestamps)
    const buckets = Array.from({ length: 24 }, (_, h) => ({
      hour: h, games: 0, totalHumans: 0, totalHumanPicks: 0,
      totalBots: 0, totalPicks: 0, totalPrizePool: 0,
      totalWinners: 0, humanWinners: 0, humanWinAmount: 0,
    }));

    for (const row of snapRes.rows) {
      const h = new Date(Number(row.ended_at)).getUTCHours();
      buckets[h].games += 1;
      buckets[h].totalHumans += Number(row.human_count || 0);
      buckets[h].totalHumanPicks += Number(row.human_picks || 0);
      buckets[h].totalBots += Number(row.bot_count || 0);
      buckets[h].totalPicks += Number(row.total_picks || 0);
      buckets[h].totalPrizePool += Number(row.prize_pool || 0);
      buckets[h].totalWinners += Number(row.winner_count || 0);
      buckets[h].humanWinners += Number(row.human_winner_count || 0);
      buckets[h].humanWinAmount += Number(row.human_win_amount || 0);
    }

    const hourly = buckets.map(b => ({
      hour: b.hour,
      games: b.games,
      avgHumans: b.games ? b.totalHumans / b.games : 0,
      totalHumans: b.totalHumans,
      totalHumanPicks: b.totalHumanPicks,
      totalBots: b.totalBots,
      totalPicks: b.totalPicks,
      avgPrizePool: b.games ? b.totalPrizePool / b.games : 0,
      totalWinners: b.totalWinners,
      humanWinners: b.humanWinners,
      humanWinAmount: b.humanWinAmount,
    }));

    const hoursWithData = hourly.filter(h => h.games > 0).length;
    const daily = hoursWithData
      ? hourly.reduce((s, h) => s + h.avgHumans, 0) / hoursWithData
      : 0;
    const peak = hourly.reduce((m, h) => Math.max(m, h.avgHumans), 0);
    const totalGames = hourly.reduce((s, h) => s + h.games, 0);
    const totalHumans = hourly.reduce((s, h) => s + h.totalHumans, 0);
    const totalHumanPicks = hourly.reduce((s, h) => s + h.totalHumanPicks, 0);
    const totalBots = hourly.reduce((s, h) => s + h.totalBots, 0);
    const totalWinners = hourly.reduce((s, h) => s + h.totalWinners, 0);
    const humanWinners = hourly.reduce((s, h) => s + h.humanWinners, 0);
    const humanWinAmount = hourly.reduce((s, h) => s + h.humanWinAmount, 0);

    res.json({
      ok: true,
      from, to, room,
      summary: {
        dailyAvgHumansPerGame: daily,
        peakHourlyAvg: peak,
        gamesRecorded: totalGames,
        hoursWithData,
        totalHumanParticipations: totalHumans,
        totalHumanPicks,
        totalBots,
        totalWinners,
        humanWinners,
        humanWinAmount,
      },
      hourly,
    });
  } catch (e) {
    console.error('[ANALYTICS/games]', e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/admin/analytics/referrals', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  try {
    const refsRes = await pool.query(
      `SELECT u.username AS referred_uid, u.display_name AS referred_name,
              u.referred_by AS referrer_uid,
              COALESCE(u.referral_earnings, 0) AS referred_ref_earnings,
              COALESCE(u.created_at, 0) AS referred_created_at,
              r.display_name AS referrer_name,
              COALESCE(r.referral_earnings, 0) AS referrer_earnings
         FROM users u
         LEFT JOIN users r ON r.username = u.referred_by
        WHERE u.referred_by IS NOT NULL AND u.referred_by <> ''`
    );

    const referrers = new Map();
    for (const row of refsRes.rows) {
      const rid = row.referrer_uid;
      if (!rid) continue;
      if (!referrers.has(rid)) {
        referrers.set(rid, {
          uid: rid,
          name: row.referrer_name || rid,
          referralEarnings: Number(row.referrer_earnings || 0),
          referred: [],
        });
      }
      const r = referrers.get(rid);
      r.referred.push({
        uid: row.referred_uid,
        name: row.referred_name || row.referred_uid,
        earnings: Number(row.referred_ref_earnings || 0),
        createdAt: Number(row.referred_created_at || 0),
      });
    }

    const list = [...referrers.values()].map(r => ({
      ...r,
      referralCount: r.referred.length,
      referralEarnings: r.referralEarnings ||
        r.referred.reduce((s, x) => s + x.earnings, 0),
    }));

    list.sort((a, b) => b.referralCount - a.referralCount);

    const totals = {
      referrers: list.length,
      referredUsers: list.reduce((s, r) => s + r.referralCount, 0),
      totalEarnings: list.reduce((s, r) => s + r.referralEarnings, 0),
      avgPerReferrer: list.length
        ? list.reduce((s, r) => s + r.referralCount, 0) / list.length
        : 0,
      topReferrer: list[0] || null,
      topEarner: [...list].sort((a, b) => b.referralEarnings - a.referralEarnings)[0] || null,
    };

    res.json({ ok: true, totals, referrers: list });
  } catch (e) {
    console.error('[ANALYTICS/referrals]', e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/admin/diagnostics', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  try {
    const since = now() - 60 * 60 * 1000;

    const [pendingTx, unprocessedSms, approvedRefs, traced24h, failedStages] = await Promise.all([
      pool.query("SELECT COUNT(*)::int AS c FROM transactions WHERE status = 'pending'"),
      pool.query("SELECT COUNT(*)::int AS c FROM incoming_sms WHERE processed = false"),
      pool.query("SELECT COUNT(*)::int AS c FROM used_references"),
      pool.query("SELECT COUNT(*)::int AS c FROM request_trace WHERE at > $1", [now() - DAY]),
      pool.query(
        `SELECT stage, status, COUNT(*)::int AS c
         FROM request_trace WHERE at > $1 AND status IN ('fail','error')
         GROUP BY stage, status ORDER BY c DESC LIMIT 20`,
        [since]
      ),
    ]);

    const stuck = await pool.query(
      `SELECT t.id, t.username, t.amount, t.reference, t.timestamp
       FROM transactions t
       LEFT JOIN used_references u
         ON UPPER(REGEXP_REPLACE(t.reference, '[^A-Za-z0-9]', '', 'g')) = u.reference
       WHERE t.status = 'pending' AND t.type = 'deposit' AND t.timestamp < $1 AND u.reference IS NULL
       ORDER BY t.timestamp DESC LIMIT 50`,
      [now() - 30 * 60 * 1000]
    );

    const smsQueue = await pool.query(
      `SELECT id, sender, LEFT(text, 80) AS preview, timestamp
       FROM incoming_sms WHERE processed = false ORDER BY timestamp ASC LIMIT 20`
    );

    const recentFails = await pool.query(
      `SELECT trace_id, uid, stage, status, reason, at
       FROM request_trace WHERE at > $1 AND status IN ('fail','error')
       ORDER BY at DESC LIMIT 30`,
      [now() - DAY]
    );

    res.json({
      ok: true,
      summary: {
        pendingTransactions: pendingTx.rows[0].c,
        unprocessedSms: unprocessedSms.rows[0].c,
        usedReferences: approvedRefs.rows[0].c,
        traced24h: traced24h.rows[0].c,
        signupBonus: SIGNUP_BONUS,
      },
      failedStagesLastHour: failedStages.rows,
      stuckDeposits: stuck.rows.map(r => ({ ...r, amount: Number(r.amount), timestamp: Number(r.timestamp) })),
      smsQueue: smsQueue.rows.map(r => ({ ...r, timestamp: Number(r.timestamp) })),
      recentFails: recentFails.rows.map(r => ({ ...r, at: Number(r.at) })),
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post('/admin/sms/reprocess', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  try {
    const smsId = String(req.body?.smsId || '').trim();
    if (!smsId) return res.status(400).json({ ok: false, error: 'smsId required' });
    const r = await pool.query('SELECT * FROM incoming_sms WHERE id = $1', [smsId]);
    if (r.rows.length === 0) return res.status(404).json({ ok: false, error: 'not-found' });
    await pool.query('UPDATE incoming_sms SET processed = false WHERE id = $1', [smsId]);
    await processIncomingSmsSnapshot(r.rows[0]);
    const after = await pool.query('SELECT processed, matched_tx_id FROM incoming_sms WHERE id = $1', [smsId]);
    res.json({ ok: true, processed: after.rows[0]?.processed, matchedTxId: after.rows[0]?.matched_tx_id || null });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post('/admin/force-approve', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  try {
    const id = String(req.body?.id || '').trim();
    const reason = String(req.body?.reason || 'manual-force-approve').slice(0, 200);
    if (!id) return res.status(400).json({ ok: false, error: 'id required' });
    const r = await adminDecision(id, 'approved', reason);
    await logAutoActivity('admin-forced', '', '', 0, id, reason);
    res.json({ ok: r.ok, transaction: r.tx, error: r.reason });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ============================================================================
// SERVER BOOT
// ============================================================================
const server = http.createServer(app);
server.on('upgrade', (req, socket, head) => {
  wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Server listening on 0.0.0.0:${PORT}`);
  console.log(`🎁 Signup bonus for new users: ${SIGNUP_BONUS} Birr`);
  (async () => {
    try {
      await initDb();
      await loadRooms();
      startAutomaticDepositMonitor();
      for (const room of Object.values(rooms)) {
        if (room.phase === 'selection' && room.endsAt <= now()) { room.endsAt = now() + SELECTION_SECONDS * 1000; room.dirty = true; }
        if (room.phase === 'selection') normalizeRoundBots(room);
        room.persist().catch(err => console.error('Room persist failed:', err));
        scheduleBots(room);
      }
      console.log('✅ Background initialization complete.');
    } catch (err) {
      console.error('⚠️ Background initialization failed (DB may be slow or unreachable):', err);
    }
  })();
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
