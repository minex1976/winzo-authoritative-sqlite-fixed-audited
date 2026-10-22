import pg from 'pg';
const { Pool } = pg;

// Use your External Access credentials if running from your computer
const pool = new Pool({
  host: '196.190.212.135', 
  port: 54390,             
  database: 'winzo_sql',
  user: 'admin_winzo_sql',
  password: 'avR9@t+ItYe-Mqa3Ulmb2Wk7U:D:Qsla', 
});

const schemaSql = `
CREATE TABLE IF NOT EXISTS users (
    username VARCHAR(64) PRIMARY KEY,
    display_name VARCHAR(128),
    telegram_id BIGINT,
    telegram_username VARCHAR(64),
    main_wallet NUMERIC DEFAULT 0,
    play_wallet NUMERIC DEFAULT 0,
    signup_bonus_remaining NUMERIC DEFAULT 0,
    signup_bonus_original NUMERIC DEFAULT 0,
    withdrawal_hold_until BIGINT DEFAULT 0,
    withdrawal_hold_reason VARCHAR(64),
    pending_withdrawal NUMERIC DEFAULT 0,
    referral_earnings NUMERIC DEFAULT 0,
    wallet_updated_at BIGINT,
    referred_by VARCHAR(64),
    last_ip VARCHAR(64),
    last_ip_hash VARCHAR(128),
    blocked BOOLEAN DEFAULT FALSE,
    blocked_at BIGINT,
    block_reason VARCHAR(256),
    block_signals TEXT,
    device_fingerprint VARCHAR(128),
    fraud_status VARCHAR(64),
    account_status VARCHAR(64),
    created_at BIGINT,
    updated_at BIGINT,
    wallet_ledger JSONB DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS transactions (
    id VARCHAR(128) PRIMARY KEY,
    username VARCHAR(64),
    uid VARCHAR(64),
    wallet_uid VARCHAR(64),
    telegram_id BIGINT,
    telegram_username VARCHAR(64),
    display_name VARCHAR(128),
    type VARCHAR(32),
    amount NUMERIC,
    status VARCHAR(32),
    timestamp BIGINT,
    reference VARCHAR(128),
    full_name VARCHAR(128),
    phone_number VARCHAR(64),
    image_data TEXT,
    reason VARCHAR(256),
    wallet_applied INT DEFAULT 0,
    client_request_id VARCHAR(128),
    updated_at BIGINT
);

CREATE TABLE IF NOT EXISTS rooms (
    id VARCHAR(64) PRIMARY KEY,
    bet NUMERIC,
    phase VARCHAR(32),
    round INT,
    ends_at BIGINT,
    winning_number INT,
    winners_json TEXT,
    prize_pool NUMERIC,
    last_result_json TEXT,
    taken_json TEXT,
    players_json TEXT,
    updated_at BIGINT
);

CREATE TABLE IF NOT EXISTS telegram_users (
    telegram_id BIGINT PRIMARY KEY,
    uid VARCHAR(64),
    username VARCHAR(64),
    telegram_username VARCHAR(64),
    updated_at BIGINT
);

CREATE TABLE IF NOT EXISTS notifications (
    id VARCHAR(128) PRIMARY KEY,
    username VARCHAR(64),
    type VARCHAR(32),
    message TEXT,
    status VARCHAR(32),
    tx_type VARCHAR(32),
    tx_id VARCHAR(128),
    timestamp BIGINT,
    read BOOLEAN DEFAULT FALSE
);
`;

async function setup() {
  try {
    await pool.query(schemaSql);
    console.log('✅ SQL Schema created successfully!');
  } catch (err) {
    console.error('❌ Error creating schema:', err);
  } finally {
    pool.end();
  }
}

setup();
