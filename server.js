import express from 'express';
import { WebSocketServer } from 'ws';
import crypto from 'crypto';
import http from 'http';
import pg from 'pg';
import TelegramBot from 'node-telegram-bot-api';
import { monitorEventLoopDelay } from 'node:perf_hooks';

const { Pool } = pg;

// ============================================================================
// PROCESS-LEVEL ERROR HANDLING
// ============================================================================
process.on('unhandledRejection', (reason, promise) => {
  console.error('🔥 [UNHANDLED REJECTION] at:', promise, 'reason:', reason);
});
process.on('uncaughtException', (err) => {
  console.error('🔥 [UNCAUGHT EXCEPTION]', err);
});

// ============================================================================
// DATABASE
// ============================================================================
// Keep the production pool at 40 by default. These settings are configurable
// for measured capacity tuning; do not raise DB_POOL_MAX without checking the
// PostgreSQL connection limit and the AletCloud database plan.
const DB_POOL_MAX = Math.min(100, Math.max(5, Number.parseInt(process.env.DB_POOL_MAX || '40', 10) || 40));
const DB_IDLE_TIMEOUT_MS = Math.min(120000, Math.max(10000, Number.parseInt(process.env.DB_IDLE_TIMEOUT_MS || '30000', 10) || 30000));
const DB_CONNECT_TIMEOUT_MS = Math.min(30000, Math.max(1000, Number.parseInt(process.env.DB_CONNECT_TIMEOUT_MS || '5000', 10) || 5000));
const pool = new Pool({
  host: process.env.DB_HOST || 'winzo-sql-rw.db-956.svc.cluster.local',
  port: Number(process.env.DB_PORT || 5432),
  database: process.env.DB_NAME || 'winzo_sql',
  user: process.env.DB_USER || 'admin_winzo_sql',
  password: process.env.DB_PASSWORD || '',
  max: DB_POOL_MAX,
  idleTimeoutMillis: DB_IDLE_TIMEOUT_MS,
  connectionTimeoutMillis: DB_CONNECT_TIMEOUT_MS,
  allowExitOnIdle: false,
});
pool.on('error', (err) => console.error('[PG-POOL-IDLE-CLIENT]', err.message));

// ============================================================================
// CONFIG
// ============================================================================
const PORT = Number(process.env.PORT || 3000);
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const TELEGRAM_WEBHOOK_SECRET = process.env.TELEGRAM_WEBHOOK_SECRET || '';
const ADMIN_KEY = process.env.ADMIN_KEY || '';

const SMS_WEBHOOK_SECRET = process.env.SMS_WEBHOOK_SECRET || '';
if (!SMS_WEBHOOK_SECRET) {
  console.error('FATAL: SMS_WEBHOOK_SECRET is not set. SMS auto-deposit webhook will be disabled.');
}

const SESSION_SECRET = String(process.env.SESSION_SECRET || '');
if (SESSION_SECRET.length < 32) {
  throw new Error('FATAL: SESSION_SECRET must be configured with at least 32 characters.');
}

const IP_HASH_SALT = process.env.IP_HASH_SALT || crypto.randomBytes(32).toString('hex');
if (!process.env.IP_HASH_SALT) {
  console.warn('WARNING: IP_HASH_SALT not set. Multi-account detection will not work across restarts.');
}

// ─── ENHANCED: stable salt for cross-day correlation (uses the same value
// for every request, so ip_history rows survive the daily rotation of
// users.last_ip_hash).
const IP_HISTORY_SALT = process.env.IP_HISTORY_SALT || IP_HASH_SALT;
if (!process.env.IP_HISTORY_SALT) {
  console.warn('WARNING: IP_HISTORY_SALT not set. History correlation falls back to IP_HASH_SALT.');
}

const NODE_ENV = process.env.NODE_ENV || 'production';
// New-user signup bonus is disabled. Legacy bonus columns/expiry handling remain
// only to preserve and safely settle any pre-existing account balances.

if (!ADMIN_KEY) console.warn('WARNING: ADMIN_KEY is not set. Admin approval will be disabled.');
if (!BOT_TOKEN) console.warn('WARNING: BOT_TOKEN is not set. All WebSocket authentication will fail.');
if (!process.env.DB_PASSWORD) console.warn('WARNING: DB_PASSWORD is not set. Configure it in the deployment environment.');

const SELECTION_SECONDS = 30;
const SPINNING_SECONDS = 2;
const RESULTS_SECONDS = 10;
const MAX_PICKS = 5;
const PAYOUT_RATE = 0.80;
const REFERRAL_RATE = 0.05;
const BOT_START_WALLET = 1_000_000;
const BOT_MIN = 2;
const BOT_MAX = 3;

const AUTO_DEPOSIT_AMOUNT_TOLERANCE = 0.01;
const MIN_REFERENCE_LENGTH = 6;
const MAX_REFERENCE_LENGTH = 128;
const MIN_TELEBIRR_REF_LENGTH = 8;
const SMS_MAX_AGE_MS = 24 * 60 * 60 * 1000;
const DEPOSIT_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000;
const MIN_DEPOSIT_AMOUNT = 1;
const MAX_DEPOSIT_AMOUNT = 1_000_000;
const DEPOSIT_RATE_LIMIT_WINDOW_MS = Number(process.env.DEPOSIT_RATE_WINDOW_MS || 10 * 60 * 1000);
const DEPOSIT_RATE_LIMIT_MAX = Number(process.env.DEPOSIT_RATE_MAX || 15);
const DUPLICATE_SMS_WINDOW_MS = 60 * 1000;
const MAX_IMAGE_BYTES = 2_000_000;
const WS_MAX_PAYLOAD = 5 * 1024 * 1024;
const WS_ALLOWED_ORIGINS = new Set(String(process.env.WS_ALLOWED_ORIGINS || 'https://minex1976.github.io,https://winzo-authoritative-sqlite-fixed-audited.app.aletcloud.com')
  .split(',').map(v => v.trim()).filter(Boolean));
const WS_ALLOW_NO_ORIGIN = process.env.WS_ALLOW_NO_ORIGIN === 'true';
const WS_MAX_CONNECTIONS_PER_IP = Math.max(2, Number(process.env.WS_MAX_CONNECTIONS_PER_IP || 8));
const WS_CONNECTION_RATE_WINDOW_MS = 60_000;
const WS_CONNECTION_RATE_MAX = Math.max(5, Number(process.env.WS_CONNECTION_RATE_MAX || 20));

// ─── ENHANCED: tighter multi-account thresholds + history window + scanner ──
const MULTI_ACCOUNT_IP_THRESHOLD = Number(process.env.MULTI_ACCOUNT_IP_THRESHOLD || 3);
const MULTI_ACCOUNT_FP_THRESHOLD = Number(process.env.MULTI_ACCOUNT_FP_THRESHOLD || 2);
const MULTI_ACCOUNT_HISTORY_DAYS = Number(process.env.MULTI_ACCOUNT_HISTORY_DAYS || 7);
const SCANNER_INTERVAL_MS = Number(process.env.SCANNER_INTERVAL_MS || 15 * 60 * 1000);
const FINGERPRINT_MAX_LEN = 128;
const FINGERPRINT_MIN_LEN = 8;

const ADMIN_IDLE_MS = Number(process.env.ADMIN_IDLE_MS || 30 * 60 * 1000);

const TRANSFER_RATE_WINDOW_MS = 60 * 1000;
const TRANSFER_RATE_MAX = 10;

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
// TELEGRAM BOT SETUP
// ============================================================================
let bot = null;
const botUserStates = new Map();

if (BOT_TOKEN) {
  bot = new TelegramBot(BOT_TOKEN, { polling: false });
  console.log('✅ Telegram Bot initialized.');
  
  bot.on('polling_error', (error) => console.error('[BOT POLLING ERROR]', error.message));
  bot.on('webhook_error', (error) => console.error('[BOT WEBHOOK ERROR]', error.message));
}

async function safeBotSend(chatId, text, options = {}) {
  if (!bot) return;
  try {
    await bot.sendMessage(chatId, text, options);
  } catch (err) {
    console.error(`[BOT SEND ERROR] Failed to send message to ${chatId}:`, err.message);
  }
}

async function sendBotDepositStatus(uid, message, parseMode = 'Markdown') {
  if (!bot || !uid || !uid.startsWith('tg_')) return;
  try {
    const telegramId = uid.replace('tg_', '');
    if (telegramId && !isNaN(telegramId)) {
      await bot.sendMessage(telegramId, message, { parse_mode: parseMode });
    }
  } catch (e) {
    console.error(`[BOT NOTIFY] Failed to send message to ${uid}:`, e.message);
  }
}

function setupTelegramBot() {
  if (!bot) return;
  
  bot.setMyCommands([
      { command: '/start', description: 'Start' },
      { command: '/register', description: 'Register' },
      { command: '/play', description: 'Play' },
      { command: '/deposit', description: 'Deposit' },
      { command: '/balance', description: 'Balance' },
      { command: '/withdraw', description: 'Withdraw' },
      { command: '/transfer', description: 'Transfer' },
      { command: '/invite', description: 'Invite' },
      { command: '/instruction', description: 'Instruction' },
      { command: '/support', description: 'Support' }
  ]).catch(err => console.error('Bot commands error:', err.message));

  const getMiniAppButton = (text = "Play Winzo 🎮", action = null) => {
      let url = process.env.MINI_APP_URL || 'https://your-aletcloud-url.com';
      if (action) {
          url += (url.includes('?') ? '&' : '?') + 'action=' + action;
      }
      return {
          reply_markup: {
              inline_keyboard: [[
                  { text: text, web_app: { url: url } }
              ]]
          }
      };
  };

  const getMainKeyboard = () => {
      return {
          keyboard: [
              [{ text: "Play 🎮" }, { text: "Register 📝" }],
              [{ text: "Check Balance 💵" }, { text: "Deposit 💵" }],
              [{ text: "Contact Support ☎️" }, { text: "Instruction 📖" }],
              [{ text: "Transfer 🎁" }, { text: "Withdraw 🤑" }],
              [{ text: "Invite 🔗" }, { text: "Convert Bonus 💱" }]
          ],
          resize_keyboard: true,
          is_persistent: true
      };
  };
  
  bot.onText(/\/start/, (msg) => {
      botUserStates.delete(msg.chat.id);
      safeBotSend(msg.chat.id, "Welcome to *Winzo*! Choose an option from the menu below.", { parse_mode: 'Markdown', reply_markup: getMainKeyboard() });
      safeBotSend(msg.chat.id, "Launch the app directly:", getMiniAppButton());
  });
  
  bot.onText(/\/play/, (msg) => {
      botUserStates.delete(msg.chat.id);
      safeBotSend(msg.chat.id, "Ready to play? Launch the app below:", getMiniAppButton("Play Now 🎲", "play"));
  });
  
  bot.onText(/\/register/, (msg) => {
      botUserStates.delete(msg.chat.id);
      safeBotSend(msg.chat.id, "Please launch the Mini App to register and verify your account.", getMiniAppButton("Register Now", "register"));
  });
  
  bot.onText(/\/withdraw/, (msg) => {
      const chatId = msg.chat.id;
      botUserStates.set(chatId, { state: 'WAITING_FOR_WITHDRAW_AMOUNT' });
      safeBotSend(chatId, "እባክዎ ወጪ የሚያደርጉትን የብር መጠን ይግለፁ");
  });
  
  bot.onText(/\/transfer/, (msg) => {
      botUserStates.delete(msg.chat.id);
      safeBotSend(msg.chat.id, "Transfer funds between Main and Play wallets in the app.", getMiniAppButton("Transfer 🔄", "transfer"));
  });
  
  bot.onText(/\/invite/, (msg) => {
      botUserStates.delete(msg.chat.id);
      const refLink = `https://t.me/WinzoBettingBot?startapp=ref_tg_${msg.from.id}`;
      safeBotSend(msg.chat.id, `🎁 *Invite friends and earn 5%!*\n\nShare this link:\n\`${refLink}\`\n\nOr open the app to see the full referral menu:`, { parse_mode: 'Markdown', ...getMiniAppButton("Open Referral Menu", "invite") });
  });
  
  bot.onText(/\/instruction/, (msg) => {
    botUserStates.delete(msg.chat.id);
      
    const text = `🎮 *የዊንዞ መጫወቻ ደንቦች (Winzo Game Rules)*

🎡 *ዊንዞ ስፒን (Winzo Spin)*
   1. ከ1-200 ቁጥሮች ውስጥ እስከ 5 ቁጥሮችን ይምረጡ።
   2. ሰዓቱ ሲያበቃ ኮምፒውተሩ 1 አሸናፊ ቁጥር በዘፈቀደ ይመርጣል።
   3. የመረጡት ቁጥር ከአሸናፊው ጋር ከተመሳሰለ ያሸንፋሉ!

🎱 *ዊንዞ ቢንጎ (Winzo Bingo)*
   1. በቦርድ ላይ ካሉት ካርዶች (1-400) ውስጥ እስከ 2 ካርዶችን ይምረጡ።
   2. ቀይ የሆኑት በሌሎች ተጫዋቾች የተያዙ ናቸው።
   3. ኮምፒውተሩ ከ1-75 ያሉ ቁጥሮችን ሲጠራ በካርድዎ ላይ ያሉትን ቁጥሮች ያመልክቱ። 
   🏆 *ለማሸነፍ:* ወደ ጎን/ታች መስመር፣ አግዳሚ መስመር (Diagonal) ወይም 4 ኮርነሮችን ማሟላት ይኖርብዎታል።

🪜 *ዊንዞ ላደር (Winzo Ladder)*
   1. የውርርድ (Betting) መጠን (15/30) መርጠው ክፍል ይክፈቱ ወይም ይቀላቀሉ።
   2. ዳይስ (Dice) የሚለውን በመጫን ወደ ፊት ይጓዙ፤ Snake ላይ ካረፉ ወደኋላ ይመለሳሉ ፤ Ladder (መንሰላል) ላይ ካረፉ ወደ ላይ ይወጣሉ።
   3. Attack የሚለውን በተን (ቁልፍ) በመጫን እና ለዚህ ሲባል ተጨማሪ 10 ብር በመክፈል በተጋጣሚዎ ላይ "ምናባዊ Snake" ማስቀመጥ ተጋጣሚዎን ወደ ታች መጎተት ይችላሉ።
   🏆 *ለማሸነፍ:* ከተጋጣሚዎ ቀድመው 100 ላይ ከደረሱ ወይም ተጋጣሚዎ ለ45 ሰከንድ ያህል ሰይጫት (ባለበት) ከቀረ አሸናፊ ይሆናሉ።
   
💡 *ማሳሰቢያ:* ሁሉም ጨዋታዎች በPlay Wallet የሚጫወቱ ሲሆን፣ የሽልማት ገንዘብ በቀጥታ ወደ Main Wallet ይገባል።`;

    safeBotSend(msg.chat.id, text, { parse_mode: 'Markdown' });
  });
  
  bot.onText(/\/support/, (msg) => {
    botUserStates.delete(msg.chat.id);
    const supportMsg = `Need help? Contact our support team: <a href="https://t.me/winzo_help_center">@winzo_help_center</a>`;
    
    safeBotSend(msg.chat.id, supportMsg, { 
        parse_mode: 'HTML',
        disable_web_page_preview: true
    });
  });
  bot.onText(/\/balance/, async (msg) => {
      botUserStates.delete(msg.chat.id);
      const uid = `tg_${msg.from.id}`; 
      try {
          const res = await pool.query('SELECT main_wallet, play_wallet FROM users WHERE username = $1', [uid]);
          if (res.rows.length === 0) {
              safeBotSend(msg.chat.id, "You don't have an account yet. Please launch the Mini App to register.", getMiniAppButton("Register Now", "register"));
          } else {
              const main = Number(res.rows[0].main_wallet).toFixed(2);
              const play = Number(res.rows[0].play_wallet).toFixed(2);
              safeBotSend(msg.chat.id, `📊 *Your Winzo Balance*\n\n💰 Main Wallet: *${main} Birr*\n🎮 Play Wallet: *${play} Birr*`, { parse_mode: 'Markdown', ...getMiniAppButton("Open App") });
          }
      } catch (e) {
          console.error('[BOT BALANCE]', e.message);
          safeBotSend(msg.chat.id, "⚠️ Could not fetch balance. Please try again later.");
      }
  });

  bot.onText(/\/deposit/, (msg) => {
      const chatId = msg.chat.id;
      botUserStates.set(chatId, { state: 'WAITING_FOR_AMOUNT' });
      safeBotSend(chatId, "የሚያስገቡትን የብር መጠን 👇 ይግለፁ");
  });

  bot.on('message', async (msg) => {
      try {
          const chatId = msg.chat.id;
          const text = msg.text ? msg.text.trim() : '';

          // Handle Custom Keyboard Buttons
          if (text === 'Play 🎮') { botUserStates.delete(chatId); return safeBotSend(chatId, "Ready to play? Launch the app below:", getMiniAppButton("Play Now 🎲", "play")); }
          if (text === 'Register 📝') { botUserStates.delete(chatId); return safeBotSend(chatId, "Please launch the Mini App to register and verify your account.", getMiniAppButton("Register Now", "register")); }
          if (text === 'Check Balance 💵') {
              botUserStates.delete(chatId);
              const uid = `tg_${msg.from.id}`; 
              try {
                  const res = await pool.query('SELECT main_wallet, play_wallet FROM users WHERE username = $1', [uid]);
                  if (res.rows.length === 0) {
                      safeBotSend(chatId, "You don't have an account yet. Please launch the Mini App to register.", getMiniAppButton("Register Now", "register"));
                  } else {
                      const main = Number(res.rows[0].main_wallet).toFixed(2);
                      const play = Number(res.rows[0].play_wallet).toFixed(2);
                      safeBotSend(chatId, `📊 *Your Winzo Balance*\n\n💰 Main Wallet: *${main} Birr*\n🎮 Play Wallet: *${play} Birr*`, { parse_mode: 'Markdown', ...getMiniAppButton("Open App") });
                  }
              } catch (e) { safeBotSend(chatId, "⚠️ Could not fetch balance. Please try again later."); }
              return;
          }
          if (text === 'Deposit 💵') {
              botUserStates.set(chatId, { state: 'WAITING_FOR_AMOUNT' });
              return safeBotSend(chatId, "የሚያስገቡትን የብር መጠን ይግለፁ 👇");
          }
          if (text === 'Contact Support ☎️') {
              botUserStates.delete(chatId);
              const supportMsg = `Need help? Contact our support team: <a href="https://t.me/winzo_help_center">@winzo_help_center</a>`;
              return safeBotSend(chatId, supportMsg, { parse_mode: 'HTML', disable_web_page_preview: true });
          }
          if (text === 'Instruction 📖') {
              botUserStates.delete(chatId);
              const instructionText = `🎮 *የዊንዞ መጫወቻ ደንቦች (Winzo Game Rules)*

🎡 *ዊንዞ ስፒን (Winzo Spin)*
   1. ከ1-200 ቁጥሮች ውስጥ እስከ 5 ቁጥሮችን ይምረጡ።
   2. ሰዓቱ ሲያበቃ ኮምፒውተሩ 1 አሸናፊ ቁጥር በዘፈቀደ ይመርጣል።
   3. የመረጡት ቁጥር ከአሸናፊው ጋር ከተመሳሰለ ያሸንፋሉ!

🎱 *ዊንዞ ቢንጎ (Winzo Bingo)*
   1. በቦርድ ላይ ካሉት ካርዶች (1-400) ውስጥ እስከ 2 ካርዶችን ይምረጡ።
   2. ቀይ የሆኑት በሌሎች ተጫዋቾች የተያዙ ናቸው።
   3. ኮምፒውተሩ ከ1-75 ያሉ ቁጥሮችን ሲጠራ በካርድዎ ላይ ያሉትን ቁጥሮች ያመልክቱ። 
   🏆 *ለማሸነፍ:* ወደ ጎን/ታች መስመር፣ አግዳሚ መስመር (Diagonal) ወይም 4 ኮርነሮችን ማሟላት ይኖርብዎታል።

🪜 *ዊንዞ ላደር (Winzo Ladder)*
   1. የውርርድ (Betting) መጠን (15/30) መርጠው ክፍል ይክፈቱ ወይም ይቀላቀሉ።
   2. ዳይስ (Dice) የሚለውን በመጫን ወደ ፊት ይጓዙ፤ Snake ላይ ካረፉ ወደኋላ ይመለሳሉ ፤ Ladder (መንሰላል) ላይ ካረፉ ወደ ላይ ይወጣሉ።
   3. Attack የሚለውን በተን (ቁልፍ) በመጫን እና ለዚህ ሲባል ተጨማሪ 10 ብር በመክፈል በተጋጣሚዎ ላይ "ምናባዊ Snake" ማስቀመጥ ተጋጣሚዎን ወደ ታች መጎተት ይችላሉ።
   🏆 *ለማሸነፍ:* ከተጋጣሚዎ ቀድመው 100 ላይ ከደረሱ ወይም ተጋጣሚዎ ለ45 ሰከንድ ያህል ሰይጫት (ባለበት) ከቀረ አሸናፊ ይሆናሉ።
   
💡 *ማሳሰቢያ:* ሁሉም ጨዋታዎች በPlay Wallet የሚጫወቱ ሲሆን፣ የሽልማት ገንዘብ በቀጥታ ወደ Main Wallet ይገባል።`; 
              return safeBotSend(chatId, instructionText, { parse_mode: 'Markdown' });
          }
          if (text === 'Transfer 🎁') { botUserStates.delete(chatId); return safeBotSend(chatId, "Transfer funds between Main and Play wallets in the app.", getMiniAppButton("Transfer 🔄", "transfer")); }
          if (text === 'Withdraw 🤑') {
              botUserStates.set(chatId, { state: 'WAITING_FOR_WITHDRAW_AMOUNT' });
              return safeBotSend(chatId, "እባክዎ ወጪ የሚያደርጉትን የብር መጠን ይላኩ");
          }
          if (text === 'Invite 🔗') {
              botUserStates.delete(chatId);
              const refLink = `https://t.me/WinzoBettingBot?startapp=ref_tg_${msg.from.id}`;
              return safeBotSend(chatId, `🎁 *Invite friends and earn 5%!*\n\nShare this link:\n\`${refLink}\`\n\nOr open the app to see the full referral menu:`, { parse_mode: 'Markdown', ...getMiniAppButton("Open Referral Menu", "invite") });
          }
          if (text === 'Convert Bonus 💱') {
              botUserStates.delete(chatId);
              return safeBotSend(chatId, "Please use the Mini App to convert your bonus.");
          }

          const state = botUserStates.get(chatId);
          if (!state || !msg.text || msg.text.startsWith('/')) return;

          const uid = `tg_${msg.from.id}`;

          // ================== DEPOSIT STATES ==================
          if (state.state === 'WAITING_FOR_AMOUNT') {
              const amount = parseFloat(text.replace(/[^0-9.]/g, ''));
              
              if (isNaN(amount) || amount < 1) {
                  return safeBotSend(chatId, "⚠️ እባክዎ ትክክለኛ የብር መጠን ያስገቡ።");
              }
              
              state.amount = amount;
              state.state = 'WAITING_FOR_SMS';
              botUserStates.set(chatId, state);

              const instructions = `💵 ገንዘብ ገቢ ለማድረግ ቴሌብር ይጠቀሙ 💵: \n1ኛ - ከታች ባለው ቁጥር  ያስገቡ ።\n 👉<code>0974603773</code>👈  👤 Abraham Abebe   \n2ኛ - የላኩበትን የፅሁፍ መልእክት (SMS) ኮፒ ያድርጉና ከስር 👇  ፔሰት (paste) አድርገው ወደኛ ይላኩ። `;
              
              return safeBotSend(chatId, instructions, { parse_mode: 'HTML' });
          }

          if (state.state === 'WAITING_FOR_SMS') {
              const smsText = text;
              const parsed = parsePaymentRecord({ text: smsText, sender: 'user_paste' });
              
              if (!parsed.reference) {
                  return safeBotSend(chatId, "⚠️ የላኩበትን 'Reference' ኮድ ማግኘት አልቻልንም። እባክዎ በትክክል መላክዎትን ያረጋግጡ።");
              }

              if (Math.abs((parsed.amount || 0) - state.amount) > 1) {
                  return safeBotSend(chatId, `⚠️ በፅሁፍ መልእክቱ ላይ ያለው መጠን (${parsed.amount}) እና እርስዎ የገለፁት (${state.amount}) ተመሳሳይ አይደለም።`);
              }

              botUserStates.delete(chatId);

              const txRes = await transactionRequest(uid, {
                  txType: 'deposit',
                  amount: state.amount,
                  reference: parsed.reference,
                  clientRequestId: 'bot_' + Date.now()
              });

              if (!txRes.ok) {
                  return; 
              }

              const smsId = crypto.randomUUID();
              const ts = now();
              const textHash = crypto.createHash('sha256').update('user_paste|' + smsText).digest('hex');
              await pool.query('INSERT INTO incoming_sms (id, sender, text, timestamp, processed, text_hash) VALUES ($1,$2,$3,$4,false,$5)', [smsId, 'user_paste', smsText, ts, textHash]);
          }

          // ================== WITHDRAW STATES ==================
          if (state.state === 'WAITING_FOR_WITHDRAW_AMOUNT') {
              const amount = parseFloat(text.replace(/[^0-9.]/g, ''));
              
              if (isNaN(amount) || amount <= 0) {
                  return safeBotSend(chatId, "⚠️ እባክዎ ትክክለኛ የብር መጠን ያስገቡ።");
              }

              const w = await walletFor(uid);
              const available = w.main - w.pending;

              if (amount > available) {
                  return safeBotSend(chatId, "በቂ ሂሳብ የሎትም");
              }

              if (available - amount < 15) {
                  return safeBotSend(chatId, "Main Wallet ላይ በትንሹ 15 ብር ቀሪ ሊኖርዎት ይገባል");
              }

              state.amount = amount;
              state.state = 'WAITING_FOR_WITHDRAW_PHONE';
              botUserStates.set(chatId, state);
              
              return safeBotSend(chatId, "እባክዎ ወጪ የሚያደርጉበትን (የቴሌብር ደንበኛ) ስልክ ቁጥር ይግለፁ");
          }

          if (state.state === 'WAITING_FOR_WITHDRAW_PHONE') {
              state.phoneNumber = text; // Basic capture
              state.state = 'WAITING_FOR_WITHDRAW_NAME';
              botUserStates.set(chatId, state);

              return safeBotSend(chatId, "እባክዎ የቴሌብር አካውንቱ (Account) ባለቤት (User Name) ስም ይግለፁ።");
          }

          if (state.state === 'WAITING_FOR_WITHDRAW_NAME') {
              const fullName = text;
              
              botUserStates.delete(chatId);

              const txRes = await transactionRequest(uid, {
                  txType: 'withdraw',
                  amount: state.amount,
                  phoneNumber: state.phoneNumber,
                  fullName: fullName,
                  clientRequestId: 'bot_wd_' + Date.now()
              });

              if (!txRes.ok) {
                  // Errors from backend (like double submission) will be handled by the backend notification
                  return; 
              }

              return safeBotSend(chatId, "ትእዛዙ ተልኳል!  የዊንዞ ቤተሰብ ስለሆኑ እናመሰግናለን!");
          }

      } catch (err) {
          console.error('[BOT STATE ERROR]', err);
          safeBotSend(msg.chat.id, "⚠️ እባክዎ እንደገና ይሞክሩ።");
      }
  });
}

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
          account_status VARCHAR(64), phone_number VARCHAR(64), created_at BIGINT, updated_at BIGINT, wallet_ledger JSONB DEFAULT '{}'::jsonb
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
      CREATE TABLE IF NOT EXISTS telegram_contacts (
          telegram_id BIGINT PRIMARY KEY, phone_number VARCHAR(64) NOT NULL, first_name VARCHAR(128), last_name VARCHAR(128), updated_at BIGINT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS telegram_contact_attempts (
          telegram_id BIGINT PRIMARY KEY, status VARCHAR(40) NOT NULL, updated_at BIGINT NOT NULL
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
          trace_id VARCHAR(64) PRIMARY KEY, uid VARCHAR(64), stage VARCHAR(64) NOT NULL, status VARCHAR(32) NOT NULL,
          type VARCHAR(32), amount NUMERIC, reference VARCHAR(128), client_request_id VARCHAR(128), tx_id VARCHAR(128),
          reason VARCHAR(256), detail JSONB DEFAULT '{}'::jsonb, at BIGINT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS game_winners (
          id VARCHAR(128) PRIMARY KEY, username VARCHAR(64) NOT NULL, display_name VARCHAR(128),
          room_id VARCHAR(64) NOT NULL, round INT NOT NULL, win_amount NUMERIC NOT NULL,
          winning_number INT, prize_pool NUMERIC, created_at BIGINT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS round_snapshots (
          id VARCHAR(160) PRIMARY KEY, room_id VARCHAR(64) NOT NULL, round INT NOT NULL, bet NUMERIC NOT NULL,
          human_count INT NOT NULL DEFAULT 0, human_picks INT NOT NULL DEFAULT 0, bot_count INT NOT NULL DEFAULT 0,
          bot_picks INT NOT NULL DEFAULT 0, total_picks INT NOT NULL DEFAULT 0, prize_pool NUMERIC NOT NULL DEFAULT 0,
          winning_number INT, winner_count INT NOT NULL DEFAULT 0, human_winner_count INT NOT NULL DEFAULT 0,
          human_win_amount NUMERIC NOT NULL DEFAULT 0, human_player_ids JSONB DEFAULT '[]'::jsonb,
          started_at BIGINT NOT NULL, ended_at BIGINT NOT NULL, created_at BIGINT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS total_picks (
          id VARCHAR(200) PRIMARY KEY,
          game VARCHAR(16) NOT NULL,
          room_id VARCHAR(64) NOT NULL,
          round_id VARCHAR(160) NOT NULL,
          stake NUMERIC NOT NULL DEFAULT 0,
          total_picks INTEGER NOT NULL DEFAULT 0 CHECK (total_picks >= 0),
          recorded_at BIGINT NOT NULL,
          UNIQUE (game, room_id, round_id)
      );
      CREATE INDEX IF NOT EXISTS idx_total_picks_game_recorded ON total_picks(game, recorded_at DESC);
      CREATE INDEX IF NOT EXISTS idx_total_picks_room_round ON total_picks(room_id, round_id);
    `);

    // ─── ENHANCED: multi-account signal history tables ──────────────────────
    await client.query(`
      CREATE TABLE IF NOT EXISTS ip_history (
        id VARCHAR(128) PRIMARY KEY,
        username VARCHAR(64) NOT NULL,
        ip_hash VARCHAR(128) NOT NULL,
        seen_at BIGINT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_ip_history_hash ON ip_history (ip_hash, seen_at DESC);
      CREATE INDEX IF NOT EXISTS idx_ip_history_user ON ip_history (username, seen_at DESC);
      CREATE INDEX IF NOT EXISTS idx_ip_history_time ON ip_history (seen_at DESC);

      CREATE TABLE IF NOT EXISTS device_fp_history (
        id VARCHAR(128) PRIMARY KEY,
        username VARCHAR(64) NOT NULL,
        fingerprint VARCHAR(128) NOT NULL,
        seen_at BIGINT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_fp_history_fp ON device_fp_history (fingerprint, seen_at DESC);
      CREATE INDEX IF NOT EXISTS idx_fp_history_user ON device_fp_history (username, seen_at DESC);
      CREATE INDEX IF NOT EXISTS idx_fp_history_time ON device_fp_history (seen_at DESC);
    `);

    await client.query(`ALTER TABLE request_trace ADD COLUMN IF NOT EXISTS trace_row_id BIGSERIAL;`);
    await client.query(`
      DO $$
      DECLARE pk_name text; DECLARE pk_is_trace_row_id boolean;
      BEGIN
        SELECT c.conname, (array_length(c.conkey, 1) = 1 AND a.attname = 'trace_row_id')
          INTO pk_name, pk_is_trace_row_id
          FROM pg_constraint c JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = c.conkey[1]
         WHERE c.conrelid = 'request_trace'::regclass AND c.contype = 'p' LIMIT 1;
        IF pk_name IS NOT NULL AND NOT COALESCE(pk_is_trace_row_id, false) THEN
          EXECUTE format('ALTER TABLE request_trace DROP CONSTRAINT %I', pk_name); pk_name := NULL;
        END IF;
        IF pk_name IS NULL THEN ALTER TABLE request_trace ADD CONSTRAINT request_trace_pkey PRIMARY KEY (trace_row_id); END IF;
      END $$;
    `);

    await client.query(`
      ALTER TABLE incoming_sms ADD COLUMN IF NOT EXISTS matched_tx_id VARCHAR(128);
      ALTER TABLE incoming_sms ADD COLUMN IF NOT EXISTS text_hash VARCHAR(64);
      ALTER TABLE transactions  ADD COLUMN IF NOT EXISTS reference VARCHAR(128);
      ALTER TABLE transactions  ADD COLUMN IF NOT EXISTS client_request_id VARCHAR(128);
      ALTER TABLE users         ADD COLUMN IF NOT EXISTS fraud_status VARCHAR(64);
      ALTER TABLE users         ADD COLUMN IF NOT EXISTS account_status VARCHAR(64);
      ALTER TABLE users         ADD COLUMN IF NOT EXISTS phone_number VARCHAR(64);
      ALTER TABLE users         ADD COLUMN IF NOT EXISTS referral_earnings NUMERIC DEFAULT 0;
      ALTER TABLE users ADD COLUMN IF NOT EXISTS signup_bonus_expires_at BIGINT DEFAULT 0;
      ALTER TABLE users ADD COLUMN IF NOT EXISTS last_seen_at BIGINT DEFAULT 0;
      ALTER TABLE users ADD COLUMN IF NOT EXISTS fraud_flagged_at BIGINT DEFAULT 0;
      ALTER TABLE notifications ADD COLUMN IF NOT EXISTS dismiss_policy VARCHAR(16) DEFAULT 'user';
      ALTER TABLE notifications ADD COLUMN IF NOT EXISTS created_by VARCHAR(64);
      ALTER TABLE notifications ADD COLUMN IF NOT EXISTS broadcast_id VARCHAR(128);
      CREATE TABLE IF NOT EXISTS security_scan_logs(id VARCHAR(128) PRIMARY KEY, scanned INT NOT NULL, flagged INT NOT NULL, findings JSONB NOT NULL DEFAULT '[]'::jsonb, created_at BIGINT NOT NULL);
      ALTER TABLE rooms         ADD COLUMN IF NOT EXISTS server_seed VARCHAR(128);
      ALTER TABLE rooms         ADD COLUMN IF NOT EXISTS server_seed_hash VARCHAR(128);
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS bingo_rounds (
        id VARCHAR(64) PRIMARY KEY, game_id VARCHAR(32) NOT NULL, round_number INTEGER NOT NULL,
        phase VARCHAR(24) NOT NULL, stake NUMERIC NOT NULL, prize_pool NUMERIC NOT NULL DEFAULT 0,
        called_numbers JSONB NOT NULL DEFAULT '[]'::jsonb, started_at BIGINT NOT NULL, ends_at BIGINT NOT NULL,
        completed_at BIGINT, winners JSONB NOT NULL DEFAULT '[]'::jsonb,
        server_seed VARCHAR(128), server_seed_hash VARCHAR(128)
      );
      CREATE TABLE IF NOT EXISTS bingo_entries (
        id VARCHAR(128) PRIMARY KEY, round_id VARCHAR(64) NOT NULL REFERENCES bingo_rounds(id),
        username VARCHAR(64) NOT NULL, card_number INTEGER NOT NULL, card_data JSONB NOT NULL,
        stake NUMERIC NOT NULL, created_at BIGINT NOT NULL, UNIQUE(round_id, username, card_number)
      );
      CREATE TABLE IF NOT EXISTS bingo_wallet_transactions (
        id VARCHAR(128) PRIMARY KEY, round_id VARCHAR(64) NOT NULL, username VARCHAR(64) NOT NULL,
        type VARCHAR(24) NOT NULL, amount NUMERIC NOT NULL, idempotency_key VARCHAR(160) UNIQUE,
        created_at BIGINT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS bingo_results (
        id VARCHAR(128) PRIMARY KEY, round_id VARCHAR(64) NOT NULL, username VARCHAR(64) NOT NULL,
        display_name VARCHAR(128), card_number INTEGER NOT NULL, pattern VARCHAR(32) NOT NULL,
        prize NUMERIC NOT NULL, call_number INTEGER, created_at BIGINT NOT NULL,
        UNIQUE(round_id, username, card_number)
      );
      CREATE INDEX IF NOT EXISTS idx_bingo_entries_round ON bingo_entries(round_id);
      CREATE UNIQUE INDEX IF NOT EXISTS idx_bingo_entries_round_card ON bingo_entries(round_id, card_number);
      CREATE INDEX IF NOT EXISTS idx_bingo_results_user ON bingo_results(username, created_at DESC);
    `);
    await client.query(`
      CREATE TABLE IF NOT EXISTS reporting_accounts (
        id VARCHAR(64) PRIMARY KEY,
        username VARCHAR(32) NOT NULL UNIQUE,
        display_name VARCHAR(100) NOT NULL,
        password_salt VARCHAR(64) NOT NULL,
        password_hash VARCHAR(128) NOT NULL,
        active BOOLEAN NOT NULL DEFAULT TRUE,
        must_change_password BOOLEAN NOT NULL DEFAULT TRUE,
        failed_attempts INT NOT NULL DEFAULT 0,
        locked_until BIGINT NOT NULL DEFAULT 0,
        created_at BIGINT NOT NULL,
        updated_at BIGINT NOT NULL,
        last_login_at BIGINT
      );
      CREATE TABLE IF NOT EXISTS reporting_sessions (
        token_hash VARCHAR(64) PRIMARY KEY,
        account_id VARCHAR(64) NOT NULL REFERENCES reporting_accounts(id) ON DELETE CASCADE,
        created_at BIGINT NOT NULL,
        expires_at BIGINT NOT NULL,
        last_seen_at BIGINT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_reporting_sessions_account ON reporting_sessions(account_id);
      CREATE INDEX IF NOT EXISTS idx_reporting_sessions_expiry ON reporting_sessions(expires_at);
      CREATE TABLE IF NOT EXISTS reporting_account_audit (
        id VARCHAR(64) PRIMARY KEY,
        username VARCHAR(32) NOT NULL,
        event VARCHAR(64) NOT NULL,
        details JSONB NOT NULL DEFAULT '{}'::jsonb,
        ip_hash VARCHAR(64),
        at BIGINT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_reporting_audit_username_at ON reporting_account_audit(username, at DESC);
      CREATE INDEX IF NOT EXISTS idx_reporting_audit_at ON reporting_account_audit(at DESC);
    `);
    await client.query(`ALTER TABLE bingo_rounds ADD COLUMN IF NOT EXISTS server_seed VARCHAR(128);`);
    await client.query(`ALTER TABLE bingo_rounds ADD COLUMN IF NOT EXISTS server_seed_hash VARCHAR(128);`);

    await client.query(`
      ALTER TABLE telegram_contacts ADD COLUMN IF NOT EXISTS phone_normalized VARCHAR(32);
      UPDATE telegram_contacts
        SET phone_normalized = regexp_replace(COALESCE(phone_number, ''), '[^0-9]', '', 'g')
        WHERE phone_normalized IS NULL;
      WITH duplicate_numbers AS (
        SELECT phone_normalized FROM telegram_contacts
        WHERE phone_normalized IS NOT NULL AND phone_normalized <> ''
        GROUP BY phone_normalized HAVING COUNT(*) > 1
      )
      UPDATE telegram_contacts tc
        SET phone_number = '', phone_normalized = ''
        WHERE tc.phone_normalized IN (SELECT phone_normalized FROM duplicate_numbers);
      UPDATE users SET phone_number = ''
        WHERE telegram_id IN (SELECT telegram_id FROM telegram_contacts WHERE phone_normalized = '');
      CREATE UNIQUE INDEX IF NOT EXISTS uq_telegram_contacts_phone_normalized
        ON telegram_contacts (phone_normalized)
        WHERE phone_normalized IS NOT NULL AND phone_normalized <> '';
    `);

    await client.query(`UPDATE users SET referral_earnings = 0 WHERE referral_earnings IS NULL;`);

    console.log('✅ Migrations applied.');

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_tx_pending_deposit ON transactions (status, type) WHERE status = 'pending' AND type = 'deposit';
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
      CREATE INDEX IF NOT EXISTS idx_users_ip_hash ON users (last_ip_hash) WHERE last_ip_hash IS NOT NULL;
      CREATE INDEX IF NOT EXISTS idx_users_fingerprint ON users (device_fingerprint) WHERE device_fingerprint IS NOT NULL;
      CREATE INDEX IF NOT EXISTS idx_round_snapshots_time ON round_snapshots (ended_at DESC);
      CREATE INDEX IF NOT EXISTS idx_round_snapshots_room_round ON round_snapshots (room_id, round);
      CREATE INDEX IF NOT EXISTS idx_round_snapshots_room_time ON round_snapshots (room_id, ended_at DESC);
      CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users (telegram_id) WHERE telegram_id IS NOT NULL;
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
// ===========================================
function toFiniteNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}
function now() { return Date.now(); }
function normalizeTelegramPhoneNumber(value) {
  const digits = String(value || '').replace(/\D/g, '');
  if (digits.length < 7 || digits.length > 15) return '';
  return digits;
}
function safeUser(username) { return String(username || '').replace(/[^A-Za-z0-9_]/g, '').slice(0, 32); }
function safeUid(value) { return String(value || '').replace(/[^A-Za-z0-9_:-]/g, '').slice(0, 64); }
function constantTimeEqual(a, b) {
  const aa = Buffer.from(String(a || ''));
  const bb = Buffer.from(String(b || ''));
  return aa.length === bb.length && crypto.timingSafeEqual(aa, bb);
}


// Reporting-account passwords use a memory-hard KDF; no plaintext passwords
// or reusable shared reporting secret are stored by the server.
function scryptReportingPassword(password, saltHex) {
  const salt = saltHex ? Buffer.from(saltHex, 'hex') : crypto.randomBytes(16);
  return new Promise((resolve, reject) => {
    crypto.scrypt(String(password), salt, 64, { N: 32768, r: 8, p: 1, maxmem: 64 * 1024 * 1024 }, (err, derived) => {
      if (err) return reject(err);
      resolve({ salt: salt.toString('hex'), hash: derived.toString('hex') });
    });
  });
}
async function verifyReportingPassword(password, saltHex, expectedHashHex) {
  const actual = await scryptReportingPassword(password, saltHex);
  return constantTimeEqual(actual.hash, expectedHashHex);
}
function validReportingPassword(password) {
  return typeof password === 'string' && password.length >= 12 && password.length <= 128;
}
function normalizeReportingUsername(value) {
  return String(value || '').trim().toLowerCase();
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
  const salt = `${IP_HASH_SALT}|${d}`;
  return hashText(`${salt}|${String(value || '')}`);
}

// ─── ENHANCED: stable IP hash for cross-day correlation ─────────────────────
function hashIpStable(value) {
  return hashText(`${IP_HISTORY_SALT}|stable|${String(value || '')}`);
}

// ─── ENHANCED: normalize + hash a client-supplied device fingerprint ────────
// Returns null for garbage (too short, too long, control chars). Otherwise a
// deterministic 128-char hex hash safe for DB storage. IMPORTANT: only pass the
// RAW client value here — do NOT re-sanitize the returned hash.
function sanitizeFingerprint(raw) {
  if (raw == null) return null;
  const s = String(raw).trim();
  if (s.length < FINGERPRINT_MIN_LEN) return null;
  if (s.length > 4 * FINGERPRINT_MAX_LEN) return null;
  if (/[\x00-\x1f\x7f\s]/.test(s)) return null;
  return hashText(`fp|${s}`).slice(0, FINGERPRINT_MAX_LEN);
}

function websocketClientIp(ws) {
  if (process.env.TRUST_PROXY_HEADERS === 'true') {
    const forwarded = String(ws?._req?.headers?.['x-forwarded-for'] || '').split(',')[0].trim();
    if (forwarded) return normalizedClientIp(forwarded);
  }
  return normalizedClientIp(ws?._socket?.remoteAddress || '');
}

function issueSessionToken(uid) {
  const ts = Date.now();
  const payload = `${uid}:${ts}`;
  const sig = crypto.createHmac('sha256', SESSION_SECRET).update(payload).digest('hex');
  return Buffer.from(`${payload}:${sig}`).toString('base64url');
}
function verifySessionToken(token) {
  try {
    const decoded = Buffer.from(String(token || ''), 'base64url').toString();
    const parts = decoded.split(':');
    if (parts.length !== 3) return null;
    const [uid, ts, sig] = parts;
    const expected = crypto.createHmac('sha256', SESSION_SECRET).update(`${uid}:${ts}`).digest('hex');
    if (!constantTimeEqual(sig, expected)) return null;
    if (Date.now() - Number(ts) > 24 * 60 * 60 * 1000) return null;
    return uid;
  } catch { return null; }
}

const transferRate = new Map();
function checkTransferRate(uid) {
  const since = now() - TRANSFER_RATE_WINDOW_MS;
  const arr = (transferRate.get(uid) || []).filter(t => t > since);
  if (arr.length >= TRANSFER_RATE_MAX) return false;
  arr.push(now());
  transferRate.set(uid, arr);
  return true;
}

class Mutex {
  constructor() { this._t = Promise.resolve(); }
  run(fn) { const n = this._t.then(fn, fn); this._t = n.then(() => {}, () => {}); return n; }
}

class SeededRNG {
  constructor(seedHex) {
    this.seed = Buffer.from(String(seedHex), 'hex');
    this.counter = 0;
    this.buffer = Buffer.alloc(0);
    this.bufferPos = 0;
  }
  _refill() {
    const chunk = crypto.createHmac('sha256', this.seed).update(`ctr:${this.counter++}`).digest();
    this.buffer = Buffer.concat([this.buffer.slice(this.bufferPos), chunk]);
    this.bufferPos = 0;
  }
  nextUInt32() {
    if (this.bufferPos + 4 > this.buffer.length) this._refill();
    const v = this.buffer.readUInt32BE(this.bufferPos);
    this.bufferPos += 4;
    return v;
  }
  nextInt(n) { return this.nextUInt32() % n; }
}

function seededShuffle(pool, seedOrRng) {
  const rng = seedOrRng instanceof SeededRNG ? seedOrRng : new SeededRNG(seedOrRng);
  for (let i = pool.length - 1; i > 0; i--) {
    const j = rng.nextInt(i + 1);
    [pool[i], pool[j]] = [pool[j], pool[i]];
  }
  return pool;
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
        String(traceId).slice(0, 64), uid ? String(uid).slice(0, 64) : null,
        String(stage).slice(0, 64), String(status).slice(0, 32),
        opts.type ? String(opts.type).slice(0, 32) : null,
        Number.isFinite(Number(opts.amount)) ? Number(opts.amount) : null,
        opts.reference ? String(opts.reference).slice(0, 128) : null,
        opts.clientRequestId ? String(opts.clientRequestId).slice(0, 128) : null,
        opts.txId ? String(opts.txId).slice(0, 128) : null,
        opts.reason ? String(opts.reason).slice(0, 256) : null,
        JSON.stringify(opts.detail || {}), now(),
      ]
    );
  } catch (e) { console.error('[TRACE] insert failed:', e.message, { traceId, stage, status }); }
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
function validateInitData(initData, maxAge = 86400) {
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
  if (ageSeconds > maxAge) return { ok: false, reason: 'expired' };
  if (ageSeconds < -300) return { ok: false, reason: 'future-date' };
  let user = null;
  try { const rawUser = params.get('user'); user = rawUser ? JSON.parse(rawUser) : null; }
  catch { return { ok: false, reason: 'user-not-json' }; }
  if (!user || typeof user.id !== 'number' || !Number.isSafeInteger(user.id) || user.id <= 0) return { ok: false, reason: 'invalid-user' };
  return { ok: true, user, authDate, params };
}

// ============================================================================
// MULTI-ACCOUNT DETECTION  (ENHANCED — records history + detects clusters)
// ============================================================================
async function recordAndDetectSignals(username, opts = {}) {
  const ip = opts.ip ? normalizedClientIp(opts.ip) : null;
  const fingerprint = opts.fingerprint ? sanitizeFingerprint(opts.fingerprint) : null;
  const t = now();

  // 1) Append to history (dedupe within 60s to avoid spam rows)
  try {
    if (ip) {
      const ipHash = hashIpStable(ip);
      const existing = await pool.query(
        'SELECT id FROM ip_history WHERE username = $1 AND ip_hash = $2 AND seen_at > $3 LIMIT 1',
        [username, ipHash, t - 60_000]
      );
      if (!existing.rows.length) {
        await pool.query(
          'INSERT INTO ip_history (id, username, ip_hash, seen_at) VALUES ($1,$2,$3,$4)',
          [crypto.randomUUID(), username, ipHash, t]
        );
      }
    }
    if (fingerprint) {
      const existing = await pool.query(
        'SELECT id FROM device_fp_history WHERE username = $1 AND fingerprint = $2 AND seen_at > $3 LIMIT 1',
        [username, fingerprint, t - 60_000]
      );
      if (!existing.rows.length) {
        await pool.query(
          'INSERT INTO device_fp_history (id, username, fingerprint, seen_at) VALUES ($1,$2,$3,$4)',
          [crypto.randomUUID(), username, fingerprint, t]
        );
      }
    }
  } catch (e) { console.error('[SIGNAL] history insert failed:', e.message); }

  // 2) Detect clusters over the history window
  const signals = [];
  const since = t - MULTI_ACCOUNT_HISTORY_DAYS * DAY;

  if (ip) {
    try {
      const ipHash = hashIpStable(ip);
      const r = await pool.query(
        `SELECT COUNT(DISTINCT username)::int AS c
           FROM ip_history
          WHERE ip_hash = $1 AND seen_at > $2 AND username <> $3`,
        [ipHash, since, username]
      );
      if (r.rows[0].c >= MULTI_ACCOUNT_IP_THRESHOLD) {
        signals.push({
          kind: 'ip-cluster', count: r.rows[0].c,
          windowDays: MULTI_ACCOUNT_HISTORY_DAYS, threshold: MULTI_ACCOUNT_IP_THRESHOLD,
          lastSeen: t,
        });
      }
    } catch (e) { console.error('[SIGNAL] ip-cluster query failed:', e.message); }
  }

  if (fingerprint) {
    try {
      const r = await pool.query(
        `SELECT COUNT(DISTINCT username)::int AS c
           FROM device_fp_history
          WHERE fingerprint = $1 AND seen_at > $2 AND username <> $3`,
        [fingerprint, since, username]
      );
      if (r.rows[0].c >= MULTI_ACCOUNT_FP_THRESHOLD) {
        signals.push({
          kind: 'fingerprint-cluster', count: r.rows[0].c,
          windowDays: MULTI_ACCOUNT_HISTORY_DAYS, threshold: MULTI_ACCOUNT_FP_THRESHOLD,
          lastSeen: t,
        });
      }
    } catch (e) { console.error('[SIGNAL] fp-cluster query failed:', e.message); }
  }

  // 3) Persist fraud state (merge with prior; never overwrite 'frozen')
  if (signals.length) {
    try {
      const prior = await pool.query('SELECT fraud_status, block_signals FROM users WHERE username = $1', [username]);
      if (!prior.rows.length) return { suspicious: signals.length > 0, signals };
      const priorSignals = (() => {
        try { return JSON.parse(prior.rows[0]?.block_signals || '{}')?.signals || []; }
        catch { return []; }
      })();
      const merged = [...priorSignals];
      for (const s of signals) {
        const idx = merged.findIndex(x => x.kind === s.kind);
        if (idx >= 0) merged[idx] = s; else merged.push(s);
      }
      const currentStatus = String(prior.rows[0].fraud_status || '').toLowerCase();
      const newStatus = currentStatus === 'frozen' ? 'frozen' : 'flagged';
      await pool.query(
        `UPDATE users
            SET fraud_status = $1,
                block_signals = $2,
                fraud_flagged_at = CASE WHEN fraud_status = 'flagged' THEN fraud_flagged_at ELSE $3 END,
                updated_at = $3
          WHERE username = $4`,
        [newStatus, JSON.stringify({ signals: merged, detectedAt: t }), t, username]
      );
      console.warn(`[MULTI-ACCOUNT] flagged ${username}:`, JSON.stringify(signals));
    } catch (e) { console.error('[SIGNAL] persist failed:', e.message); }
  }

  return { suspicious: signals.length > 0, signals };
}

// Backward-compatible alias — if called with a pre-hashed IP, that IP is
// ignored for history lookup (history uses the stable hash). Prefer
// recordAndDetectSignals() from new call sites.
async function detectMultiAccount(username, ipOrHash = null, fingerprint = null) {
  const looksHashed = ipOrHash && /^[a-f0-9]{64}$/i.test(String(ipOrHash));
  return recordAndDetectSignals(username, {
    ip: looksHashed ? null : (ipOrHash || null),
    fingerprint: fingerprint || null,
  });
}

// ============================================================================
// USER MANAGEMENT  (ENHANCED: signal recording on IP/FP change)
// ============================================================================
async function resolveReferralUid(referralCode, newUserUid) {
  const suppliedCode = safeUid(referralCode);
  const newUid = safeUid(newUserUid);
  // Accept both ref_tg_<id> and tg_<id>, resolving them to the same UID.
  const code = suppliedCode ? suppliedCode.replace(/^ref_(tg_\d{1,20})$/, '$1') : '';
  if (!code || !newUid || code === newUid) return null;
  // Telegram referral links use stable numeric Telegram IDs (tg_<id>), not
  // mutable @usernames. Resolve legacy IDs to the account's canonical UID.
  let result = null;
  const tgMatch = code.match(/^tg_(\d{1,20})$/);
  if (tgMatch) {
    const id = Number(tgMatch[1]);
    if (!Number.isSafeInteger(id) || id <= 0) return null;
    const r = await pool.query(
      `SELECT username FROM users WHERE telegram_id = $1 OR username = $2
       UNION SELECT uid AS username FROM telegram_users WHERE telegram_id = $1 AND uid IS NOT NULL
       LIMIT 1`, [id, code]
    );
    result = r.rows[0]?.username || null;
  } else {
    // Backward compatibility for previously shared referral links.
    const r = await pool.query('SELECT username FROM users WHERE username = $1', [code]);
    result = r.rows[0]?.username || null;
  }
  result = result ? safeUid(result) : null;
  return result && result !== newUid ? result : null;
}

async function ensureUser(username, referredBy = null, bot = false, extra = {}) {
  const clean = safeUid(username);
  if (!clean) return null;
  const client = await pool.connect();
  let signalPayload = null;
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
      if (extra.device_fingerprint) {
        const sanitized = sanitizeFingerprint(extra.device_fingerprint);
        if (sanitized && current.device_fingerprint !== sanitized) patch.device_fingerprint = sanitized;
      }
      if (Object.keys(patch).length) {
        const setClauses = Object.keys(patch).map((key, i) => `${key} = $${i + 2}`).join(', ');
        const values = [clean, ...Object.values(patch)];
        await client.query(`UPDATE users SET ${setClauses}, updated_at = $${values.length + 1} WHERE username = $1`, [...values, now()]);
      }
      if (extra.last_ip || extra.device_fingerprint) {
        signalPayload = { ip: extra.last_ip || null, fingerprint: extra.device_fingerprint || null };
      }
      return { ...current, ...patch };
    }
    const t = now();
    const sanitizedFp = extra.device_fingerprint ? sanitizeFingerprint(extra.device_fingerprint) : null;

    const newUser = {
      username: clean, display_name: extra.display_name || clean, telegram_id: extra.telegram_id || null,
      telegram_username: extra.telegram_username || null, main_wallet: 0,
      play_wallet: bot ? BOT_START_WALLET : 0,
      signup_bonus_remaining: 0,
      signup_bonus_original: 0,
      signup_bonus_expires_at: 0,
      withdrawal_hold_until: 0, withdrawal_hold_reason: null,
      pending_withdrawal: 0, referral_earnings: 0, wallet_updated_at: t, referred_by: await resolveReferralUid(referredBy, clean),
      last_ip_hash: extra.last_ip ? hashWithRotatingSalt(normalizedClientIp(extra.last_ip)) : null,
      device_fingerprint: sanitizedFp,
      created_at: t, updated_at: t
    };
    await client.query(`
      INSERT INTO users (username, display_name, telegram_id, telegram_username, main_wallet, play_wallet, signup_bonus_remaining, signup_bonus_original, signup_bonus_expires_at, withdrawal_hold_until, withdrawal_hold_reason, pending_withdrawal, referral_earnings, wallet_updated_at, referred_by, last_ip_hash, device_fingerprint, created_at, updated_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
    `, Object.values(newUser));
    if (extra.last_ip || extra.device_fingerprint) {
      signalPayload = {
        ip: extra.last_ip || null,
        fingerprint: extra.device_fingerprint || null,
      };
    }

    // New accounts receive no signup bonus.
    return newUser;
  } finally {
    client.release();
    // Async — never blocks user creation
    if (signalPayload && (signalPayload.ip || signalPayload.fingerprint)) {
      recordAndDetectSignals(clean, signalPayload).catch(e => console.error('[SIGNAL] post-ensure failed:', e.message));
    }
  }
}

async function resolveTelegramUser(user, referredBy = null, ctx = {}) {
  if (!user?.id) return null;
  const telegramId = Number(user.id);
  const telegramUsername = safeUser(user.username || '');
  const displayName = [user.first_name, user.last_name].filter(Boolean).join(' ').trim() || telegramUsername || `tg_${telegramId}`;
  const canonicalUid = `tg_${telegramId}`;
  const ip = ctx.ip || null;
  const fingerprint = ctx.fingerprint || null;
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
        return {
          uid: mappedUid,
          user: await ensureUser(mappedUid, referredBy, false, {
            telegram_id: telegramId, display_name: displayName,
            telegram_username: telegramUsername || null,
            last_ip: ip, device_fingerprint: fingerprint,
          })
        };
      }
    }
    // Recover a canonical tg_<id> account even when an older deployment did
    // not populate telegram_id/telegram_users. If the Telegram ID already
    // points to a legacy username, preserve that username: other financial,
    // referral, transaction and game records may reference it.
    const found = await client.query(
      `SELECT * FROM users
        WHERE username = $1 OR telegram_id = $2
        ORDER BY CASE WHEN telegram_id = $2 THEN 0 ELSE 1 END
        LIMIT 1 FOR UPDATE`,
      [canonicalUid, telegramId]
    );
    let finalUid = canonicalUid;
    if (found.rows.length > 0) {
      const row = found.rows[0];
      if (row.telegram_id && Number(row.telegram_id) !== telegramId) throw new Error('conflicting-user');
      finalUid = row.username; // Preserve established account identity and all related records.
      if (!row.telegram_id) {
        await client.query(
          'UPDATE users SET telegram_id = $1, updated_at = $2 WHERE username = $3 AND (telegram_id IS NULL OR telegram_id = $1)',
          [telegramId, now(), finalUid]
        );
      }
    }
    await client.query('COMMIT');
    const created = await ensureUser(finalUid, referredBy, false, {
      telegram_id: telegramId, display_name: displayName,
      telegram_username: telegramUsername || null,
      last_ip: ip, device_fingerprint: fingerprint,
    });
    await pool.query('INSERT INTO telegram_users (telegram_id, uid, username, telegram_username, updated_at) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (telegram_id) DO UPDATE SET uid=EXCLUDED.uid, username=EXCLUDED.username, updated_at=EXCLUDED.updated_at',
      [telegramId, finalUid, created.username, telegramUsername, now()]);
    return { uid: finalUid, user: created, telegramId, telegramUsername };
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('[IDENTITY] error', e.message);
    return null;
  } finally { client.release(); }
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
  const wsPayload = { ...data, type: 'notification', notificationType: dbType, id, timestamp };
  await pool.query(
    'INSERT INTO notifications (id, username, type, message, status, tx_type, tx_id, timestamp, read, dismiss_policy, created_by, broadcast_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,false,$9,$10,$11)',
    [id, username, dbType, String(data.message || ''), data.status || null, data.txType || null, data.txId || null, timestamp, data.dismissPolicy || 'user', data.createdBy || null, data.broadcastId || null]
  );
  const set = conns.get(username);
  if (set) for (const ws of set) if (ws.readyState === 1) ws.send(JSON.stringify(wsPayload));
}

// ─── FIX 1: differentiate rejection/approval by transaction type ────────────
async function notifyTransaction(username, tx) {
  const isDeposit  = tx.type === 'deposit';
  const isWithdraw = tx.type === 'withdraw';
  const label      = isDeposit ? 'Deposit' : isWithdraw ? 'Withdrawal' : 'Request';
  const labelLower = label.toLowerCase();

  let message;
  if (tx.status === 'approved') {
    if (isDeposit) {
      message = 'ትዕዛዙ ተፈፅሟል። የዊንዞ ቤተሰብ በመሆንዎ እናመሰግናለን። መልካም ጨዋታ!';
      const w = await walletFor(username);
      await sendBotDepositStatus(
        username,
        `✅ *Deposit Approved!*\n\nYour deposit of *${tx.amount} Birr* has been approved.\n\n💰 Main Wallet: *${w.main} Birr*\n🎮 Play Wallet: *${w.play} Birr*`
      );
    } else if (isWithdraw) {
      message = 'Withdrawal approved.';
      await sendBotDepositStatus(
        username,
        `✅ *Withdrawal Approved!*\n\nYour withdrawal of *${tx.amount} Birr* has been approved.`
      );
    } else {
      message = 'Request approved.';
      await sendBotDepositStatus(
        username,
        `✅ *${label} Approved!*\n\nYour ${labelLower} of *${tx.amount} Birr* has been approved.`
      );
    }
  } else {
    const reason = tx.reason || 'Invalid request';
    message = `${label} rejected by admin.`;
    await sendBotDepositStatus(
      username,
      `❌ *${label} Rejected*\n\nYour ${labelLower} request of *${tx.amount} Birr* was rejected.\nReason: ${reason}`
    );
  }

  await addNotification(username, {
    type: 'transaction_result',
    status: tx.status,
    txType: tx.type,
    txId: tx.id,
    reason: tx.reason || '',
    message,
  });

  const set = conns.get(username);
  if (set) for (const ws of set) if (ws.readyState === 1)
    ws.send(JSON.stringify({ type: 'transaction-status', id: tx.id, status: tx.status, txType: tx.type, reason: tx.reason || '' }));
}

async function recordWalletCredit(client, username, amount, type, meta = {}) {
  const id = crypto.randomUUID();
  const t = now();
  await client.query(
    `INSERT INTO transactions (id, username, uid, wallet_uid, type, amount, status, timestamp, reference, reason, wallet_applied, updated_at)
     VALUES ($1,$2,$2,$2,$3,$4,'approved',$5,$6,$7,1,$5)`,
    [id, username, type, Number(amount), t,
     String(meta.reference || '').slice(0, 128),
     String(meta.reason || '').slice(0, 256)]
  );
  return id;
}

// ============================================================================
// TRANSACTIONS  (ENHANCED: fresh-signal detection + flagged enforcement)
// ============================================================================
async function transactionRequest(uid, data) {
  const traceId = String(data.__traceId || newTraceId());
  const type = data.txType;
  const amount = Number(data.amount);
  const rawRef = normalizePaymentReference(data.reference);
  const clientRequestId = String(data.clientRequestId || '').slice(0, 128);
  const imgLen = String(data.imageData || '').length;

  await trace(traceId, uid, 'tx.received', 'ok', { type, amount, reference: rawRef, clientRequestId, detail: { imgLen, hasImage: imgLen > 0 } });

  if (!['deposit', 'withdraw'].includes(type)) {
    await trace(traceId, uid, 'tx.validate', 'fail', { type, reason: 'bad-type' });
    return { ok: false, reason: 'bad-type', traceId };
  }
  if (!Number.isFinite(amount) || amount <= 0 || amount > 1_000_000) {
    await trace(traceId, uid, 'tx.validate', 'fail', { type, amount, reason: 'bad-amount' });
    return { ok: false, reason: 'bad-amount', traceId };
  }

  // ─── ENHANCED: re-run detection with fresh WS signals ──────────────────
  try {
    const freshIp = data.__ip || null;
    const freshFp = data.__fingerprint || null;
    if (freshIp || freshFp) {
      await recordAndDetectSignals(uid, { ip: freshIp, fingerprint: freshFp });
    }
  } catch (e) { console.error('[TX-DETECT]', e.message); }

  if (type === 'deposit') {
    if (amount < MIN_DEPOSIT_AMOUNT || amount > MAX_DEPOSIT_AMOUNT) {
      await trace(traceId, uid, 'tx.ratelimit', 'fail', { type, amount, reason: 'amount-out-of-range' });
      return { ok: false, reason: 'amount-out-of-range', traceId };
    }
    const since = now() - DEPOSIT_RATE_LIMIT_WINDOW_MS;
    const recent = await pool.query('SELECT COUNT(*)::int AS c FROM deposit_attempts WHERE username = $1 AND created_at > $2', [uid, since]);
    if (recent.rows[0].c >= DEPOSIT_RATE_LIMIT_MAX) {
      await trace(traceId, uid, 'tx.ratelimit', 'fail', { type, amount, reason: 'rate-limited', detail: { count: recent.rows[0].c, max: DEPOSIT_RATE_LIMIT_MAX, windowMs: DEPOSIT_RATE_LIMIT_WINDOW_MS } });
      return { ok: false, reason: 'rate-limited', traceId };
    }
    await pool.query('INSERT INTO deposit_attempts (id, username, created_at) VALUES ($1,$2,$3)', [crypto.randomUUID(), uid, now()]);
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

    // ─── FIX 2: flagged accounts pause withdrawals ONLY for fingerprint clusters
    // IP-cluster flags (shared network / NAT) do NOT block withdrawals.
    // Fingerprint-cluster flags (shared device) DO block withdrawals (unchanged).
    if (type === 'withdraw' && String(user.fraud_status || '').toLowerCase() === 'flagged') {
      let flaggedSignals = [];
      try {
        const parsed = JSON.parse(user.block_signals || '{}');
        flaggedSignals = Array.isArray(parsed.signals) ? parsed.signals : [];
      } catch (_) { flaggedSignals = []; }

      const hasFingerprintFlag = flaggedSignals.some(
        s => s && String(s.kind) === 'fingerprint-cluster'
      );

      if (hasFingerprintFlag) {
        await trace(traceId, uid, 'tx.flagged', 'fail', { type, amount, reason: 'account-flagged-fp' });
        try { await client.query('ROLLBACK'); } catch (_) {}
        try {
          await addNotification(uid, {
            type: 'fraud_flag',
            message: '⚠️ Withdrawals are temporarily paused for identity verification. Please contact support.'
          });
        } catch (_) {}
        try {
          await sendBotDepositStatus(uid,
            `⚠️ *Withdrawal Paused*\n\nFor security reasons, we need to verify your account. Contact support: @winzo_help_center`);
        } catch (_) {}
        return { ok: false, reason: 'account-flagged', traceId };
      }

      // IP-cluster-only flag → withdrawal allowed. Log for audit trail.
      await trace(traceId, uid, 'tx.flagged-ip-only', 'warn', {
        type, amount, reason: 'ip-cluster-withdraw-allowed'
      });
    }

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
      const main = Number(user.main_wallet);
      const pending = Number(user.pending_withdrawal);
      
      if (main - pending < amount) throw new Error('insufficient-main');
      if (main - pending - amount < 15) throw new Error("በ 'Main Wallet' ላይ በትንሹ 15 ብር ቀሪ ሊያኖሩ ይገባል! እባክዎ በድጋሚ ይዘዙ");
      
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
    if (!committed) { try { await client.query('ROLLBACK'); } catch (_) {} }
    await trace(traceId, uid, 'tx.rollback', 'fail', { type, amount, reference: rawRef, reason: e.message });
    console.error('[TX-REQUEST] FAILED', { traceId, uid, type, amount, reason: e.message });
    
    if (e.message === 'reference-already-used') {
        await sendBotDepositStatus(uid, "❌ *Reference Error*\n\nይህ Reference code ከዚህ በፊት አገልግሎት ላይ የዋለ ነው።");
    }
    
    return { ok: false, reason: e.message, traceId };
  } finally { client.release(); }

  try { await sendWallet(uid); await trace(traceId, uid, 'tx.wallet-push', 'ok', { type, amount, txId }); }
  catch (e) { await trace(traceId, uid, 'tx.wallet-push', 'fail', { type, amount, txId, reason: e.message }); }

  if (type === 'deposit') {
    rescanPendingSms(traceId)
      .then(async () => {
        try {
          const statusCheck = await pool.query('SELECT status FROM transactions WHERE id = $1', [txId]);
          if (statusCheck.rows[0]?.status === 'pending') {
            await addNotification(uid, { type: 'notification', message: 'እባክዎ፣ ገንዘብ ገቢ ማድረግዎን ያረጋግጡና በድጋሚ ይሞክሩ።' });
            await trace(traceId, uid, 'tx.wait-msg', 'ok', { txId });
            await sendBotDepositStatus(uid, `⏳ *እባክዎ ትንሽ ይጠብቁ...*\n\nገንዘብ መላክዎትን የሚያሳይ መልእክት ወደ ሲሰተም አልደረሰም። (Ref: ${rawRef})`);
          }
        } catch (e) { console.error('[TX-WAIT-MSG]', e.message); }
      })
      .catch(err => { console.error('[TX-RESCAN]', err.message); trace(traceId, uid, 'tx.rescan', 'fail', { txId, reason: err.message }).catch(() => {}); });
    await trace(traceId, uid, 'tx.rescan', 'ok', { txId });
  }
  return { ok: true, id: txId, traceId, clientRequestId };
}

async function transactionImage(uid, id, imageData) {
  if (!id || !imageData || String(imageData).length > MAX_IMAGE_BYTES) return false;
  const res = await pool.query('UPDATE transactions SET image_data = $1, updated_at = $2 WHERE id = $3 AND username = $4 AND status = $5 RETURNING id',
    [String(imageData), now(), id, uid, 'pending']);
  return res.rowCount > 0;
}

async function adminListTransactions() {
  const res = await pool.query("SELECT * FROM transactions WHERE status = 'pending' ORDER BY timestamp DESC LIMIT 500");
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
    if (tx.type === 'deposit' && decision === 'approved') { newPlay += amount; }
    else if (tx.type === 'withdraw' && decision === 'approved') {
      if (newMain < amount || newPending < amount) throw new Error('insufficient-funds');
      newMain -= amount; newPending -= amount;
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
  } finally { client.release(); }
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
    const m = text.match(/(?:transaction|txn|ref(?:erence)?|የሂሳብ እንቅስቃሴ ቁጥር(?:ዎ)?)\s*(?:number|code|no\.?|id)?\s*(?:is|:|\-)?\s*([A-Z0-9]{6,})/i)
           || text.match(/receipt\/([A-Za-z0-9]{6,24})/i);
    if (m) reference = m[1];
  }
  
  let amount = Number(r.amount);
  if (!Number.isFinite(amount) || amount <= 0) {
    const m = text.match(/(?:ETB|Birr|ብር|Br\b|Amt|Amount)\s*[:\-]?\s*([0-9][0-9,]*(?:\.\d{1,2})?)/i)
           || text.match(/([0-9][0-9,]*(?:\.\d{1,2})?)\s*(?:ETB|Birr|ብር|Br\b)/i);
    if (m) amount = Number(String(m[1]).replace(/,/g, ''));
  }
  
  return { reference: normalizePaymentReference(reference), amount: Number.isFinite(amount) ? amount : 0, isTrusted: isTrustedSmsSender(r), text };
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
  
  if (!payment.reference) { await trace(traceId, null, 'auto.ref', 'fail', { reason: 'no-reference' }); return { ok: false, reason: 'no-reference', traceId }; }
  if (!isValidReference(payment.reference)) { await trace(traceId, null, 'auto.ref', 'fail', { reason: 'invalid-reference', reference: payment.reference }); return { ok: false, reason: 'invalid-reference', traceId }; }
  const smsAt = Number(smsRecord.timestamp || now());
  if (!Number.isFinite(smsAt) || (now() - smsAt) > SMS_MAX_AGE_MS) { await trace(traceId, null, 'auto.age', 'fail', { reason: 'stale-sms', reference: payment.reference }); return { ok: false, reason: 'stale-sms', traceId }; }
  if (!(payment.amount > 0)) { await trace(traceId, null, 'auto.amount', 'fail', { reason: 'no-amount', reference: payment.reference }); return { ok: false, reason: 'no-amount', traceId }; }
if (payment.amount < MIN_DEPOSIT_AMOUNT || payment.amount > MAX_DEPOSIT_AMOUNT) { await trace(traceId, null, 'auto.amount', 'fail', { reason: 'amount-out-of-range', reference: payment.reference, amount: payment.amount }); return { ok: false, reason: 'amount-out-of-range', traceId }; }
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
    if (!lockRes.rows[0]?.got) { await trace(traceId, null, 'auto.lock', 'fail', { reason: 'reference-locked', reference: payment.reference }); return { ok: false, reason: 'reference-locked', traceId }; }
    try {
      const recheck = await pool.query('SELECT tx_id FROM used_references WHERE reference = $1', [payment.reference]);
      if (recheck.rows.length > 0) { await trace(traceId, null, 'auto.reuse-recheck', 'fail', { reason: 'reference-reused', reference: payment.reference }); return { ok: false, reason: 'reference-reused', traceId }; }
      const txRes = await pool.query("SELECT * FROM transactions WHERE status = 'pending' AND type = 'deposit' AND timestamp > $1", [now() - DEPOSIT_MAX_AGE_MS]);
      const candidates = [];
      for (const tx of txRes.rows) {
        const txRef = normalizePaymentReference(tx.reference);
        if (!txRef || txRef !== payment.reference) continue;
        const txAmt = Number(tx.amount);
        if (!Number.isFinite(txAmt)) continue;
        if (Math.abs(txAmt - payment.amount) > AUTO_DEPOSIT_AMOUNT_TOLERANCE) continue;
        candidates.push(tx);
      }
      if (candidates.length === 0) { await logAutoActivity('skipped', smsId, payment.reference, payment.amount, '', 'no-match'); await trace(traceId, null, 'auto.match', 'fail', { reason: 'no-match', reference: payment.reference, amount: payment.amount, detail: { scanned: txRes.rows.length } }); return { ok: false, reason: 'no-match', traceId }; }
      if (candidates.length > 1) { await logAutoActivity('skipped', smsId, payment.reference, payment.amount, '', 'ambiguous-tx'); await trace(traceId, null, 'auto.match', 'fail', { reason: 'ambiguous-tx', reference: payment.reference, amount: payment.amount, detail: { count: candidates.length } }); return { ok: false, reason: 'ambiguous-tx', traceId }; }
      const tx = candidates[0];
      await trace(traceId, tx.username, 'auto.candidate', 'ok', { txId: tx.id, reference: payment.reference, amount: payment.amount });
      const owner = await pool.query('SELECT blocked, fraud_status, account_status FROM users WHERE username = $1', [tx.wallet_uid || tx.uid]);
      if (owner.rows.length === 0) { await logAutoActivity('skipped', smsId, payment.reference, payment.amount, tx.id, 'owner-missing'); return { ok: false, reason: 'owner-missing', traceId }; }
      const o = owner.rows[0];
      if (o.blocked) { await logAutoActivity('skipped', smsId, payment.reference, payment.amount, tx.id, 'owner-blocked'); return { ok: false, reason: 'owner-blocked', traceId }; }
      if (String(o.fraud_status || '').toLowerCase() === 'frozen') { await logAutoActivity('skipped', smsId, payment.reference, payment.amount, tx.id, 'owner-frozen'); return { ok: false, reason: 'owner-frozen', traceId }; }
      if (!smsMatchesDepositIdentity(payment.text, tx)) { await logAutoActivity('skipped', smsId, payment.reference, payment.amount, tx.id, 'identity-mismatch'); return { ok: false, reason: 'identity-mismatch', traceId }; }
      const reserve = await pool.query(
        `INSERT INTO used_references (reference, sms_id, tx_id, amount, sender, username, consumed_at) VALUES ($1,$2,$3,$4,$5,$6,$7)
         ON CONFLICT (reference) DO NOTHING RETURNING reference`,
        [payment.reference, smsId, tx.id, payment.amount, String(smsRecord.sender || smsRecord.from || ''), tx.username, now()]
      );
      if (reserve.rowCount === 0) { await logAutoActivity('skipped', smsId, payment.reference, payment.amount, tx.id, 'reference-reused'); return { ok: false, reason: 'reference-reused', traceId }; }
      const result = await adminDecision(tx.id, 'approved', `Auto-approved: SMS ${String(smsId).slice(0, 80)} (ref ${payment.reference})`);
      if (!result.ok) {
        await pool.query('DELETE FROM used_references WHERE reference = $1 AND tx_id = $2', [payment.reference, tx.id]);
        await logAutoActivity('error', smsId, payment.reference, payment.amount, tx.id, result.reason);
        return { ok: false, reason: result.reason, traceId };
      }
      await logAutoActivity('approved', smsId, payment.reference, payment.amount, tx.id, 'verified');
      await trace(traceId, tx.username, 'auto.decision', 'ok', { txId: tx.id, reference: payment.reference, amount: payment.amount });
      return { ok: true, txId: tx.id, tx: result.tx, traceId };
    } finally { try { await lockClient.query('SELECT pg_advisory_unlock($1)', [lockKey]); } catch {} }
  } finally { lockClient.release(); }
}

function advisoryKeyForReference(ref) {
  const h = crypto.createHash('sha256').update('ref:' + String(ref || '')).digest();
  return Number(h.readUIntBE(0, 6));
}

async function processIncomingSmsSnapshot(row, parentTraceId = null) {
  if (row.processed) return;
  const traceId = parentTraceId || newTraceId();
  try {
    const res = await autoApproveMatchingDeposits(row.id, row, traceId);
    const definitive = res.ok || ['untrusted-sender', 'no-reference', 'invalid-reference', 'no-amount', 'amount-out-of-range', 'stale-sms', 'reference-reused'].includes(res.reason);
    if (definitive) {
      try {
        await pool.query('UPDATE incoming_sms SET processed = true, matched_tx_id = COALESCE($1, matched_tx_id) WHERE id = $2', [res.ok ? res.txId : null, row.id]);
      } catch { try { await pool.query('UPDATE incoming_sms SET processed = true WHERE id = $1', [row.id]); } catch (_) {} }
    }
  } catch (e) { console.error('[AUTO-DEPOSIT]', e); }
}

async function rescanPendingSms(parentTraceId = null) {
  const traceId = parentTraceId || newTraceId();
  try {
    const res = await pool.query('SELECT * FROM incoming_sms WHERE processed = false ORDER BY timestamp ASC LIMIT 50');
    if (res.rows.length > 0) await trace(traceId, null, 'rescan', 'ok', { detail: { queued: res.rows.length } });
    for (const row of res.rows) await processIncomingSmsSnapshot(row, traceId);
  } catch (e) { console.error('[RESCAN-SMS]', e.message); }
}

function startAutomaticDepositMonitor() {
  setInterval(async () => {
    try {
      const res = await pool.query('SELECT * FROM incoming_sms WHERE processed = false ORDER BY timestamp ASC LIMIT 10');
      for (const row of res.rows) await processIncomingSmsSnapshot(row);
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
    await recordWalletCredit(client, uid, amount, `game_${kind}`, { reason: `${roomId} r${round}`, reference: key });
    await client.query('COMMIT');
    await sendWallet(uid);
    return true;
  } catch (e) { await client.query('ROLLBACK'); return false; }
  finally { client.release(); }
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
    await client.query(
      `UPDATE users SET main_wallet = COALESCE(main_wallet,0) + $1, referral_earnings = COALESCE(referral_earnings,0) + $1, wallet_updated_at = $2, updated_at = $2 WHERE username = $3`,
      [amount, now(), ref]
    );
    await client.query('INSERT INTO notifications (id, username, type, read, timestamp) VALUES ($1,$2,$3,true,$4)', [key, ref, 'referral_marker', now()]);
    await recordWalletCredit(client, ref, amount, 'referral_commission', { reason: `From ${winnerUid} in ${roomId} r${round}`, reference: key });
    await client.query('COMMIT');
    await addNotification(ref, { type: 'referral_commission', message: `🎁 You earned ${amount} Birr commission from a win.` });
    await sendWallet(ref);
    return true;
  } catch (e) { await client.query('ROLLBACK'); console.error('[REFERRAL] error:', e.message); return false; }
  finally { client.release(); }
}

async function recordHumanWinner({ username, displayName, roomId, round, winAmount, winningNumber, prizePool }) {
  try {
    await pool.query(
      `INSERT INTO game_winners (id, username, display_name, room_id, round, win_amount, winning_number, prize_pool, created_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (id) DO NOTHING`,
      [`win_${roomId}_${round}_${username}`, username, displayName || username, roomId, round, winAmount, winningNumber, prizePool, now()]
    );
  } catch (e) { console.error('[WINNER-RECORD] failed:', e.message); }
}

async function recordRoundSnapshot({ roomId, round, bet, humanCount, humanPicks, botCount, botPicks, totalPicks, prizePool, winningNumber, winnerCount, humanWinnerCount, humanWinAmount, humanPlayerIds, startedAt, endedAt }) {
  try {
    const id = `snap_${roomId}_${round}`;
    await pool.query(
      `INSERT INTO round_snapshots (id, room_id, round, bet, human_count, human_picks, bot_count, bot_picks, total_picks, prize_pool, winning_number, winner_count, human_winner_count, human_win_amount, human_player_ids, started_at, ended_at, created_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
       ON CONFLICT (id) DO UPDATE SET human_count = EXCLUDED.human_count, human_picks = EXCLUDED.human_picks, bot_count = EXCLUDED.bot_count, bot_picks = EXCLUDED.bot_picks, total_picks = EXCLUDED.total_picks, prize_pool = EXCLUDED.prize_pool, winning_number = EXCLUDED.winning_number, winner_count = EXCLUDED.winner_count, human_winner_count = EXCLUDED.human_winner_count, human_win_amount = EXCLUDED.human_win_amount, human_player_ids = EXCLUDED.human_player_ids, ended_at = EXCLUDED.ended_at`,
      [id, roomId, round, bet, humanCount, humanPicks, botCount, botPicks, totalPicks, prizePool, winningNumber, winnerCount, humanWinnerCount, humanWinAmount, JSON.stringify(humanPlayerIds || []), startedAt, endedAt, now()]
    );
    await pool.query(
      `INSERT INTO total_picks (id, game, room_id, round_id, stake, total_picks, recorded_at)
       VALUES ($1, 'spin', $2, $3, $4, $5, $6)
       ON CONFLICT (game, room_id, round_id) DO UPDATE
       SET stake = EXCLUDED.stake, total_picks = EXCLUDED.total_picks, recorded_at = EXCLUDED.recorded_at`,
      [`spin_${roomId}_${round}`, roomId, String(round), bet, humanPicks, endedAt]
    );
  } catch (e) { console.error('[ROUND-SNAPSHOT] failed:', e.message, { roomId, round }); }
}

// ============================================================================
// GAME ROOM (Number game with provably-fair commit-reveal)
// ============================================================================
// Indexed WebSocket membership avoids scanning every connected socket for
// each Spin room broadcast. The actual roomId property remains authoritative.
const roomSocketMembers = new Map([['room_15', new Set()], ['room_30', new Set()]]);
function setRoomSocketMembership(ws, roomId) {
  const previous = ws.roomId;
  if (previous && roomSocketMembers.has(previous)) roomSocketMembers.get(previous).delete(ws);
  if (roomId && roomSocketMembers.has(roomId)) roomSocketMembers.get(roomId).add(ws);
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
    this.winners = row?.winners_json ? JSON.parse(row.winners_json) : [];
    this.prizePool = Number(row?.prize_pool || 0);
    this.lastResult = row?.last_result_json ? JSON.parse(row.last_result_json) : null;
    this.mutex = new Mutex(); this.botTimers = []; this.dirty = true;
    this.roundStartedAt = now();
    this.serverSeed = row?.server_seed || null;
    this.serverSeedHash = row?.server_seed_hash || null;
    if (!this.serverSeed) {
      this.serverSeed = crypto.randomBytes(32).toString('hex');
      this.serverSeedHash = crypto.createHash('sha256').update(this.serverSeed).digest('hex');
    }
    if (row?.taken_json) for (const [n, uid] of Object.entries(JSON.parse(row.taken_json))) this.taken.set(Number(n), uid);
    if (row?.players_json) {
      const saved = JSON.parse(row.players_json);
      for (const [uid, p] of Object.entries(saved)) this.players.set(uid, { uid, picks: Array.isArray(p.picks) ? p.picks.map(Number) : [], isBot: !!p.isBot, active: !!p.active, displayName: String(p.displayName || uid) });
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
      serverSeedHash: this.serverSeedHash,
    };
  }
  async persist() {
    const players = {};
    for (const [uid, p] of this.players) players[uid] = { picks: [...p.picks], isBot: !!p.isBot, active: !!p.active, displayName: p.displayName || uid };
    const taken = Object.fromEntries(this.taken.entries());
    await pool.query(`
      INSERT INTO rooms (id, bet, phase, round, ends_at, winning_number, winners_json, prize_pool, last_result_json, taken_json, players_json, updated_at, server_seed, server_seed_hash)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
      ON CONFLICT (id) DO UPDATE SET
        phase = EXCLUDED.phase, round = EXCLUDED.round, ends_at = EXCLUDED.ends_at, winning_number = EXCLUDED.winning_number,
        winners_json = EXCLUDED.winners_json, prize_pool = EXCLUDED.prize_pool, last_result_json = EXCLUDED.last_result_json,
        taken_json = EXCLUDED.taken_json, players_json = EXCLUDED.players_json, updated_at = EXCLUDED.updated_at,
        server_seed = EXCLUDED.server_seed, server_seed_hash = EXCLUDED.server_seed_hash
    `, [this.id, this.bet, this.phase, this.round, this.endsAt, this.winningNumber, JSON.stringify(this.winners), this.prizePool,
        this.lastResult ? JSON.stringify(this.lastResult) : null, JSON.stringify(taken), JSON.stringify(players), now(),
        this.serverSeed, this.serverSeedHash]);
    this.dirty = false;
  }
  broadcast() {
    const members = roomSocketMembers.get(this.id);
    if (!members || members.size === 0) return;
    const payload = JSON.stringify({ type: 'state', room: this.id, state: this.snapshot() });
    for (const ws of members) {
      if (ws.readyState === 1 && ws.authed && ws.roomId === this.id) {
        try { ws.send(payload); } catch (e) { /* close handler removes stale members */ }
      } else {
        members.delete(ws);
      }
    }
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

async function debit(uid, amount) {
  const amt = toFiniteNumber(amount, 0);
  if (amt <= 0) return { ok: false, reason: 'bad-amount' };
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const res = await client.query('SELECT play_wallet FROM users WHERE username = $1 FOR UPDATE', [uid]);
    if (res.rows.length === 0) throw new Error('user-not-found');
    if (Number(res.rows[0].play_wallet) < amt) throw new Error('insufficient');
    await client.query('UPDATE users SET play_wallet = play_wallet - $1, signup_bonus_remaining = GREATEST(0,signup_bonus_remaining-GREATEST(0,$1-(play_wallet-signup_bonus_remaining))), updated_at = $2, wallet_updated_at = $2 WHERE username = $3', [amt, now(), uid]);
    await client.query('COMMIT');
    await sendWallet(uid);
    return { ok: true };
  } catch (e) {
    await client.query('ROLLBACK');
    await sendWallet(uid);
    return { ok: false, reason: e.message };
  } finally { client.release(); }
}

async function refundWager(uid, amount) {
  const amt = toFiniteNumber(amount, 0);
  if (amt <= 0) return false;
  try {
    await pool.query('UPDATE users SET play_wallet = play_wallet + $1, signup_bonus_remaining = CASE WHEN signup_bonus_expires_at > $2 THEN LEAST(signup_bonus_original, signup_bonus_remaining + $1) ELSE signup_bonus_remaining END, updated_at = $2, wallet_updated_at = $2 WHERE username = $3', [amt, now(), uid]);
    await sendWallet(uid);
    return true;
  } catch (e) { return false; }
}

async function pickIntent(room, uid, num) {
  return room.mutex.run(async () => {
    if (room.phase !== 'selection') return { ok: false, reason: 'not-selection' };
    if (!(num >= 1 && num <= 400)) return { ok: false, reason: 'range' };
    let p = room.players.get(uid);
    if (!p) { p = { uid, picks: [], isBot: false, active: true, displayName: uid }; room.players.set(uid, p); }
    p.active = true;
    if (p.picks.length >= MAX_PICKS) return { ok: false, reason: 'max-picks' };
    if (p.picks.includes(num)) return { ok: false, reason: 'duplicate' };
    if (room.taken.has(num)) return { ok: false, reason: 'taken' };
    const d = await debit(uid, room.bet);
    if (!d.ok) return d;
    room.taken.set(num, uid); p.picks.push(num); room.dirty = true;
    return { ok: true };
  });
}

async function unpickIntent(room, uid, num) {
  return room.mutex.run(async () => {
    if (room.phase !== 'selection') return { ok: false, reason: 'not-selection' };
    const p = room.players.get(uid);
    if (!p || !p.picks.includes(num) || room.taken.get(num) !== uid) return { ok: false, reason: 'not-picked' };
    if (!(await refundWager(uid, room.bet))) return { ok: false, reason: 'refund-failed' };
    p.picks = p.picks.filter(n => n !== num); room.taken.delete(num); room.dirty = true;
    return { ok: true };
  });
}

async function refundDisconnectedPlayer(room, p) {
  if (!p || p.isBot || !p.picks.length) return;
  await refundWager(p.uid, room.bet * p.picks.length);
  for (const n of p.picks) if (room.taken.get(n) === p.uid) room.taken.delete(n);
  p.picks = []; p.active = false; room.dirty = true;
}

async function toSpinning(room) {
  if (room.phase !== 'selection') return;
  const allWithPicks = [...room.players.values()].filter(p => p.picks.length > 0 && (p.isBot || p.active));
  if (!allWithPicks.length) return resetRound(room, 'no-picks');
  const allNums = [...new Set(allWithPicks.flatMap(p => p.picks))];
  if (!allNums.length) return resetRound(room, 'no-picks');

  const commitment = crypto.createHmac('sha256', Buffer.from(room.serverSeed, 'hex'))
    .update(`${room.id}:${room.round}:${allNums.join(',')}`)
    .digest();
  const winningNumber = allNums[commitment.readUInt32BE(0) % allNums.length];

  const winners = allWithPicks.filter(p => p.picks.includes(winningNumber)).map(p => p.uid);
  const totalPicksInRoom = allWithPicks.reduce((s, p) => s + p.picks.length, 0);
  const prizePool = Math.floor(totalPicksInRoom * room.bet * PAYOUT_RATE);
  const humans = allWithPicks.filter(p => !p.isBot);
  const bots = allWithPicks.filter(p => p.isBot);
  const humanWinners = winners.filter(uid => !room.players.get(uid)?.isBot);
  const roundStartedAt = room.roundStartedAt || (now() - SELECTION_SECONDS * 1000);

  room.winningNumber = winningNumber; room.winners = winners; room.prizePool = prizePool;
  room.phase = 'spinning'; room.endsAt = now() + SPINNING_SECONDS * 1000; room.dirty = true;

  try {
    await recordRoundSnapshot({
      roomId: room.id, round: room.round, bet: room.bet,
      humanCount: humans.length, humanPicks: humans.reduce((s, p) => s + p.picks.length, 0),
      botCount: bots.length, botPicks: bots.reduce((s, p) => s + p.picks.length, 0),
      // Analytics Total Picks is human-only; bot picks remain separately recorded in bot_picks.
      totalPicks: humans.reduce((sum, player) => sum + player.picks.length, 0), prizePool, winningNumber,
      winnerCount: winners.length, humanWinnerCount: humanWinners.length,
      humanWinAmount: winners.length && prizePool > 0 ? Math.floor(prizePool / winners.length) * humanWinners.length : 0,
      humanPlayerIds: humans.map(p => p.uid),
      startedAt: roundStartedAt, endedAt: now(),
    });
  } catch (e) { console.error('[ROUND-SNAPSHOT] persist failed:', e.message); }

  room.lastResult = {
    round: room.round, winningNumber,
    winners: winners.map(uid => room.players.get(uid)?.displayName || uid),
    winAmount: (winners.length && prizePool > 0) ? Math.floor(prizePool / winners.length) : 0,
    serverSeed: room.serverSeed, serverSeedHash: room.serverSeedHash,
    setAt: now()
  };
}

async function toResults(room) {
  if (room.phase !== 'spinning') return;
  const winners = Array.isArray(room.winners) ? room.winners : [];
  const prizePool = Number(room.prizePool || 0);
  if (winners.length && prizePool > 0) {
    const share = Math.floor(prizePool / winners.length);
    for (const uid of winners) {
      if (share <= 0) break;
      if (!(await claimPayout(uid, share, room.id, room.round, 'win'))) continue;
      const player = room.players.get(uid);
      if (player?.isBot) continue;
      await recordHumanWinner({ username: uid, displayName: player?.displayName || uid, roomId: room.id, round: room.round, winAmount: share, winningNumber: room.winningNumber, prizePool });
      try { await addNotification(uid, { message: `🏆 You won ${share} Birr in round ${room.round}!` }); }
      catch (e) { console.error('[WIN-NOTIFY]', e.message, { uid }); }
      try {
        const refRes = await pool.query('SELECT referred_by FROM users WHERE username = $1', [uid]);
        const refBy = refRes.rows[0]?.referred_by;
        if (refBy && refBy !== uid) {
          const commission = Math.floor(share * REFERRAL_RATE);
          if (commission > 0) await creditReferral(refBy, commission, room.id, room.round, uid);
        }
      } catch (e) { console.error('[REFERRAL] credit failed:', e.message, { uid, share }); }
    }
  }
  room.phase = 'results';
  room.endsAt = now() + RESULTS_SECONDS * 1000;
  room.dirty = true;
}

async function resetRound(room, why) {
  for (const p of room.players.values()) {
    if (p.isBot || !p.picks.length) continue;
    if (room.phase === 'selection' || !p.active) await refundWager(p.uid, room.bet * p.picks.length);
  }
  room.round += 1; room.phase = 'selection'; room.endsAt = now() + SELECTION_SECONDS * 1000;
  room.taken.clear(); room.winningNumber = null; room.winners = []; room.prizePool = 0; room.lastResult = null;
  for (const p of room.players.values()) { p.picks = []; p.active = false; }
  room.roundStartedAt = now();
  room.serverSeed = crypto.randomBytes(32).toString('hex');
  room.serverSeedHash = crypto.createHash('sha256').update(room.serverSeed).digest('hex');
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
    for (let i = pass.length - 1; i > 0; i--) { const j = crypto.randomInt(i + 1); [pass[i], pass[j]] = [pass[j], pass[i]]; }
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
          const candidate = 1 + crypto.randomInt(400);
          if (!room.taken.has(candidate) && !p.picks.includes(candidate)) { num = candidate; break; }
        }
        if (num == null) { for (let candidate = 1; candidate <= 400; candidate++) if (!room.taken.has(candidate)) { num = candidate; break; } }
        if (num == null) return;
        room.taken.set(num, uid); p.picks.push(num); room.dirty = true; room.broadcast();
      });
    } catch (err) { console.error('[BOT-PICK]', err); }
    if (room.phase !== 'selection' || index >= order.length) return;
    room.botTimers.push(setTimeout(runNextPick, 20));
  };
  room.botTimers.push(setTimeout(runNextPick, 50));
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
/* WINZO BINGO */
// ============================================================================
const BINGO_DEFAULT_STAKE = [15,30].includes(Number(process.env.BINGO_STAKE)) ? Number(process.env.BINGO_STAKE) : 15;
const BINGO_STAKES = [15, 30];
const BINGO_SELECTION_SECONDS = Math.max(15, Number(process.env.BINGO_SELECTION_SECONDS || 30));
const BINGO_CALL_INTERVAL_MS = Math.max(1000, Number(process.env.BINGO_CALL_INTERVAL_MS || 6000));
const BINGO_RESULT_SECONDS = Math.max(5, Number(process.env.BINGO_RESULT_SECONDS || 12));
const BINGO_BOT_COUNT = 10;
const bingoMembers = new Set();
const bingoRounds = new Map();
const bingoRoundCounters = new Map([[15, 0], [30, 0]]);

function bingoStake(value) { const n=Number(value); return BINGO_STAKES.includes(n) ? n : BINGO_DEFAULT_STAKE; }
function bingoId() { return crypto.randomBytes(5).toString('hex').toUpperCase(); }
function bingoNewCard() {
  const grid=Array.from({length:5},()=>Array(5).fill(0));
  for(let c=0;c<5;c++){const start=c*15+1,pool=Array.from({length:15},(_,i)=>start+i);for(let i=pool.length-1;i>0;i--){const j=crypto.randomInt(i+1);[pool[i],pool[j]]=[pool[j],pool[i]];}for(let r=0;r<5;r++)grid[r][c]=pool[r];}
  grid[2][2]=0;return grid;
}
function bingoPattern(grid,called,calledSet=null){
  const set=calledSet||new Set(called),marked=(r,c)=>r===2&&c===2||set.has(grid[r][c]);
  for(let r=0;r<5;r++)if([0,1,2,3,4].every(c=>marked(r,c)))return 'Horizontal Line';
  for(let c=0;c<5;c++)if([0,1,2,3,4].every(r=>marked(r,c)))return 'Vertical Line';
  if([0,1,2,3,4].every(i=>marked(i,i))||[0,1,2,3,4].every(i=>marked(i,4-i)))return 'Diagonal Line';
  if([[0,0],[0,4],[4,0],[4,4]].every(([r,c])=>marked(r,c)))return 'Four Corners';
  if(grid.every((row,r)=>row.every((n,c)=>marked(r,c))))return 'Full House';
  return null;
}
function bingoRoundFor(ws) { return bingoRounds.get(bingoStake(ws?._bingoStake)); }
function bingoWithRoundLock(round, fn) {
  const prior = round.operationQueue || Promise.resolve();
  const current = prior.catch(() => {}).then(fn);
  round.operationQueue = current.catch(() => {});
  return current;
}
function clearBingoBots(round){for(const t of (round.botTimers||[]))clearTimeout(t);round.botTimers=[];}
function bingoShuffle(pool){for(let i=pool.length-1;i>0;i--){const j=crypto.randomInt(i+1);[pool[i],pool[j]]=[pool[j],pool[i]];}return pool;}

// ─── FIX C4: prefer human card as guaranteed winner ────────────────────────
function bingoBuildGuaranteedPool(round, guaranteedWinnerCount = 1) {
  const allEntries = [...round.entries.values()].sort((a, b) => a.id.localeCompare(b.id));
  if (!allEntries.length) return bingoShuffle(Array.from({ length: 75 }, (_, i) => i + 1));
  // Bots participate in the same winner selection as real players.
  // The guaranteed winner is selected from every entered card, not humans only.
  const entries = allEntries;

  const entryFingerprint = crypto.createHash('sha256').update(entries.map(e => e.id).join(',')).digest('hex');
  const effectiveSeed = crypto.createHmac('sha256', Buffer.from(round.serverSeed, 'hex'))
    .update(`bingo:${round.id}:${round.roundNumber}:${entryFingerprint}`)
    .digest('hex');
  const rng = new SeededRNG(effectiveSeed);

  const shuffledEntries = seededShuffle([...entries], rng);
  const designated = shuffledEntries.slice(0, Math.min(guaranteedWinnerCount, shuffledEntries.length));
  const winnerCard = designated[0];

  const g = winnerCard.grid;
  const patternChoices = [];
  for (let r = 0; r < 5; r++) patternChoices.push([0,1,2,3,4].map(c => g[r][c]));
  for (let c = 0; c < 5; c++) patternChoices.push([0,1,2,3,4].map(r => g[r][c]));
  patternChoices.push([0,1,2,3,4].map(i => g[i][i]));
  patternChoices.push([0,1,2,3,4].map(i => g[i][4 - i]));

  const chosen = patternChoices[rng.nextInt(patternChoices.length)];
  const mustCall = chosen.filter(n => n > 0);
  const allNumbers = Array.from({ length: 75 }, (_, i) => i + 1);
  const fillerPool = seededShuffle(allNumbers.filter(n => !mustCall.includes(n)), rng);

  const pool = [];
  let mustIdx = 0;
  const maxMustPos = Math.min(30, mustCall.length * 5);
  for (let pos = 0; pos < 75 && (mustIdx < mustCall.length || fillerPool.length); pos++) {
    const placeMust = mustIdx < mustCall.length && (
      pos >= maxMustPos - mustCall.length + mustIdx || rng.nextInt(4) === 0
    );
    if (placeMust) pool.push(mustCall[mustIdx++]);
    else if (fillerPool.length) pool.push(fillerPool.shift());
  }
  while (mustIdx < mustCall.length) pool.push(mustCall[mustIdx++]);
  return pool;
}

function bingoForceWinner(round) {
  const allCards = [...round.entries.values()];
  if (!allCards.length) return null;
  let best = null, bestMissing = Infinity, bestPattern = null;
  for (const card of allCards) {
    const g = card.grid;
    const patterns = [];
    for (let r = 0; r < 5; r++) patterns.push({ name: 'Horizontal Line', nums: [0,1,2,3,4].map(c => g[r][c]) });
    for (let c = 0; c < 5; c++) patterns.push({ name: 'Vertical Line', nums: [0,1,2,3,4].map(r => g[r][c]) });
    patterns.push({ name: 'Diagonal Line', nums: [0,1,2,3,4].map(i => g[i][i]) });
    patterns.push({ name: 'Diagonal Line', nums: [0,1,2,3,4].map(i => g[i][4 - i]) });
    patterns.push({ name: 'Four Corners', nums: [g[0][0], g[0][4], g[4][0], g[4][4]] });
    patterns.push({ name: 'Full House', nums: g.flat() });
    for (const p of patterns) {
      const need = p.nums.filter(n => n > 0 && !round.called.includes(n));
      if (need.length < bestMissing) { bestMissing = need.length; best = card; bestPattern = p; }
      if (need.length === 0) return { ...card, pattern: p.name };
    }
  }
  if (!best || !bestPattern) return null;
  for (const n of bestPattern.nums) if (n > 0 && !round.called.includes(n)) round.called.push(n);
  return { ...best, pattern: bestPattern.name };
}

function scheduleBingoBots(round){
  clearBingoBots(round);if(!round||round.phase!=='selection')return;
  const shuffled=[...BOT_NAMES].sort(()=>0.5-Math.random());
  const active=shuffled.slice(0,Math.min(shuffled.length,BINGO_BOT_COUNT+Math.floor(Math.random()*3)));
  active.forEach((botName)=>{
    const timer=setTimeout(async()=>{
      if(bingoRounds.get(round.stake)!==round||round.phase!=='selection')return;
      try {
        await bingoWithRoundLock(round, async () => {
          if(bingoRounds.get(round.stake)!==round||round.phase!=='selection')return;
          const uid=botUid(botName);
          if([...round.entries.values()].some(e=>e.uid===uid))return;
          const taken=new Set([...round.entries.values()].map(e=>e.cardNumber));
          let card=crypto.randomInt(400)+1,attempts=0;
          while(taken.has(card)&&attempts++<100)card=crypto.randomInt(400)+1;
          if(taken.has(card))return;
          const grid=bingoNewCard(),id=crypto.randomUUID();
          const db=await pool.connect();
          try {
            await db.query('BEGIN');
            const locked=await db.query('SELECT phase FROM bingo_rounds WHERE id=$1 FOR UPDATE',[round.id]);
            if(!locked.rows.length||locked.rows[0].phase!=='selection'){await db.query('ROLLBACK');return;}
            await db.query('INSERT INTO bingo_entries(id,round_id,username,card_number,card_data,stake,created_at) VALUES($1,$2,$3,$4,$5::jsonb,$6,$7)',[id,round.id,uid,card,JSON.stringify(grid),round.stake,Date.now()]);
            await db.query('UPDATE bingo_rounds SET prize_pool=prize_pool+$1 WHERE id=$2',[round.stake,round.id]);
            await db.query('COMMIT');
          } catch(e){await db.query('ROLLBACK').catch(()=>{});throw e;} finally{db.release();}
          round.entries.set(id,{id,uid,name:botName,cardNumber:card,grid,stake:round.stake,won:false});
          round.prizePool+=round.stake;
          bingoBroadcast(round.stake);
        });
      } catch(e){console.error(`[BINGO-BOT ${round.stake}] pick failed`,e.message);}
    },1000+Math.random()*15000);
    round.botTimers.push(timer);
  });
}

async function bingoNewRound(stake){
  stake=bingoStake(stake);const counter=(bingoRoundCounters.get(stake)||0)+1;bingoRoundCounters.set(stake,counter);
  const serverSeed=crypto.randomBytes(32).toString('hex');
  const serverSeedHash=crypto.createHash('sha256').update(serverSeed).digest('hex');
  const round={stake,id:crypto.randomUUID(),gameId:'B'+bingoId(),roundNumber:counter,phase:'selection',prizePool:0,startedAt:Date.now(),endsAt:Date.now()+BINGO_SELECTION_SECONDS*1000,called:[],pool:bingoShuffle(Array.from({length:75},(_,i)=>i+1)),entries:new Map(),winners:[],botTimers:[],tickRunning:false,operationQueue:Promise.resolve(),serverSeed,serverSeedHash};
  bingoRounds.set(stake,round);await persistBingoRound(round);scheduleBingoBots(round);return round;
}

async function restoreOneBingoRound(stake){
  const latest=await pool.query("SELECT * FROM bingo_rounds WHERE stake=$1 AND phase IN ('selection','playing') ORDER BY started_at DESC LIMIT 1",[stake]);
  const counter=await pool.query('SELECT COALESCE(MAX(round_number),0) AS n FROM bingo_rounds WHERE stake=$1',[stake]);bingoRoundCounters.set(stake,Number(counter.rows[0]?.n||0));
  if(!latest.rows.length)return bingoNewRound(stake);
  const r=latest.rows[0];
  const round={stake,id:r.id,gameId:r.game_id,roundNumber:Number(r.round_number),phase:r.phase,prizePool:Number(r.prize_pool),startedAt:Number(r.started_at),endsAt:Number(r.ends_at),called:Array.isArray(r.called_numbers)?r.called_numbers:[],pool:[],entries:new Map(),winners:Array.isArray(r.winners)?r.winners:[],botTimers:[],tickRunning:false,operationQueue:Promise.resolve(),serverSeed:r.server_seed,serverSeedHash:r.server_seed_hash};
  if (!round.serverSeed) {
    round.serverSeed = crypto.randomBytes(32).toString('hex');
    round.serverSeedHash = crypto.createHash('sha256').update(round.serverSeed).digest('hex');
  }
  round.pool=bingoShuffle(Array.from({length:75},(_,i)=>i+1).filter(n=>!round.called.includes(n)));
  const entries=await pool.query('SELECT e.id,e.username,e.card_number,e.card_data,e.stake,u.display_name FROM bingo_entries e LEFT JOIN users u ON u.username=e.username WHERE e.round_id=$1',[round.id]),found=[];
  const restoredCalledSet=new Set(round.called);
  for(const e of entries.rows){const grid=typeof e.card_data==='string'?JSON.parse(e.card_data):e.card_data;const pattern=bingoPattern(grid,round.called,restoredCalledSet);const entry={id:e.id,uid:e.username,name:e.display_name||e.username,cardNumber:Number(e.card_number),grid,stake:Number(e.stake),won:!!pattern};round.entries.set(e.id,entry);if(pattern&&round.phase==='playing')found.push({...entry,pattern});}
  bingoRounds.set(stake,round);
  if(round.phase==='playing'&&found.length){await bingoFinish(round,found);return round;}
  if(round.phase==='selection'&&round.endsAt<=Date.now())round.endsAt=Date.now()+BINGO_SELECTION_SECONDS*1000;
  if(round.phase==='playing'&&round.endsAt<=Date.now())round.endsAt=Date.now()+BINGO_CALL_INTERVAL_MS;
  if(round.phase==='playing'&&(!round.pool||!round.pool.length))round.pool=bingoBuildGuaranteedPool(round,1);
  await persistBingoRound(round);if(round.phase==='selection')scheduleBingoBots(round);
  console.log(`[BINGO] Restored ${stake}-Birr round ${round.gameId} (${round.phase})`);return round;
}
async function restoreBingoRound(){for(const stake of BINGO_STAKES)await restoreOneBingoRound(stake);}

async function persistBingoRound(round){
  if(!round)return;
  await pool.query(
    `INSERT INTO bingo_rounds(id,game_id,round_number,phase,stake,prize_pool,called_numbers,started_at,ends_at,completed_at,winners,server_seed,server_seed_hash)
     VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10,$11::jsonb,$12,$13)
     ON CONFLICT(id) DO UPDATE SET phase=EXCLUDED.phase,prize_pool=EXCLUDED.prize_pool,called_numbers=EXCLUDED.called_numbers,ends_at=EXCLUDED.ends_at,completed_at=EXCLUDED.completed_at,winners=EXCLUDED.winners,server_seed=EXCLUDED.server_seed,server_seed_hash=EXCLUDED.server_seed_hash`,
    [round.id,round.gameId,round.roundNumber,round.phase,round.stake,round.prizePool,JSON.stringify(round.called),round.startedAt,round.endsAt,round.completedAt||null,JSON.stringify(round.winners),round.serverSeed,round.serverSeedHash]
  );
}

function bingoBuildBroadcastSnapshot(round){
  const entries=[...round.entries.values()];
  const byUid=new Map();
  for(const entry of entries){
    let mine=byUid.get(entry.uid);
    if(!mine){mine=[];byUid.set(entry.uid,mine);}
    mine.push(entry);
  }
  return {
    roundId:round.id,gameId:round.gameId,roundNumber:round.roundNumber,
    phase:round.phase,stake:round.stake,prizePool:Number(round.prizePool.toFixed(2)),
    called:[...round.called],taken:entries.map(e=>e.cardNumber),
    playerCount:byUid.size,endsAt:round.endsAt,winners:round.winners,byUid,
    serverSeedHash:round.serverSeedHash,
  };
}
function bingoPublicState(round,uid,snapshot=null){
  if(!round)return null;
  const shared=snapshot||bingoBuildBroadcastSnapshot(round);
  const mine=shared.byUid.get(uid)||[];
  return {
    roundId:shared.roundId,gameId:shared.gameId,roundNumber:shared.roundNumber,
    phase:shared.phase,stake:shared.stake,prizePool:shared.prizePool,
    called:shared.called,taken:shared.taken,playerCount:shared.playerCount,
    endsAt:shared.endsAt,mySelections:mine.map(e=>e.cardNumber),
    myCards:mine.map(e=>({id:e.cardNumber,grid:e.grid,won:!!e.won})),winners:shared.winners,
    serverSeedHash:shared.serverSeedHash,
  };
}
function bingoSendState(ws,snapshot=null){if(ws?.readyState===1&&ws.authed){const round=bingoRoundFor(ws);send(ws,{type:'bingo-state',state:bingoPublicState(round,ws.uid,snapshot)});}}
const bingoBroadcastPending = new Set();
function bingoBroadcastNow(stake){
  const snapshots=new Map();
  for(const ws of bingoMembers){
    if(ws.readyState!==1||!ws.authed)continue;
    const wsStake=bingoStake(ws._bingoStake);
    if(stake&&wsStake!==stake)continue;
    const round=bingoRounds.get(wsStake);
    if(!round){bingoSendState(ws);continue;}
    let snapshot=snapshots.get(round);
    if(!snapshot){snapshot=bingoBuildBroadcastSnapshot(round);snapshots.set(round,snapshot);}
    bingoSendState(ws,snapshot);
  }
}
function bingoBroadcast(stake){
  const key=stake?bingoStake(stake):'__all__';
  if(bingoBroadcastPending.has(key))return;
  bingoBroadcastPending.add(key);
  setImmediate(()=>{bingoBroadcastPending.delete(key);bingoBroadcastNow(key==='__all__'?null:key);});
}

async function bingoSelectUnlocked(ws,numbers){
  const round=bingoRoundFor(ws);if(!round||round.phase!=='selection'||Date.now()>=round.endsAt)return send(ws,{type:'bingo-error',message:'Card selection is closed for this round.'});
  if(!Array.isArray(numbers)||numbers.length<1||numbers.length>2||numbers.some(n=>!Number.isInteger(n)||n<1||n>400)||new Set(numbers).size!==numbers.length)return send(ws,{type:'bingo-error',message:'Choose one or two different card numbers from 1 to 400.'});
  for(const n of numbers)if([...round.entries.values()].some(e=>e.cardNumber===n))return send(ws,{type:'bingo-error',message:'Card number '+n+' has already been taken.'});
  const cost=Number((round.stake*numbers.length).toFixed(2)),db=await pool.connect(),newEntries=[];
  try{
    await db.query('BEGIN');
    const locked=await db.query('SELECT phase,ends_at FROM bingo_rounds WHERE id=$1 FOR UPDATE',[round.id]);
    if(!locked.rows.length||locked.rows[0].phase!=='selection'||Date.now()>=Number(locked.rows[0].ends_at))throw new Error('Card selection is closed for this round.');
    const usedRows=await db.query('SELECT card_number FROM bingo_entries WHERE round_id=$1 AND card_number = ANY($2::int[])',[round.id,numbers]),used=new Set(usedRows.rows.map(r=>Number(r.card_number)));
    const priorCountRes = await db.query('SELECT COUNT(*)::int AS c FROM bingo_entries WHERE round_id=$1 AND username=$2',[round.id,ws.uid]);
    const existingCount = priorCountRes.rows[0]?.c || 0;
    if (existingCount + numbers.length > 2) throw new Error('You can select up to 2 cards per round (you already have ' + existingCount + ').');
    for(const n of numbers)if(used.has(n))throw new Error('Card number '+n+' has already been taken.');
    const user=await db.query('SELECT play_wallet FROM users WHERE username=$1 FOR UPDATE',[ws.uid]);if(!user.rows.length)throw new Error('Account not found.');if(Number(user.rows[0].play_wallet)<cost)throw new Error('Insufficient play-wallet balance.');
    await db.query('UPDATE users SET play_wallet=play_wallet-$1,signup_bonus_remaining=GREATEST(0,signup_bonus_remaining-GREATEST(0,$1-(play_wallet-signup_bonus_remaining))),wallet_updated_at=$2 WHERE username=$3',[cost,Date.now(),ws.uid]);
    for(const n of numbers){
      const grid=bingoNewCard(),id=crypto.randomUUID();
      await db.query('INSERT INTO bingo_entries(id,round_id,username,card_number,card_data,stake,created_at) VALUES($1,$2,$3,$4,$5::jsonb,$6,$7)',[id,round.id,ws.uid,n,JSON.stringify(grid),round.stake,Date.now()]);
      await db.query('INSERT INTO bingo_wallet_transactions(id,round_id,username,type,amount,idempotency_key,created_at) VALUES($1,$2,$3,$4,$5,$6,$7)',[crypto.randomUUID(),round.id,ws.uid,'stake',round.stake,'bingo-stake:'+id,Date.now()]);
      newEntries.push({id,uid:ws.uid,name:ws.displayName||ws.uid,cardNumber:n,grid,stake:round.stake,won:false});
    }
    await db.query('UPDATE bingo_rounds SET prize_pool=prize_pool+$1 WHERE id=$2',[cost,round.id]);await db.query('COMMIT');
    for(const e of newEntries)round.entries.set(e.id,e);
    round.prizePool+=cost;
    send(ws,{type:'bingo-selected',message:'Selection confirmed. '+numbers.length+' card(s) reserved.',state:bingoPublicState(round,ws.uid)});
    Promise.resolve().then(()=>sendWallet(ws.uid)).catch(e=>console.error('[BINGO] wallet refresh failed',e.message));
    bingoBroadcast(round.stake);
  }catch(e){await db.query('ROLLBACK').catch(()=>{});send(ws,{type:'bingo-error',message:e.message||'Could not reserve your Bingo card.'});}finally{db.release();}
}
async function bingoSelect(ws,numbers){
  const round=bingoRoundFor(ws);
  if(!round)return send(ws,{type:'bingo-error',message:'The selected Bingo room is not ready yet.'});
  return bingoWithRoundLock(round,()=>bingoSelectUnlocked(ws,numbers));
}

async function bingoUnselectUnlocked(ws,number){
  const round=bingoRoundFor(ws);
  const n=Number(number);
  if(!round||round.phase!=='selection'||Date.now()>=round.endsAt)
    return send(ws,{type:'bingo-unselect-error',number:n,message:'Card selection is closed for this round.'});
  if(!Number.isInteger(n)||n<1||n>400)
    return send(ws,{type:'bingo-unselect-error',number:n,message:'Invalid card number.'});
  const db=await pool.connect();
  let removed=null;
  try{
    await db.query('BEGIN');
    const locked=await db.query('SELECT phase,ends_at FROM bingo_rounds WHERE id=$1 FOR UPDATE',[round.id]);
    if(!locked.rows.length||locked.rows[0].phase!=='selection'||Date.now()>=Number(locked.rows[0].ends_at))
      throw new Error('Card selection is closed for this round.');
    const entryRes=await db.query('SELECT id,stake FROM bingo_entries WHERE round_id=$1 AND username=$2 AND card_number=$3 FOR UPDATE',[round.id,ws.uid,n]);
    if(!entryRes.rows.length)throw new Error('That card is not reserved by your account.');
    removed=entryRes.rows[0];
    const refund=Number(removed.stake);
    const ledger=await db.query(
      `INSERT INTO bingo_wallet_transactions(id,round_id,username,type,amount,idempotency_key,created_at)
       VALUES($1,$2,$3,'refund',$4,$5,$6) ON CONFLICT(idempotency_key) DO NOTHING RETURNING id`,
      [crypto.randomUUID(),round.id,ws.uid,refund,'bingo-refund:'+removed.id,Date.now()]
    );
    if(ledger.rowCount){
      const upd=await db.query('UPDATE users SET play_wallet=play_wallet+$1,signup_bonus_remaining=CASE WHEN signup_bonus_expires_at>$2 THEN LEAST(signup_bonus_original,signup_bonus_remaining+$1) ELSE signup_bonus_remaining END,wallet_updated_at=$2 WHERE username=$3',[refund,Date.now(),ws.uid]);
      if(!upd.rowCount)throw new Error('Wallet refund failed.');
    }
    await db.query('DELETE FROM bingo_entries WHERE id=$1',[removed.id]);
    await db.query('UPDATE bingo_rounds SET prize_pool=GREATEST(0,prize_pool-$1) WHERE id=$2',[refund,round.id]);
    await db.query('COMMIT');
    round.entries.delete(removed.id);
    round.prizePool=Math.max(0,Number((round.prizePool-refund).toFixed(2)));
    send(ws,{type:'bingo-unselected',number:n,message:'Card '+n+' removed. '+refund+' Birr refunded to your play wallet.',state:bingoPublicState(round,ws.uid)});
    Promise.resolve().then(()=>sendWallet(ws.uid)).catch(e=>console.error('[BINGO] wallet refresh failed',e.message));
    bingoBroadcast(round.stake);
  }catch(e){
    await db.query('ROLLBACK').catch(()=>{});
    send(ws,{type:'bingo-unselect-error',number:n,message:e.message||'Could not remove this card.'});
  }finally{db.release();}
}
async function bingoUnselect(ws,number){
  const round=bingoRoundFor(ws);
  if(!round)return send(ws,{type:'bingo-unselect-error',number:Number(number),message:'The selected Bingo room is not ready yet.'});
  return bingoWithRoundLock(round,()=>bingoUnselectUnlocked(ws,number));
}

// Bingo prize distribution: all winning cards, including bot cards, share 80% equally.
// Real-player shares are credited to their wallets; bot shares are recorded as virtual winnings
// in bingo_results and the round winner payload (bots do not have real user wallets).
async function bingoFinish(round,winners){
  if(!round||round.phase!=='playing')return;
  round.phase='results';round.completedAt=Date.now();
  const eligible=[...new Map(winners.map(w=>[w.id,w])).values()];
  const prizeEach = eligible.length
    ? Math.floor((round.prizePool * 0.8 * 100) / eligible.length) / 100
    : 0;
  const db=await pool.connect();
  try{
    await db.query('BEGIN');
    for(const w of eligible){
      const isBot = String(w.uid).startsWith('__bot_');
      const prize = prizeEach;
      if(!isBot && prize>0){
        const ledger=await db.query('INSERT INTO bingo_wallet_transactions(id,round_id,username,type,amount,idempotency_key,created_at) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(idempotency_key) DO NOTHING RETURNING id',[crypto.randomUUID(),round.id,w.uid,'prize',prize,'bingo-prize:'+round.id+':'+w.id,Date.now()]);
        if(ledger.rowCount){
          const upd=await db.query('UPDATE users SET main_wallet=main_wallet+$1,wallet_updated_at=$2 WHERE username=$3',[prize,Date.now(),w.uid]);
          if(!upd.rowCount)throw new Error('Winner wallet update failed');
          await recordWalletCredit(db, w.uid, prize, 'bingo_win', { reason: `Bingo ${round.id} card ${w.cardNumber}`, reference: `bingo-prize:${round.id}:${w.id}` });
        }
      }
      // Bot prize amounts are persisted here as virtual winnings; no real wallet is credited.
      await db.query('INSERT INTO bingo_results(id,round_id,username,display_name,card_number,pattern,prize,call_number,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(round_id,username,card_number) DO UPDATE SET prize=EXCLUDED.prize,pattern=EXCLUDED.pattern,call_number=EXCLUDED.call_number',[w.id,round.id,w.uid,w.name,w.cardNumber,w.pattern,prize,round.called.at(-1)||null,Date.now()]);
      w.prize=prize;
    }
    // Total picks counts card-number selections by real players only; bot cards are excluded.
    await db.query(
      `INSERT INTO total_picks (id, game, room_id, round_id, stake, total_picks, recorded_at)
       SELECT $1, 'bingo', $2, $3, $4, COUNT(*)::int, $5
       FROM bingo_entries
       WHERE round_id = $3 AND LEFT(username, 6) <> '__bot_'
       ON CONFLICT (game, room_id, round_id) DO UPDATE
       SET stake = EXCLUDED.stake, total_picks = EXCLUDED.total_picks, recorded_at = EXCLUDED.recorded_at`,
      [`bingo_${round.id}`, `room_${round.stake}`, round.id, round.stake, Date.now()]
    );
    await db.query('UPDATE bingo_rounds SET phase=$1,completed_at=$2,winners=$3::jsonb WHERE id=$4',['results',Date.now(),JSON.stringify(eligible),round.id]);
    await db.query('COMMIT');
  }catch(e){await db.query('ROLLBACK').catch(()=>{});console.error('[BINGO-PRIZE]',e.message);}finally{db.release();}

  round.winners=eligible;
  const winnersPayload = eligible.map(w => ({ name: w.name, prize: w.prize, cardNumber: w.cardNumber, grid: w.grid, pattern: w.pattern }));
  const calledSnapshot = [...round.called];
  for (const ws of bingoMembers) {
    if (ws.readyState === 1 && ws.authed && bingoStake(ws._bingoStake) === round.stake) {
      send(ws, {
        type: 'bingo-winner-popup',
        winners: winnersPayload, called: calledSnapshot,
        winningNumber: calledSnapshot.length ? calledSnapshot[calledSnapshot.length - 1] : null,
        roundId: round.id, gameId: round.gameId, prizePool: round.prizePool,
        serverSeed: round.serverSeed, serverSeedHash: round.serverSeedHash,
      });
    }
  }
  for(const uid of new Set(eligible.filter(w=>!String(w.uid).startsWith('__bot_')&&w.prize>0).map(w=>w.uid))){
    await sendWallet(uid);
  }
  await persistBingoRound(round);bingoBroadcast(round.stake);clearBingoBots(round);
  setTimeout(async()=>{if(bingoRounds.get(round.stake)===round&&round.phase==='results'){await bingoNewRound(round.stake);bingoBroadcast(round.stake);}},BINGO_RESULT_SECONDS*1000);
}

async function bingoTickUnlocked(round) {
  if (!round) return;
  try {
    if (round.phase === 'selection' && Date.now() >= round.endsAt) {
      if (!round.entries.size) { await bingoNewRound(round.stake); bingoBroadcast(round.stake); return; }
      round.phase = 'playing';
      round.endsAt = Date.now() + BINGO_CALL_INTERVAL_MS;
      round.pool = bingoBuildGuaranteedPool(round, 1);
      await persistBingoRound(round);
      bingoBroadcast(round.stake);
      return;
    }
    if (round.phase === 'playing' && Date.now() >= round.endsAt) {
      const n = round.pool.shift();
      if (n == null) { const forced = bingoForceWinner(round); await bingoFinish(round, forced ? [forced] : []); return; }
      round.called.push(n);
      const winners = [];
      const calledSet = new Set(round.called);
      for (const e of round.entries.values()) {
        const pattern = bingoPattern(e.grid, round.called, calledSet);
        if (pattern && !e.won) { e.won = true; winners.push({ ...e, pattern }); }
      }
      round.endsAt = Date.now() + BINGO_CALL_INTERVAL_MS;
      const tooManyCalls = round.called.length >= 45;
      const nearPoolEnd = round.pool.length <= 5;
      if (winners.length || !round.pool.length || tooManyCalls || nearPoolEnd) {
        if (!winners.length) { const forced = bingoForceWinner(round); if (forced) winners.push(forced); }
        await bingoFinish(round, winners); return;
      }
      await persistBingoRound(round);
      bingoBroadcast(round.stake);
    }
  } finally { round.tickRunning = false; }
}
async function bingoTick(round){
  if(!round||round.tickRunning)return;
  round.tickRunning=true;
  return bingoWithRoundLock(round,()=>bingoTickUnlocked(round)).catch(e=>{round.tickRunning=false;throw e;});
}
setInterval(()=>{for(const round of bingoRounds.values())bingoTick(round).catch(e=>console.error(`[BINGO-TICK ${round.stake}]`,e.message));},250);

// ============================================================================
// WINZO LADDER GAME (ENHANCED)
// ============================================================================
const LADDER_STAKES = [15, 30];
const LADDER_TARGET = 100;
const LADDER_SNAKES = {71: 50, 82: 62, 56: 36};
const LADDER_LADDERS = {24: 44, 33: 68};
const ladderRooms = new Map();
// Keep per-match and lobby socket indexes so updates are O(match members),
// not O(all connected users), during large multi-match sessions.
const ladderRoomSockets = new Map();
const ladderLobbySockets = new Set();
function setLadderRoomSocket(ws, roomId) {
  const previous = ws._ladderRoomId;
  if (previous) {
    const oldSet = ladderRoomSockets.get(previous);
    if (oldSet) { oldSet.delete(ws); if (!oldSet.size) ladderRoomSockets.delete(previous); }
  }
  ws._ladderRoomId = roomId || null;
  if (roomId) {
    let set = ladderRoomSockets.get(roomId);
    if (!set) { set = new Set(); ladderRoomSockets.set(roomId, set); }
    set.add(ws);
  }
}
function setLadderLobbySocket(ws, enabled) {
  ws._ladderLobby = !!enabled;
  if (ws._ladderLobby) ladderLobbySockets.add(ws);
  else ladderLobbySockets.delete(ws);
}

const LADDER_INACTIVITY_WARN_MS = 45 * 1000;
const LADDER_INACTIVITY_FORFEIT_75_MS = 2 * 60 * 1000;
const LADDER_INACTIVITY_MAX_MS = 5 * 60 * 1000;
const LADDER_ABANDON_GRACE_MS = 2 * 60 * 1000;
const LADDER_AFK_WARN_MESSAGE = 'እንስቃሴ ሳያደርጉ ከ45 ሰከንድ በላይ ወስደዋል። ወደ ጨዋታ ካልተመለሱ፣ የርሶ ተጋጣሚ አሸናፊ ይሆናል።';
const LADDER_OPP_WARN_MESSAGE = 'የእርሶ ተጋጣሚ ጨዋታውን እያስቀጠለ አይደለም። ወደ ጨዋታው ካለተመለሰ፣ እርሶ እንዳሸነፉ የሚቆጠር ይሆናል።';

function getCombinedSnakes(room) {
    const snakes = { ...LADDER_SNAKES };
    if (room && room.players) {
        for (const p of Object.values(room.players)) {
            if (p && Array.isArray(p.virtualSnakes)) {
                for (const head of p.virtualSnakes) snakes[head] = head - 20;
            }
        }
    }
    return snakes;
}

function calculateLadderWinChance(playerPos, opponentPos) {
    const a = Number(playerPos || 1);
    const b = Number(opponentPos || 1);
    const diff = a - b;
    return Math.max(0.05, Math.min(0.95, 0.5 + diff * 0.01));
}

function notifyLadderPlayer(roomId, uid, message) {
    try {
        if (!uid) return;
        const ws = Array.from(conns.get(uid) || []).find(w => w._ladderRoomId === roomId && w.readyState === 1);
        if (ws) send(ws, { type: 'ladder-notification', message });
    } catch (e) { console.error('[LADDER-NOTIFY]', e.message); }
}

function awardLadderWin(room, winnerUid) {
    try {
        if (!room || !winnerUid) return;
        room.phase = 'results';
        room.winner = winnerUid;
        room.abandonment = null;
        if (room._timeout) { clearTimeout(room._timeout); room._timeout = null; }
        const prize = Math.floor(room.prizePool * 0.8);
        if (prize > 0 && !String(winnerUid).startsWith('__bot_')) {
            claimPayout(winnerUid, prize, `ladder_${room.id}`, 1, 'win')
                .catch(e => console.error('[LADDER-PAYOUT]', e.message));
        }
        const loserUid = winnerUid === room.creatorUid ? room.joinerUid : room.creatorUid;
        notifyLadderPlayer(room.id, winnerUid, `🎉 እንኳን ደስ አለዎት! ${prize} ብር አሸንፈዋል።`);
        if (loserUid) notifyLadderPlayer(room.id, loserUid, 'ጨዋታው ተጠናቅቋል። በሚቀጥለው ጊዜ ይሞክሩ።');
        try { broadcastLadder(room); } catch (e) {}
        setTimeout(() => { try { ladderRooms.delete(room.id); } catch (e) {} }, 12000);
    } catch (e) { console.error('[LADDER-AWARD]', e.message); }
}

function startLadderAbandonGrace(room, leaverUid) {
    try {
        if (!room || room.phase !== 'playing') return;
        const remainingUid = leaverUid === room.creatorUid ? room.joinerUid : room.creatorUid;
        if (!remainingUid) {
            if (room._timeout) { clearTimeout(room._timeout); room._timeout = null; }
            ladderRooms.delete(room.id);
            return;
        }
        const leaverPos = room.players[leaverUid]?.pos || 1;
        const remainingPos = room.players[remainingUid]?.pos || 1;
        const remainingChance = calculateLadderWinChance(remainingPos, leaverPos);

        if (remainingChance < 0.75) {
            awardLadderWin(room, remainingUid);
            return;
        }

        room.abandonment = {
            leaverUid, remainingUid,
            at: Date.now(),
            deadline: Date.now() + LADDER_ABANDON_GRACE_MS
        };
        if (room._timeout) { clearTimeout(room._timeout); room._timeout = null; }

        notifyLadderPlayer(room.id, remainingUid, LADDER_OPP_WARN_MESSAGE);
        notifyLadderPlayer(room.id, leaverUid, LADDER_AFK_WARN_MESSAGE);

        try { broadcastLadder(room); } catch (e) {}
        console.log(`[LADDER-GRACE] ${room.id}: ${leaverUid} abandoned, ${remainingUid} has ${(remainingChance*100).toFixed(1)}% chance.`);
    } catch (e) { console.error('[LADDER-GRACE]', e.message); }
}

function resetLadderActivity(room, uid) {
    if (!room || !uid) return;
    room.lastActionAt = room.lastActionAt || {};
    room.lastActionAt[uid] = Date.now();
    room.inactivityWarned = room.inactivityWarned || {};
    room.inactivityWarned[uid] = false;
    if (room.abandonment && room.abandonment.leaverUid === uid) {
        room.abandonment = null;
    }
}

function getLadderSnapshot(room) {
  return {
    id: room.id, bet: room.bet, phase: room.phase,
    creatorUid: room.creatorUid, joinerUid: room.joinerUid,
    multiplier: room.multiplier, currentTurn: room.currentTurn,
    players: room.players, lastRoll: room.lastRoll,
    lastMovePath: room.lastMovePath || null,
    winner: room.winner, prizePool: room.prizePool,
    displayPrize: Math.floor(room.prizePool * 0.8),
    pendingJoiners: Array.from(room.pendingJoiners.entries()).map(([uid, name]) => ({ uid, name })),
    abandonment: room.abandonment ? {
      leaverUid: room.abandonment.leaverUid,
      remainingUid: room.abandonment.remainingUid,
      deadline: room.abandonment.deadline,
      remainingMs: Math.max(0, room.abandonment.deadline - Date.now())
    } : null
  };
}

function broadcastLadder(room) {
  try {
    const payload = JSON.stringify({ type: 'ladder-state', room: getLadderSnapshot(room) });
    const members = ladderRoomSockets.get(room.id);
    if (!members) return;
    for (const ws of members) {
      if (ws.readyState === 1 && ws.authed && ws._ladderRoomId === room.id) {
        try { ws.send(payload); } catch (e) {}
      } else members.delete(ws);
    }
    if (!members.size) ladderRoomSockets.delete(room.id);
  } catch (e) { console.error('[LADDER-BROADCAST]', e.message); }
}

function broadcastLadderLobby() {
  try {
    const openRooms = Array.from(ladderRooms.values())
      .filter(r => r.phase === 'waiting')
      .map(r => ({
        id: r.id, bet: r.bet,
        creatorName: r.players[r.creatorUid]?.name || 'Unknown',
        isBot: r.creatorUid.startsWith('__bot_')
      }));
    const payload = JSON.stringify({ type: 'ladder-lobby', rooms: openRooms });
    for (const ws of ladderLobbySockets) {
      if (ws.readyState === 1 && ws.authed && ws._ladderLobby) {
        try { ws.send(payload); } catch (e) {}
      } else ladderLobbySockets.delete(ws);
    }
  } catch (e) { console.error('[LADDER-LOBBY-BROADCAST]', e.message); }
}

async function executeLadderRoll(room, uid) {
    if (!room || room.phase !== 'playing' || room.currentTurn !== uid) return;
    if (room._timeout) { clearTimeout(room._timeout); room._timeout = null; }

    resetLadderActivity(room, uid);

    const dice = Math.floor(Math.random() * 6) + 1;
    room.lastRoll = dice;
    const p = room.players[uid];
    if (!p) return;

    const startPos = p.pos;
    const actualDiceTarget = Math.min(startPos + dice, LADDER_TARGET);
    const path = [];
    for (let i = startPos + 1; i <= actualDiceTarget; i++) path.push(i);

    const snakes = getCombinedSnakes(room);
    let finalPos = actualDiceTarget;
    if (snakes[actualDiceTarget]) { finalPos = snakes[actualDiceTarget]; path.push(finalPos); }
    else if (LADDER_LADDERS[actualDiceTarget]) { finalPos = LADDER_LADDERS[actualDiceTarget]; path.push(finalPos); }

    p.pos = finalPos;
    room.lastMovePath = [startPos, ...path];
    room.phase = 'moving';
    try { broadcastLadder(room); } catch (e) {}

    setTimeout(() => {
        try {
            // ─── FIX C5: abort if room was terminalized during the 5s animation
            if (room.phase !== 'moving') return;
            if (!room.players[uid]) return;
            if (p.pos >= LADDER_TARGET) {
                awardLadderWin(room, uid);
                return;
            }
            room.phase = 'playing';
            room.lastMovePath = null;
            room.currentTurn = (uid === room.creatorUid) ? room.joinerUid : room.creatorUid;
            resetLadderActivity(room, room.currentTurn);
            try { broadcastLadder(room); } catch (e) {}

            if (room.currentTurn && room.currentTurn.startsWith('__bot_')) {
                setTimeout(() => {
                    try {
                        if (room.phase === 'playing' && room.currentTurn && room.currentTurn.startsWith('__bot_')) {
                            executeLadderRoll(room, room.currentTurn).catch(e => console.error('[LADDER-BOT-ROLL]', e.message));
                        }
                    } catch (e) { console.error('[LADDER-BOT-ROLL]', e.message); }
                }, 1500 + Math.random() * 1000);
            }
        } catch (e) { console.error('[LADDER-ROLL-CB]', e.message); }
    }, 5000);
}

async function handleLadderLeave(ws, isDisconnect = false) {
    try {
        const roomId = ws._ladderRoomId;
        if (!roomId) return;
        const room = ladderRooms.get(roomId);
        if (!room) {
            setLadderRoomSocket(ws, null);
            if (!isDisconnect) try { send(ws, { type: 'ladder-left', reason: 'left' }); } catch (e) {}
            return;
        }

        if (room.pendingJoiners.has(ws.uid)) {
            room.pendingJoiners.delete(ws.uid);
            setLadderRoomSocket(ws, null);
            if (!isDisconnect) try { send(ws, { type: 'ladder-left', reason: 'cancelled' }); } catch (e) {}
            try { broadcastLadder(room); } catch (e) {}
            return;
        }

        if (room.phase === 'waiting' || room.phase === 'betting') {
            if (room.creatorUid === ws.uid) {
                const joinerWs = Array.from(conns.get(room.joinerUid) || []).find(w => w._ladderRoomId === roomId);
                if (joinerWs) { setLadderRoomSocket(joinerWs, null); try { send(joinerWs, { type: 'ladder-left', reason: 'creator-left' }); } catch (e) {} }
                if (room._waitTimeout) clearTimeout(room._waitTimeout);
                ladderRooms.delete(room.id);
                broadcastLadderLobby();   // FIX S5
            } else if (room.joinerUid === ws.uid) {
                delete room.players[room.joinerUid];
                room.joinerUid = null;
                room.phase = 'waiting';
                room.multiplier = 1;
                if (room._waitTimeout) clearTimeout(room._waitTimeout);
                room._waitTimeout = setTimeout(() => {
                    try {
                        if (room.phase === 'waiting') {
                            ladderRooms.delete(room.id);
                            const creatorWs = Array.from(conns.get(room.creatorUid) || []).find(w => w._ladderRoomId === room.id);
                            if (creatorWs) { setLadderRoomSocket(creatorWs, null); send(creatorWs, { type: 'ladder-left', reason: 'timeout' }); }
                            broadcastLadderLobby();
                        }
                    } catch (e) { console.error('[LADDER-WAIT-TIMEOUT]', e.message); }
                }, 5 * 60 * 1000);
                try { broadcastLadder(room); } catch (e) {}
            }
        } else if (room.phase === 'playing' || room.phase === 'moving') {
            setLadderRoomSocket(ws, null);
            if (!isDisconnect) try { send(ws, { type: 'ladder-left', reason: 'left' }); } catch (e) {}
            startLadderAbandonGrace(room, ws.uid);
            try { broadcastLadderLobby(); } catch (e) {}
            return;
        }

        setLadderRoomSocket(ws, null);
        if (!isDisconnect) try { send(ws, { type: 'ladder-left', reason: 'left' }); } catch (e) {}
        try { broadcastLadderLobby(); } catch (e) {}
    } catch (e) {
        console.error('[LADDER-LEAVE]', e.message);
    }
}

async function handleLadderRejoin(ws, roomId) {
    try {
        const room = ladderRooms.get(roomId);
        if (!room) return send(ws, { type: 'ladder-error', message: 'Room no longer exists' });
        if (!room.players[ws.uid]) return send(ws, { type: 'ladder-error', message: 'You are not a player in this room' });

        setLadderRoomSocket(ws, roomId);
        setLadderLobbySocket(ws, false);

        if (room.abandonment && room.abandonment.leaverUid === ws.uid && room.phase === 'playing') {
            room.abandonment = null;
            room.phase = 'playing';
            if (room._timeout) { clearTimeout(room._timeout); room._timeout = null; }
            const opponentUid = ws.uid === room.creatorUid ? room.joinerUid : room.creatorUid;
            if (opponentUid) notifyLadderPlayer(roomId, opponentUid, 'ተጋጣሚዎ ወደ ጨዋታው ተመልሷል። ጨዋታው ይቀጥላል!');
            resetLadderActivity(room, ws.uid);
            if (opponentUid) resetLadderActivity(room, opponentUid);
        }
        try { broadcastLadder(room); } catch (e) {}
    } catch (e) { console.error('[LADDER-REJOIN]', e.message); }
}

setInterval(() => {
    try {
        const t = Date.now();
        for (const [roomId, room] of ladderRooms) {
            try {
                if (!room) continue;

                if (room.abandonment && room.phase === 'playing' && t >= room.abandonment.deadline) {
                    console.log(`[LADDER-GRACE-EXPIRED] ${roomId}: awarding ${room.abandonment.remainingUid}`);
                    awardLadderWin(room, room.abandonment.remainingUid);
                    continue;
                }

                if (room.phase === 'playing' && room.currentTurn && !room.currentTurn.startsWith('__bot_')) {
                    const lastAct = room.lastActionAt?.[room.currentTurn] || room.roundStartedAt || 0;
                    const inactiveMs = t - lastAct;
                    const opponentUid = room.currentTurn === room.creatorUid ? room.joinerUid : room.creatorUid;

                    if (inactiveMs >= LADDER_INACTIVITY_WARN_MS && !room.inactivityWarned?.[room.currentTurn]) {
                        room.inactivityWarned = room.inactivityWarned || {};
                        room.inactivityWarned[room.currentTurn] = true;
                        notifyLadderPlayer(roomId, room.currentTurn, LADDER_AFK_WARN_MESSAGE);
                        if (opponentUid) notifyLadderPlayer(roomId, opponentUid, LADDER_OPP_WARN_MESSAGE);
                        console.log(`[LADDER-AFK-WARN] ${roomId}: ${room.currentTurn} idle ${Math.floor(inactiveMs/1000)}s`);
                    }

                    if (inactiveMs >= LADDER_INACTIVITY_FORFEIT_75_MS && opponentUid) {
                        const inactivePos = room.players[room.currentTurn]?.pos || 1;
                        const opponentPos = room.players[opponentUid]?.pos || 1;
                        const opponentChance = calculateLadderWinChance(opponentPos, inactivePos);
                        if (opponentChance >= 0.75) {
                            console.log(`[LADDER-AFK-FORFEIT-75] ${roomId}: ${room.currentTurn} forfeited`);
                            awardLadderWin(room, opponentUid);
                            continue;
                        }
                    }

                    if (inactiveMs >= LADDER_INACTIVITY_MAX_MS && opponentUid) {
                        console.log(`[LADDER-AFK-FORFEIT-MAX] ${roomId}: ${room.currentTurn} forfeited (5min)`);
                        awardLadderWin(room, opponentUid);
                        continue;
                    }
                }
            } catch (e) { console.error(`[LADDER-TICK ${roomId}]`, e.message); }
        }
    } catch (e) { console.error('[LADDER-TICK-LOOP]', e.message); }
}, 5000);

setInterval(() => {
    try {
        const openBots = Array.from(ladderRooms.values()).filter(r => r.phase === 'waiting' && r.creatorUid.startsWith('__bot_'));
        if (openBots.length < 3) {
            const bName = BOT_NAMES[Math.floor(Math.random() * BOT_NAMES.length)];
            const bUid = `__bot_${safeUser(bName)}`;
            const bet = LADDER_STAKES[Math.floor(Math.random() * LADDER_STAKES.length)];

            ensureUser(bUid, null, true).then(() => {
                const roomId = 'L' + crypto.randomBytes(3).toString('hex').toUpperCase();
                const room = {
                  id: roomId, bet, phase: 'waiting', creatorUid: bUid, joinerUid: null,
                  multiplier: 1, currentTurn: null, lastRoll: null, winner: null, prizePool: 0,
                  players: { [bUid]: { name: bName, pos: 1, virtualSnakes: [] } },
                  pendingJoiners: new Map(),
                  lastActionAt: {}, inactivityWarned: {}, abandonment: null,
                  roundStartedAt: Date.now()
                };

                room._waitTimeout = setTimeout(() => {
                  if (room.phase === 'waiting') {
                    ladderRooms.delete(room.id);
                    broadcastLadderLobby();
                  }
                }, 5 * 60 * 1000);

                ladderRooms.set(roomId, room);
                broadcastLadderLobby();
            }).catch(e => console.error('[LADDER-BOT-CREATE]', e.message));
        }
    } catch (e) { console.error('[LADDER-BOT-ENGINE]', e.message); }
}, 15000);

// ============================================================================
// WEBSOCKET SETUP
// ============================================================================
const conns = new Map();
const wsIpConnections = new Map();
const wsIpAttempts = new Map();
function allowWsConnection(ip) {
  const key = normalizedClientIp(ip || 'unknown');
  const t = now();
  const recent = (wsIpAttempts.get(key) || []).filter(x => x > t - WS_CONNECTION_RATE_WINDOW_MS);
  if (recent.length >= WS_CONNECTION_RATE_MAX) return false;
  const active = Number(wsIpConnections.get(key) || 0);
  if (active >= WS_MAX_CONNECTIONS_PER_IP) return false;
  recent.push(t); wsIpAttempts.set(key, recent); wsIpConnections.set(key, active + 1);
  return true;
}

// ─── FIX S1: prune stale rate-limit buckets every 15 min ──────────────────
setInterval(() => {
  const cutoff = now() - WS_CONNECTION_RATE_WINDOW_MS;
  for (const [k, v] of wsIpAttempts) {
    const trimmed = v.filter(t => t > cutoff);
    if (trimmed.length) wsIpAttempts.set(k, trimmed);
    else wsIpAttempts.delete(k);
  }
}, 15 * 60 * 1000);

const wss = new WebSocketServer({ noServer: true, maxPayload: WS_MAX_PAYLOAD });

// Lightweight production diagnostics for capacity testing. No player data,
// wallet values, tokens, or message contents are logged.
const PERF_LOG_INTERVAL_MS = Math.max(15000, Number(process.env.PERF_LOG_INTERVAL_MS || 60000));
const eventLoopDelay = monitorEventLoopDelay({ resolution: 20 });
eventLoopDelay.enable();
let previousCpuUsage = process.cpuUsage();
setInterval(() => {
  const currentCpu = process.cpuUsage();
  const userMicros = currentCpu.user - previousCpuUsage.user;
  const systemMicros = currentCpu.system - previousCpuUsage.system;
  previousCpuUsage = currentCpu;
  const heap = process.memoryUsage();
  const loopP95Ms = Number((eventLoopDelay.percentile(95) / 1e6).toFixed(1));
  const loopMaxMs = Number((eventLoopDelay.max / 1e6).toFixed(1));
  console.log('[PERF]', JSON.stringify({
    wsClients: wss.clients.size,
    spinRoomClients: [...roomSocketMembers.values()].reduce((n, set) => n + set.size, 0),
    bingoClients: bingoMembers.size,
    ladderMatchClients: [...ladderRoomSockets.values()].reduce((n, set) => n + set.size, 0),
    ladderLobbyClients: ladderLobbySockets.size,
    dbPool: { total: pool.totalCount, idle: pool.idleCount, waiting: pool.waitingCount, max: DB_POOL_MAX },
    memoryMb: { rss: Math.round(heap.rss / 1048576), heapUsed: Math.round(heap.heapUsed / 1048576) },
    cpuMsSinceLastSample: { user: Math.round(userMicros / 1000), system: Math.round(systemMicros / 1000) },
    eventLoopDelayMs: { p95: loopP95Ms, max: loopMaxMs }
  }));
  eventLoopDelay.reset();
}, PERF_LOG_INTERVAL_MS).unref();

function send(ws, obj) { try { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); } catch (e) {} }

wss.on('connection', (ws, req) => {
  ws._ipKey = normalizedClientIp((process.env.TRUST_PROXY_HEADERS === 'true' && req && req.headers && req.headers['x-forwarded-for'] ? String(req.headers['x-forwarded-for']).split(',')[0].trim() : (req && req.socket && req.socket.remoteAddress)) || 'unknown');
  ws.isAlive = true; ws.authenticatedAt = null;
  ws.authTimer = setTimeout(() => { if (!ws.authed) { try { ws.close(4001, 'authentication timeout'); } catch {} } }, 10000);
  ws.uid = null; ws.roomId = null; ws.authed = false; ws.isAdmin = false; ws.lastAdminActivity = 0;
  setLadderLobbySocket(ws, false);
  setLadderRoomSocket(ws, null);

  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', async raw => {
    const size = raw?.length || raw?.byteLength || 0;
    let m;
    try { m = JSON.parse(raw.toString()); } catch (e) { console.warn(`[WS-PARSE] uid=${ws.uid || '?'} size=${size} err=${e.message}`); return; }
    try { await handle(ws, m); }
    catch (e) { console.error('[WS-MSG]', { type: m?.type, err: e.message }); send(ws, { type: 'error', message: 'server-error' }); }
  });

  ws.on('close', async (code, reason) => {
    clearTimeout(ws.authTimer);
    const activeForIp = Math.max(0, Number(wsIpConnections.get(ws._ipKey) || 1) - 1);
    if (activeForIp) wsIpConnections.set(ws._ipKey, activeForIp); else wsIpConnections.delete(ws._ipKey);
    if (ws.uid) console.log(`[WS-CLOSE] uid=${ws.uid} code=${code} reason=${String(reason || '').slice(0, 80)}`);
    bingoMembers.delete(ws);
    setRoomSocketMembership(ws, null);
    setLadderLobbySocket(ws, false);
    if (ws._ladderRoomId) {
      try { await handleLadderLeave(ws, true); }
      catch (e) { console.error('[LADDER-CLOSE]', e.message); }
    }
    if (!ws.uid) return;
    const set = conns.get(ws.uid);
    if (set) { set.delete(ws); if (!set.size) { conns.delete(ws.uid); await pool.query('UPDATE users SET last_seen_at = $1 WHERE username = $2', [now(), ws.uid]).catch(()=>{}); } }
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
    // ─── FIX C2: reject a second auth on the same socket ──────────────────
    if (ws.authed) return send(ws, { type: 'error', message: 'already-authed' });

    const verification = validateInitData(m.initData || '');
    if (!verification.ok) {
      send(ws, { type: 'error', message: 'auth-failed', reason: verification.reason || 'unknown' });
      if (verification.reason === 'expired') { try { ws.close(4001, 'auth-expired'); } catch {} }
      return;
    }
    const telegramUser = verification.user;
    if (!['login', 'signup'].includes(m.authMode)) {
      send(ws, { type: 'error', message: 'auth-failed', reason: 'invalid-auth-mode' });
      try { ws.close(4003, 'invalid-auth-mode'); } catch {}
      return;
    }
    const authIp = websocketClientIp(ws) || 'unknown';
    const authRateKey = `auth:${authIp}`;
    const authNow = now();
    const authRecent = (wsIpAttempts.get(authRateKey) || []).filter(x => x > authNow - 60_000);
    if (authRecent.length >= 10) { send(ws, { type: 'error', message: 'rate-limited' }); try { ws.close(4008, 'auth-rate-limited'); } catch {} return; }
    authRecent.push(authNow); wsIpAttempts.set(authRateKey, authRecent);
    const telegramId = Number(telegramUser.id);
    const canonicalUid = `tg_${telegramId}`;
    // Include the canonical username as a safe recovery path for older rows
    // whose Telegram ID column/mapping was never populated. Do not guess an
    // account from a Telegram display name or Telegram @username.
    const existingAccount = await pool.query(
      `SELECT username FROM users WHERE telegram_id = $1 OR username = $2
       UNION
       SELECT uid AS username FROM telegram_users WHERE telegram_id = $1 AND uid IS NOT NULL
       LIMIT 1`,
      [telegramId, canonicalUid]
    );

    if (m.authMode === 'login' && !existingAccount.rows.length) {
      send(ws, { type: 'login-required-signup', message: 'No account exists for this Telegram user. Please choose Sign Up first.' });
      return;
    }
    if (!existingAccount.rows.length) {
      const contact = await pool.query('SELECT phone_number FROM telegram_contacts WHERE telegram_id = $1', [Number(telegramUser.id)]);
      if (!contact.rows.length || !contact.rows[0].phone_number) {
        send(ws, { type: 'error', message: 'phone-contact-required', reason: 'phone-contact-required' });
        return;
      }
    }

    // ─── ENHANCED: capture IP + fingerprint, pass through to identity flow ─
    const authFp = sanitizeFingerprint(m.deviceFingerprint) || null;
    const resolved = await resolveTelegramUser(
      telegramUser,
      m.authMode === 'signup' ? (m.ref || null) : null,
      { ip: websocketClientIp(ws) || null, fingerprint: authFp }
    );
    if (!resolved?.uid) return send(ws, { type: 'error', message: 'auth-failed', reason: 'identity-failed' });

    const uid = resolved.uid;
    const isBlockedRes = await pool.query('SELECT blocked, block_reason, blocked_at FROM users WHERE username = $1', [uid]);
    if (isBlockedRes.rows[0]?.blocked) {
      send(ws, { type: 'account-blocked', reason: isBlockedRes.rows[0].block_reason, at: Number(isBlockedRes.rows[0].blocked_at) });
      try { ws.close(4003, 'account-blocked'); } catch {}
      return;
    }

    const clientIp = websocketClientIp(ws);
    await ensureUser(uid, m.ref || null, false, {
      telegram_id: telegramUser.id,
      display_name: [telegramUser.first_name, telegramUser.last_name].filter(Boolean).join(' ') || uid,
      last_ip: clientIp,
      device_fingerprint: m.deviceFingerprint || null,
    });
    if (m.authMode === 'signup') {
      await pool.query(`UPDATE users SET phone_number = (SELECT phone_number FROM telegram_contacts WHERE telegram_id = $1), updated_at = $2 WHERE username = $3`, [Number(telegramUser.id), now(), uid]);
    }

    // ─── ENHANCED: cache ip/fp on the ws for transaction-request re-detection ─
    // Signal recording + detection already ran inside ensureUser() (async).
    // No need to call detectMultiAccount() again here.
    ws._ip = clientIp || null;
    ws._fingerprint = authFp;

    clearTimeout(ws.authTimer);
    ws.authenticatedAt = now(); ws.uid = uid; ws.authed = true;
    ws.displayName = resolved.user.display_name;

    let set = conns.get(uid);
    if (!set) { set = new Set(); conns.set(uid, set); }
    set.add(ws);
    await pool.query('UPDATE users SET last_seen_at = $1 WHERE username = $2', [now(), uid]);

    const holdRes = await pool.query('SELECT withdrawal_hold_until FROM users WHERE username = $1', [uid]);
    const sessionToken = issueSessionToken(uid);
    send(ws, { type: 'authed', uid, displayName: ws.displayName, telegramId: telegramUser.id, withdrawalHoldUntil: 0, sessionToken });

    await sendPendingBroadcasts(uid, ws);
    await expireSignupBonus(uid);
    const bonusInfo = await pool.query('SELECT signup_bonus_remaining, signup_bonus_expires_at FROM users WHERE username=$1',[uid]);
    if (Number(bonusInfo.rows[0]?.signup_bonus_remaining||0)>0 && Number(bonusInfo.rows[0]?.signup_bonus_expires_at||0)>now()) send(ws,{type:'signup-bonus',amount:Number(bonusInfo.rows[0].signup_bonus_remaining),expiresAt:Number(bonusInfo.rows[0].signup_bonus_expires_at)});
    send(ws, { type: 'wallet', ...(await walletFor(uid)) });

    const txSnap = await pool.query("SELECT id, type, amount, status, timestamp FROM transactions WHERE wallet_uid = $1 AND status = 'pending' ORDER BY timestamp DESC LIMIT 10", [uid]);
    send(ws, { type: 'pending-transactions', transactions: txSnap.rows.map(t => ({ ...t, timestamp: Number(t.timestamp), amount: Number(t.amount) })) });
    return;
  }

  if (m.type === 'admin-auth') {
    if (!ADMIN_KEY || typeof m.key !== 'string' || !constantTimeEqual(m.key, ADMIN_KEY)) return ws.close(4003, 'admin auth failed');
    ws.authed = true; ws.isAdmin = true; ws.lastAdminActivity = now(); send(ws, { type: 'admin-authed' }); return;
  }

  if (ws.isAdmin) {
    ws.lastAdminActivity = now();
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

  if (m.type === 'bingo-join') {
    bingoMembers.add(ws);
    const requestedStake = m.stake !== undefined ? m.stake : (m.room === 'room_30' ? 30 : m.room === 'room_15' ? 15 : undefined);
    ws._bingoStake = bingoStake(requestedStake);
    if(!bingoRounds.has(ws._bingoStake)) await bingoNewRound(ws._bingoStake);
    return bingoSendState(ws);
  }
  if (m.type === 'bingo-state') {
    // ─── FIX C1: register for live broadcasts ─────────────────────────────
    bingoMembers.add(ws);
    const requestedStake = m.stake !== undefined ? m.stake : (m.room === 'room_30' ? 30 : m.room === 'room_15' ? 15 : undefined);
    if (requestedStake !== undefined) ws._bingoStake=bingoStake(requestedStake);
    if(!bingoRounds.has(bingoStake(ws._bingoStake))) await bingoNewRound(bingoStake(ws._bingoStake));
    return bingoSendState(ws);
  }
  if (m.type === 'bingo-leave') { bingoMembers.delete(ws); ws._bingoStake = undefined; return; }
  if (m.type === 'bingo-select') {
    // ─── FIX C1: register for live broadcasts ─────────────────────────────
    bingoMembers.add(ws);
    if (m.stake !== undefined) ws._bingoStake=bingoStake(m.stake);
    return bingoSelect(ws,m.numbers);
  }
  if (m.type === 'bingo-unselect') { if (m.stake !== undefined) ws._bingoStake=bingoStake(m.stake); return bingoUnselect(ws,m.number); }
  if (m.type === 'bingo-history') {
    if (!checkTransferRate(`bingo-history:${ws.uid}`)) return send(ws, { type: 'error', message: 'rate-limited' });
    const r=await pool.query(`SELECT DISTINCT r.game_id,r.round_number,r.phase,r.prize_pool,r.called_numbers,r.completed_at
      FROM bingo_rounds r JOIN bingo_entries e ON e.round_id=r.id
      WHERE e.username=$1 ORDER BY r.completed_at DESC NULLS LAST, r.round_number DESC LIMIT 20`,[ws.uid]);
    return send(ws,{type:'bingo-history',items:r.rows});
  }

  if (m.type === 'ladder-join-lobby') {
    setLadderLobbySocket(ws, true);
    broadcastLadderLobby();
    return;
  }
  if (m.type === 'ladder-leave-lobby') {
    setLadderLobbySocket(ws, false);
    return;
  }
  if (m.type === 'ladder-rejoin') {
    await handleLadderRejoin(ws, String(m.roomId || ''));
    return;
  }
  if (m.type === 'ladder-create') {
    const bet = Number(m.bet);
    if (!LADDER_STAKES.includes(bet)) return send(ws, { type: 'ladder-error', message: 'Invalid bet' });
    const roomId = 'L' + crypto.randomBytes(3).toString('hex').toUpperCase();
    const room = {
      id: roomId, bet, phase: 'waiting', creatorUid: ws.uid, joinerUid: null,
      multiplier: 1, currentTurn: null, lastRoll: null, winner: null, prizePool: 0,
      players: { [ws.uid]: { name: ws.displayName, pos: 1, virtualSnakes: [] } },
      pendingJoiners: new Map(),
      lastActionAt: {}, inactivityWarned: {}, abandonment: null,
      roundStartedAt: Date.now()
    };
    room._waitTimeout = setTimeout(() => {
      try {
        if (room.phase === 'waiting') {
          ladderRooms.delete(room.id);
          const creatorWs = Array.from(conns.get(room.creatorUid) || []).find(w => w._ladderRoomId === room.id);
          if (creatorWs) { setLadderRoomSocket(creatorWs, null); send(creatorWs, { type: 'ladder-left', reason: 'timeout' }); }
          broadcastLadderLobby();
        }
      } catch (e) { console.error('[LADDER-CREATE-TIMEOUT]', e.message); }
    }, 5 * 60 * 1000);
    ladderRooms.set(roomId, room);
    setLadderRoomSocket(ws, roomId);
    setLadderLobbySocket(ws, false);
    broadcastLadder(room);
    broadcastLadderLobby();

    setTimeout(() => {
      try {
        if (room.phase === 'waiting') {
          const bName = BOT_NAMES[Math.floor(Math.random() * BOT_NAMES.length)];
          const bUid = `__bot_${safeUser(bName)}`;
          ensureUser(bUid, null, true).then(() => {
            room.pendingJoiners.set(bUid, bName);
            broadcastLadder(room);
          }).catch(e => console.error('[LADDER-CREATE-BOT]', e.message));
        }
      } catch (e) { console.error('[LADDER-CREATE-BOT-CB]', e.message); }
    }, 3000 + Math.random() * 3000);
    return;
  }
  if (m.type === 'ladder-request-join') {
    const room = ladderRooms.get(m.roomId);
    if (!room || room.phase !== 'waiting' || room.creatorUid === ws.uid) return send(ws, { type: 'ladder-error', message: 'Room not available' });
    room.pendingJoiners.set(ws.uid, ws.displayName);
    setLadderRoomSocket(ws, room.id);
    setLadderLobbySocket(ws, false);

    if (room.creatorUid.startsWith('__bot_')) {
        setTimeout(async () => {
           try {
             if (room.phase === 'waiting' && room.pendingJoiners.has(ws.uid)) {
                 room.joinerUid = ws.uid;
                 room.players[ws.uid] = { name: ws.displayName, pos: 1, virtualSnakes: [] };
                 room.pendingJoiners.delete(ws.uid);
                 room.phase = 'betting';
                 if (room._waitTimeout) { clearTimeout(room._waitTimeout); room._waitTimeout = null; }
                 broadcastLadder(room);
             }
           } catch (e) { console.error('[LADDER-BOT-APPROVE]', e.message); }
        }, 1500);
    } else {
        broadcastLadder(room);
    }
    return;
  }
  if (m.type === 'ladder-approve') {
    const room = ladderRooms.get(ws._ladderRoomId);
    if (!room || room.creatorUid !== ws.uid || room.phase !== 'waiting') return;
    const joinerUid = m.targetUid;
    if (!room.pendingJoiners.has(joinerUid)) return;
    room.joinerUid = joinerUid;
    room.players[joinerUid] = { name: room.pendingJoiners.get(joinerUid), pos: 1, virtualSnakes: [] };
    room.pendingJoiners.delete(joinerUid);
    room.phase = 'betting';
    if (room._waitTimeout) { clearTimeout(room._waitTimeout); room._waitTimeout = null; }
    broadcastLadder(room);
    return;
  }
  if (m.type === 'ladder-reject') {
    const room = ladderRooms.get(ws._ladderRoomId);
    if (!room || room.creatorUid !== ws.uid) return;
    room.pendingJoiners.delete(m.targetUid);
    const targetWs = Array.from(conns.get(m.targetUid) || []).find(w => w._ladderRoomId === room.id);
    if (targetWs) {
      setLadderRoomSocket(targetWs, null);
      send(targetWs, { type: 'ladder-rejected' });
    }
    broadcastLadder(room);
    return;
  }
  if (m.type === 'ladder-set-mult') {
    const room = ladderRooms.get(ws._ladderRoomId);
    if (!room || room.phase !== 'betting') return;
    const mult = Number(m.mult);
    if (mult < 1 || mult > 5) return;
    room.multiplier = mult;
    broadcastLadder(room);
    return;
  }
  if (m.type === 'ladder-start') {
    const room = ladderRooms.get(ws._ladderRoomId);
    if (!room || room.phase !== 'betting') return;

    const cost = room.bet * room.multiplier;
    const cBal = await walletFor(room.creatorUid);
    const jBal = await walletFor(room.joinerUid);

    if ((!room.creatorUid.startsWith('__bot_') && cBal.play < cost) || (!room.joinerUid.startsWith('__bot_') && jBal.play < cost)) {
      return send(ws, { type: 'ladder-error', message: 'Insufficient play balance.' });
    }

    if (!room.creatorUid.startsWith('__bot_')) {
        const d1 = await debit(room.creatorUid, cost);
        if (!d1.ok) return send(ws, { type: 'ladder-error', message: 'Failed to deduct creator balance.' });
    }
    if (!room.joinerUid.startsWith('__bot_')) {
        const d2 = await debit(room.joinerUid, cost);
        if (!d2.ok) {
            if (!room.creatorUid.startsWith('__bot_')) await refundWager(room.creatorUid, cost);
            return send(ws, { type: 'ladder-error', message: 'Failed to deduct joiner balance.' });
        }
    }

    room.prizePool = cost * 2;
    room.phase = 'playing';
    room.currentTurn = room.creatorUid;
    room.lastRoll = null;
    room.lastMovePath = null;
    room.lastActionAt = { [room.creatorUid]: Date.now(), [room.joinerUid]: Date.now() };
    room.inactivityWarned = {};
    room.abandonment = null;
    room.roundStartedAt = Date.now();
    broadcastLadder(room);

    if (room.currentTurn.startsWith('__bot_')) {
        setTimeout(() => {
          try {
            if (room.phase === 'playing' && room.currentTurn && room.currentTurn.startsWith('__bot_')) {
              executeLadderRoll(room, room.currentTurn).catch(e => console.error('[LADDER-START-BOT]', e.message));
            }
          } catch (e) { console.error('[LADDER-START-BOT-CB]', e.message); }
        }, 1500);
    }
    return;
  }
  if (m.type === 'ladder-kick') {
    const room = ladderRooms.get(ws._ladderRoomId);
    if (!room || room.creatorUid !== ws.uid || room.phase !== 'betting') return;
    const joinerUid = room.joinerUid;
    if (joinerUid) {
      const targetWs = Array.from(conns.get(joinerUid) || []).find(w => w._ladderRoomId === room.id);
      if (targetWs) {
        setLadderRoomSocket(targetWs, null);
        send(targetWs, { type: 'ladder-kicked' });
      }
      delete room.players[joinerUid];
      room.joinerUid = null;
      room.phase = 'waiting';
      room.multiplier = 1;
      broadcastLadder(room);
      broadcastLadderLobby();
    }
    return;
  }
  if (m.type === 'ladder-roll') {
    const room = ladderRooms.get(ws._ladderRoomId);
    if (room && room.phase === 'playing' && room.currentTurn === ws.uid) {
      await executeLadderRoll(room, ws.uid).catch(e => console.error('[LADDER-ROLL]', e.message));
    }
    return;
  }
  if (m.type === 'ladder-skip') {
    const room = ladderRooms.get(ws._ladderRoomId);
    if (room && room.phase === 'playing' && room.currentTurn === ws.uid) {
      if (room._timeout) { clearTimeout(room._timeout); room._timeout = null; }

      const opponent = (ws.uid === room.creatorUid) ? room.joinerUid : room.creatorUid;
      room.currentTurn = opponent;
      room.lastActionAt = room.lastActionAt || {};
      room.lastActionAt[opponent] = Date.now();
      if (room.inactivityWarned) room.inactivityWarned[opponent] = false;
      broadcastLadder(room);

      const oppWs = Array.from(conns.get(opponent) || []).find(w => w._ladderRoomId === room.id);
      if (oppWs) send(oppWs, { type: 'ladder-notification', message: 'Opponent skipped their turn!' });

      if (room.currentTurn.startsWith('__bot_')) {
          setTimeout(() => {
            try {
              if (room.phase === 'playing' && room.currentTurn && room.currentTurn.startsWith('__bot_')) {
                executeLadderRoll(room, room.currentTurn).catch(e => console.error('[LADDER-SKIP-BOT]', e.message));
              }
            } catch (e) { console.error('[LADDER-SKIP-BOT-CB]', e.message); }
          }, 1500);
      }
    }
    return;
  }
  if (m.type === 'ladder-attack') {
    const room = ladderRooms.get(ws._ladderRoomId);
    if (!room || room.phase !== 'playing' || room.currentTurn !== ws.uid) return;

    const p = room.players[ws.uid];
    if (!p.virtualSnakes) p.virtualSnakes = [];
    if (p.virtualSnakes.length >= 2) return send(ws, { type: 'ladder-error', message: 'Max 2 virtual snakes allowed.' });

    const head = Number(m.head);
    if (!Number.isInteger(head) || head < 21 || head > 99) return send(ws, { type: 'ladder-error', message: 'Invalid snake position. Choose 21-99.' });

    if (getCombinedSnakes(room)[head] || LADDER_LADDERS[head] || head === 100) return send(ws, { type: 'ladder-error', message: 'Position occupied by another snake or ladder.' });

    const cost = 10;
    const w = await walletFor(ws.uid);
    if (w.play < cost) return send(ws, { type: 'ladder-error', message: 'Insufficient play balance for attack.' });

    const deduct = await debit(ws.uid, cost);
    if (!deduct.ok) return send(ws, { type: 'ladder-error', message: 'Failed to deduct balance.' });

    p.virtualSnakes.push(head);
    broadcastLadder(room);
    send(ws, { type: 'ladder-notification', message: `Virtual snake placed at ${head}! -10 Birr` });

    return;
  }
  if (m.type === 'ladder-leave') {
    await handleLadderLeave(ws);
    return;
  }

  if (m.type === 'join') {
    const room = rooms[m.room]; if (!room) return send(ws, { type: 'error', message: 'bad-room' });
    setRoomSocketMembership(ws, m.room);
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
    setRoomSocketMembership(ws, null);
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

  // ─── FIX C6: transactional transfer with audit trail ────────────────────
  if (m.type === 'transfer') {
    if (!checkTransferRate(ws.uid)) return send(ws, { type: 'error', message: 'rate-limited' });
    const amount = Number(m.amount);
    if (!Number.isFinite(amount) || amount <= 0 || amount > 1000000) return send(ws, { type: 'error', message: 'bad-amount' });
    const c = await pool.connect();
    try {
      await c.query('BEGIN');
      const u = await c.query('SELECT main_wallet FROM users WHERE username=$1 FOR UPDATE', [ws.uid]);
      if (!u.rows.length || Number(u.rows[0].main_wallet) < amount) { await c.query('ROLLBACK'); return send(ws, { type: 'error', message: 'insufficient-main' }); }
      await c.query('UPDATE users SET main_wallet=main_wallet-$1, play_wallet=play_wallet+$1, wallet_updated_at=$2, updated_at=$2 WHERE username=$3', [amount, now(), ws.uid]);
      await recordWalletCredit(c, ws.uid, amount, 'transfer_main_to_play', { reason: 'ws-transfer' });
      await c.query('COMMIT');
      await sendWallet(ws.uid);
    } catch (e) { await c.query('ROLLBACK').catch(()=>{}); send(ws, { type: 'error', message: 'transfer-failed' }); }
    finally { c.release(); }
    return;
  }

  if (m.type === 'wallet-refresh') { await sendWallet(ws.uid); return; }

  if (m.type === 'transaction-request') {
    const traceId = newTraceId();
    const imgLen = String(m.imageData || '').length;
    console.log(`[WS-TX] trace=${traceId} uid=${ws.uid} type=${m.txType} amount=${m.amount} ref=${String(m.reference||'').slice(0,20)} imgLen=${imgLen}`);
    await trace(traceId, ws.uid, 'ws.arrived', 'ok', { type: m.txType, amount: m.amount, reference: normalizePaymentReference(m.reference), clientRequestId: m.clientRequestId, detail: { imgLen } });
    if (imgLen > MAX_IMAGE_BYTES) {
      await trace(traceId, ws.uid, 'ws.image-guard', 'fail', { type: m.txType, reason: 'image-too-large', detail: { imgLen, max: MAX_IMAGE_BYTES } });
      send(ws, { type: 'error', message: 'image-too-large' });
      send(ws, { type: 'transaction-rejected', reason: 'image-too-large', traceId });
      return;
    }
    let r;
    // ─── ENHANCED: forward cached ip/fp so transactionRequest re-detects ───
    try { r = await transactionRequest(ws.uid, {
      ...m,
      __traceId: traceId,
      __ip: ws._ip || websocketClientIp(ws),
      __fingerprint: ws._fingerprint || null,
    }); }
    catch (e) {
      console.error('[WS-TX] uncaught:', e.message);
      await trace(traceId, ws.uid, 'ws.uncaught', 'fail', { type: m.txType, reason: e.message });
      send(ws, { type: 'error', message: 'server-error' });
      send(ws, { type: 'transaction-rejected', reason: 'server-error', traceId });
      return;
    }
    if (!r.ok) {
      if (r.reason === 'reference-already-used') {
        try { await addNotification(ws.uid, { type: 'notification', message: 'አገልግሎት ላይ የዋለ Reference code ተጠቅመዋል። እባክዎ በድጋሚ ይሞክሩ።' }); }
        catch (e) { console.error('[TX-USED-REF-MSG]', e.message); }
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
  if (m.type === 'notification-delete') { await pool.query("DELETE FROM notifications WHERE id=$1 AND username=$2 AND dismiss_policy='user'",[String(m.id||''),ws.uid]); return; }
}

setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    if (ws.isAdmin && now() - ws.lastAdminActivity > ADMIN_IDLE_MS) {
      try { ws.close(4002, 'admin-idle-timeout'); } catch {}
      continue;
    }
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  }
}, 60000);
// ============================================================================
// EXPRESS API
// ============================================================================
const app = express();

// ─── FIX S3: paths that may be called from any origin (auth already via body)
const CORS_ANY_ORIGIN_PATHS = new Set(['/api/telegram-contact/status']);

app.use((req, res, next) => {
  const origin = String(req.headers.origin || '').trim();
  const isAnyOrigin = CORS_ANY_ORIGIN_PATHS.has(req.path);

  if (isAnyOrigin) {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'Content-Type');
    res.header('Access-Control-Allow-Methods', 'POST, OPTIONS');
  } else if (origin && WS_ALLOWED_ORIGINS.has(origin)) {
    res.header('Access-Control-Allow-Origin', origin);
    res.header('Vary', 'Origin');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, X-Admin-Key, X-Session-Token, X-Accountant-Session, x-sms-secret, x-telegram-bot-api-secret-token');
    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  }

  res.header('Access-Control-Max-Age', '86400');
  res.header('X-Content-Type-Options', 'nosniff');
  res.header('Referrer-Policy', 'strict-origin-when-cross-origin');
  res.header('Permissions-Policy', 'camera=(), microphone=(), geolocation=()');

  if (req.method === 'OPTIONS') {
    if (isAnyOrigin) return res.sendStatus(204);
    return origin && !WS_ALLOWED_ORIGINS.has(origin) ? res.sendStatus(403) : res.sendStatus(204);
  }
  next();
});
app.use(express.json({ limit: '3mb' }));

// ============================================================================
// TELEGRAM BOT WEBHOOK ROUTE
// ============================================================================
async function handleTelegramWebhook(req, res) {
  if (!BOT_TOKEN || !bot) return res.status(503).json({ ok: false, error: 'telegram-bot-not-configured' });
  if (!TELEGRAM_WEBHOOK_SECRET) return res.status(503).json({ ok: false, error: 'telegram-webhook-secret-not-configured' });
  const supplied = req.get('x-telegram-bot-api-secret-token') || '';
  if (!supplied || !constantTimeEqual(supplied, TELEGRAM_WEBHOOK_SECRET)) return res.status(401).json({ ok: false });

  try {
    const update = req.body || {};
    const msg = update.message || update.edited_message;
    const contact = msg?.contact;
    let contactResult = null;

    // Telegram contact sharing must be self-contact: the contact's user_id
    // must equal the authenticated sender. Never trust a phone number alone.
    if (contact) {
      const senderId = Number(msg?.from?.id || 0);
      const contactUserId = Number(contact.user_id || 0);
      const phone = String(contact.phone_number || '').trim();
      const normalizedPhone = normalizeTelegramPhoneNumber(phone);

      if (!Number.isSafeInteger(senderId) || senderId <= 0 || contactUserId !== senderId || !normalizedPhone || phone.length > 64) {
        contactResult = { accepted: false, reason: 'invalid-self-contact' };
      } else {
        const db = await pool.connect();
        try {
          await db.query('BEGIN');
          await db.query(`INSERT INTO telegram_contacts
              (telegram_id, phone_number, phone_normalized, first_name, last_name, updated_at)
            VALUES ($1,$2,$3,$4,$5,$6)
            ON CONFLICT (telegram_id) DO UPDATE SET
              phone_number=EXCLUDED.phone_number,
              phone_normalized=EXCLUDED.phone_normalized,
              first_name=EXCLUDED.first_name,
              last_name=EXCLUDED.last_name,
              updated_at=EXCLUDED.updated_at`,
            [senderId, phone, normalizedPhone, String(contact.first_name || '').slice(0,128), String(contact.last_name || '').slice(0,128), now()]);
          await db.query(`INSERT INTO telegram_contact_attempts (telegram_id, status, updated_at)
            VALUES ($1, 'accepted', $2)
            ON CONFLICT (telegram_id) DO UPDATE SET status='accepted', updated_at=EXCLUDED.updated_at`,
            [senderId, now()]);
          await db.query('UPDATE users SET phone_number=$1, updated_at=$2 WHERE telegram_id=$3', [phone, now(), senderId]);
          await db.query('COMMIT');
          contactResult = { accepted: true };
        } catch (e) {
          try { await db.query('ROLLBACK'); } catch {}
          if (e.code === '23505') {
            await pool.query(`INSERT INTO telegram_contact_attempts (telegram_id, status, updated_at)
              VALUES ($1, 'phone-already-registered', $2)
              ON CONFLICT (telegram_id) DO UPDATE SET status='phone-already-registered', updated_at=EXCLUDED.updated_at`,
              [senderId, now()]);
            contactResult = { accepted: false, reason: 'phone-already-registered' };
          } else {
            throw e;
          }
        } finally {
          db.release();
        }
      }
    }

    // Process all Telegram updates (commands, callbacks, etc.) through the
    // same configured webhook. Contact persistence above completes first.
    bot.processUpdate(update);
    return res.status(200).json({ ok: true, ...(contactResult || {}) });
  } catch (e) {
    console.error('[TELEGRAM-WEBHOOK]', e.message);
    return res.status(500).json({ ok: false, error: 'telegram-webhook-processing-failed' });
  }
}

if (BOT_TOKEN) {
  // Keep the old token-based route as a compatibility alias during deployment.
  app.post(`/bot${BOT_TOKEN}`, handleTelegramWebhook);
}
app.post('/api/telegram/webhook', handleTelegramWebhook);

// ─── FIX S2: rate limit the SMS webhook (only after secret check) ──────────
const smsWebhookRate = new Map();
const SMS_WEBHOOK_RATE_WINDOW_MS = 60_000;
const SMS_WEBHOOK_RATE_MAX = Math.max(5, Number(process.env.SMS_WEBHOOK_RATE_MAX || 30));
function checkSmsWebhookRate(ip) {
  const key = normalizedClientIp(ip || 'unknown');
  const t = now();
  const arr = (smsWebhookRate.get(key) || []).filter(x => x > t - SMS_WEBHOOK_RATE_WINDOW_MS);
  if (arr.length >= SMS_WEBHOOK_RATE_MAX) return false;
  arr.push(t); smsWebhookRate.set(key, arr);
  return true;
}
setInterval(() => {
  const cutoff = now() - SMS_WEBHOOK_RATE_WINDOW_MS;
  for (const [k, v] of smsWebhookRate) {
    const t = v.filter(x => x > cutoff);
    if (t.length) smsWebhookRate.set(k, t); else smsWebhookRate.delete(k);
  }
}, 10 * 60 * 1000);

app.post('/api/sms/incoming', express.json(), async (req, res) => {
  if (!SMS_WEBHOOK_SECRET) {
    console.warn('[SECURITY] SMS webhook called but SMS_WEBHOOK_SECRET is not configured.');
    return res.status(503).json({ ok: false, error: 'sms-webhook-disabled' });
  }
  const providedSecret = req.headers['x-sms-secret'];
  if (!providedSecret || !constantTimeEqual(providedSecret, SMS_WEBHOOK_SECRET)) {
    console.warn('[SECURITY] Unauthorized SMS attempt.');
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }

  const clientIp = (process.env.TRUST_PROXY_HEADERS === 'true'
    ? String(req.headers['x-forwarded-for'] || '').split(',')[0].trim()
    : '') || req.socket.remoteAddress || 'unknown';
  if (!checkSmsWebhookRate(clientIp)) {
    console.warn(`[SMS] Rate-limited from ${clientIp}`);
    return res.status(429).json({ ok: false, error: 'rate-limited' });
  }

  const traceId = newTraceId();
  try {
    const { from, text } = req.body || {};
    if (!text) { await trace(traceId, null, 'sms.validate', 'fail', { reason: 'missing-text' }); return res.status(400).json({ ok: false, error: 'Missing text' }); }
    const id = crypto.randomUUID();
    const timestamp = now();
    const textHash = crypto.createHash('sha256').update(String(from || '') + '|' + String(text)).digest('hex');
    await trace(traceId, null, 'sms.received', 'ok', { reference: '', detail: { smsId: id, sender: from, textLen: String(text).length } });
    try {
      const dupe = await pool.query('SELECT id FROM incoming_sms WHERE text_hash = $1 AND timestamp > $2 LIMIT 1', [textHash, timestamp - DUPLICATE_SMS_WINDOW_MS]);
      if (dupe.rows.length > 0) { await logAutoActivity('skipped', id, '', 0, '', 'duplicate-sms'); await trace(traceId, null, 'sms.dedupe', 'skipped', { detail: { smsId: id, originalId: dupe.rows[0].id } }); return res.json({ ok: true, received: true, duplicate: true, matched: false }); }
    } catch (dedupeErr) { console.warn('[SMS] dedupe skipped:', dedupeErr.message); }
    try { await pool.query('INSERT INTO incoming_sms (id, sender, text, timestamp, processed, text_hash) VALUES ($1,$2,$3,$4,false,$5)', [id, from || 'Unknown', text, timestamp, textHash]); }
    catch (insertErr) { console.warn('[SMS] extended insert failed, falling back:', insertErr.message); await pool.query('INSERT INTO incoming_sms (id, sender, text, timestamp, processed) VALUES ($1,$2,$3,$4,false)', [id, from || 'Unknown', text, timestamp]); }
    await trace(traceId, null, 'sms.persisted', 'ok', { detail: { smsId: id } });
    console.log(`[SMS] Received securely from ${from}: ${String(text).substring(0, 30)}...`);
    const smsRecord = { id, sender: from, text, timestamp };
    const matchResult = await autoApproveMatchingDeposits(id, smsRecord, traceId);
    const definitive = matchResult.ok || ['untrusted-sender', 'no-reference', 'invalid-reference', 'no-amount', 'amount-out-of-range', 'stale-sms', 'reference-reused'].includes(matchResult.reason);
    if (definitive) {
      try { await pool.query('UPDATE incoming_sms SET processed = true, matched_tx_id = COALESCE($1, matched_tx_id) WHERE id = $2', [matchResult.ok ? matchResult.txId : null, id]); }
      catch { try { await pool.query('UPDATE incoming_sms SET processed = true WHERE id = $1', [id]); } catch (_) {} }
    }
    return res.json({ ok: true, received: true, matched: !!matchResult.ok, reason: matchResult.reason || null, traceId });
  } catch (e) {
    console.error('[SMS API ERROR]', e);
    await trace(traceId, null, 'sms.error', 'fail', { reason: e.message });
    return res.status(200).json({ ok: false, error: e.message, traceId });
  }
});
// ─── FIX S3: route-level CORS headers handled by global middleware now ─────
app.post('/api/telegram-contact/status', async (req, res) => {
  const verification = validateInitData(String(req.body?.initData || ''), 86400);
  if (!verification.ok) return res.status(401).json({ ok: false, error: 'telegram-auth-invalid' });
  try {
    const telegramId = Number(verification.user.id);
    const r = await pool.query('SELECT phone_number FROM telegram_contacts WHERE telegram_id=$1', [telegramId]);
    const a = await pool.query('SELECT status FROM telegram_contact_attempts WHERE telegram_id=$1', [telegramId]);
    return res.json({ ok: true, shared: !!r.rows[0]?.phone_number, phoneNumber: r.rows[0]?.phone_number || '', reason: a.rows[0]?.status === 'phone-already-registered' ? 'phone-already-registered' : '' });
  } catch (e) { return res.status(500).json({ ok: false, error: 'contact-check-failed' }); }
});

app.get('/', (_, res) => res.send('Winzo Server is running. Database is PostgreSQL.'));
app.get('/health', (_, res) => res.status(200).send('OK'));
let databaseReady = false;
app.get('/readyz', (_, res) => {
  if (!databaseReady) return res.status(503).json({ ok: false, ready: false });
  res.status(200).json({ ok: true, ready: true });
});

function isAdmin(req) { return ADMIN_KEY && constantTimeEqual(String(req.get('x-admin-key') || ''), ADMIN_KEY); }
function sessionUid(req) { return verifySessionToken(req.get('x-session-token')); }


const REPORTING_SESSION_TTL_MS = 8 * 60 * 60 * 1000;
const REPORTING_LOGIN_WINDOW_MS = 15 * 60 * 1000;
const REPORTING_LOGIN_IP_MAX = 20;
const reportingLoginIpAttempts = new Map();

function reportingIpHash(req) {
  const ip = normalizedClientIp(req.ip || req.socket?.remoteAddress || 'unknown');
  return hashText(`reporting-ip|${SESSION_SECRET}|${ip}`);
}
async function auditReportingAccount(username, event, req, details = {}) {
  try {
    await pool.query(
      `INSERT INTO reporting_account_audit (id, username, event, details, ip_hash, at)
       VALUES ($1,$2,$3,$4::jsonb,$5,$6)`,
      [crypto.randomUUID(), String(username || 'unknown').slice(0, 32), String(event).slice(0, 64), JSON.stringify(details || {}), reportingIpHash(req), now()]
    );
  } catch (e) { console.error('[REPORTING-AUDIT]', e.message); }
}
function checkReportingLoginIp(req) {
  const key = reportingIpHash(req);
  const cutoff = now() - REPORTING_LOGIN_WINDOW_MS;
  const recent = (reportingLoginIpAttempts.get(key) || []).filter(t => t > cutoff);
  if (recent.length >= REPORTING_LOGIN_IP_MAX) return false;
  recent.push(now());
  reportingLoginIpAttempts.set(key, recent);
  if (reportingLoginIpAttempts.size > 5000) {
    for (const [k, values] of reportingLoginIpAttempts) {
      const fresh = values.filter(t => t > cutoff);
      if (fresh.length) reportingLoginIpAttempts.set(k, fresh);
      else reportingLoginIpAttempts.delete(k);
    }
  }
  return true;
}
async function reportingAccountFromRequest(req) {
  const token = String(req.get('x-accountant-session') || '').trim();
  if (!token || token.length > 256) return null;
  const tokenHash = hashText(token);
  const result = await pool.query(
    `SELECT a.id, a.username, a.display_name, a.must_change_password
       FROM reporting_sessions s
       JOIN reporting_accounts a ON a.id = s.account_id
      WHERE s.token_hash = $1 AND s.expires_at > $2 AND a.active = TRUE
      LIMIT 1`,
    [tokenHash, now()]
  );
  if (!result.rows.length) return null;
  await pool.query('UPDATE reporting_sessions SET last_seen_at = $1 WHERE token_hash = $2', [now(), tokenHash]);
  return { ...result.rows[0], tokenHash };
}
async function reportingReadAuth(req, res, next) {
  if (isAdmin(req)) return next();
  try {
    const account = await reportingAccountFromRequest(req);
    if (!account) return res.status(403).json({ ok: false, error: 'reporting-login-required' });
    if (account.must_change_password) return res.status(403).json({ ok: false, error: 'password-change-required' });
    req.reportingAccount = account;
    await auditReportingAccount(account.username, 'report.access', req, { path: req.path });
    return next();
  } catch (e) {
    console.error('[REPORTING-AUTH]', e.message);
    return res.status(503).json({ ok: false, error: 'reporting-auth-unavailable' });
  }
}

// Accountant login. The opaque session token is stored only as a SHA-256 hash.
app.post('/accountant/login', async (req, res) => {
  const username = normalizeReportingUsername(req.body?.username);
  const password = typeof req.body?.password === 'string' ? req.body.password : '';
  if (!checkReportingLoginIp(req)) return res.status(429).json({ ok: false, error: 'too-many-login-attempts' });
  if (!/^[a-z0-9._-]{3,32}$/.test(username) || !password || password.length > 128) {
    return res.status(401).json({ ok: false, error: 'invalid-credentials' });
  }
  try {
    const result = await pool.query(
      `SELECT id, username, display_name, password_salt, password_hash, active,
              must_change_password, failed_attempts, locked_until
         FROM reporting_accounts WHERE username = $1 LIMIT 1`, [username]
    );
    const account = result.rows[0];
    if (!account || !account.active) {
      await auditReportingAccount(username, 'login.failed', req, { reason: 'invalid-credentials' });
      return res.status(401).json({ ok: false, error: 'invalid-credentials' });
    }
    if (Number(account.locked_until || 0) > now()) {
      await auditReportingAccount(username, 'login.blocked', req, { reason: 'temporary-lock' });
      return res.status(429).json({ ok: false, error: 'temporarily-locked' });
    }
    const valid = await verifyReportingPassword(password, account.password_salt, account.password_hash);
    if (!valid) {
      const lockUntil = now() + 15 * 60 * 1000;
      await pool.query(
        `UPDATE reporting_accounts
            SET failed_attempts = failed_attempts + 1,
                locked_until = CASE WHEN failed_attempts + 1 >= 5 THEN $1 ELSE locked_until END,
                updated_at = $2
          WHERE id = $3`, [lockUntil, now(), account.id]
      );
      await auditReportingAccount(username, 'login.failed', req, { reason: 'invalid-credentials' });
      return res.status(401).json({ ok: false, error: 'invalid-credentials' });
    }
    const token = crypto.randomBytes(32).toString('base64url');
    const createdAt = now();
    const expiresAt = createdAt + REPORTING_SESSION_TTL_MS;
    await pool.query(
      `UPDATE reporting_accounts SET failed_attempts = 0, locked_until = 0, last_login_at = $1, updated_at = $1 WHERE id = $2`,
      [createdAt, account.id]
    );
    await pool.query(
      `INSERT INTO reporting_sessions (token_hash, account_id, created_at, expires_at, last_seen_at)
       VALUES ($1,$2,$3,$4,$3)`, [hashText(token), account.id, createdAt, expiresAt]
    );
    await pool.query('DELETE FROM reporting_sessions WHERE expires_at <= $1', [createdAt]);
    await auditReportingAccount(username, 'login.success', req);
    return res.json({
      ok: true, sessionToken: token, expiresAt,
      account: { username: account.username, displayName: account.display_name, mustChangePassword: !!account.must_change_password }
    });
  } catch (e) {
    console.error('[ACCOUNTANT-LOGIN]', e);
    return res.status(500).json({ ok: false, error: 'login-failed' });
  }
});

app.get('/accountant/me', async (req, res) => {
  try {
    const account = await reportingAccountFromRequest(req);
    if (!account) return res.status(401).json({ ok: false, error: 'session-expired' });
    return res.json({ ok: true, account: { username: account.username, displayName: account.display_name, mustChangePassword: !!account.must_change_password } });
  } catch (e) { return res.status(500).json({ ok: false, error: 'account-check-failed' }); }
});

app.post('/accountant/logout', async (req, res) => {
  const token = String(req.get('x-accountant-session') || '').trim();
  if (token) {
    try {
      const tokenHash = hashText(token);
      const account = await pool.query(
        `SELECT a.username FROM reporting_sessions s JOIN reporting_accounts a ON a.id=s.account_id WHERE s.token_hash=$1 LIMIT 1`, [tokenHash]
      );
      await pool.query('DELETE FROM reporting_sessions WHERE token_hash = $1', [tokenHash]);
      if (account.rows[0]) await auditReportingAccount(account.rows[0].username, 'logout', req);
    } catch (e) { console.error('[ACCOUNTANT-LOGOUT]', e.message); }
  }
  return res.json({ ok: true });
});

app.post('/accountant/change-password', async (req, res) => {
  const account = await reportingAccountFromRequest(req).catch(() => null);
  if (!account) return res.status(401).json({ ok: false, error: 'session-expired' });
  const currentPassword = typeof req.body?.currentPassword === 'string' ? req.body.currentPassword : '';
  const newPassword = typeof req.body?.newPassword === 'string' ? req.body.newPassword : '';
  if (!validReportingPassword(newPassword)) return res.status(400).json({ ok: false, error: 'password-must-be-12-to-128-characters' });
  try {
    const current = await pool.query('SELECT password_salt, password_hash FROM reporting_accounts WHERE id=$1 AND active=TRUE', [account.id]);
    if (!current.rows.length || !(await verifyReportingPassword(currentPassword, current.rows[0].password_salt, current.rows[0].password_hash))) {
      return res.status(400).json({ ok: false, error: 'current-password-incorrect' });
    }
    const nextHash = await scryptReportingPassword(newPassword);
    await pool.query(
      `UPDATE reporting_accounts SET password_salt=$1, password_hash=$2, must_change_password=FALSE,
              failed_attempts=0, locked_until=0, updated_at=$3 WHERE id=$4`,
      [nextHash.salt, nextHash.hash, now(), account.id]
    );
    await pool.query('DELETE FROM reporting_sessions WHERE account_id=$1 AND token_hash<>$2', [account.id, account.tokenHash]);
    await auditReportingAccount(account.username, 'password.changed', req);
    return res.json({ ok: true });
  } catch (e) {
    console.error('[ACCOUNTANT-PASSWORD-CHANGE]', e.message);
    return res.status(500).json({ ok: false, error: 'password-change-failed' });
  }
});

// Full administrators alone can create, reset, deactivate/reactivate, or remove
// accountant accounts. These records are separate from player accounts/wallets.
app.get('/admin/reporting-accounts', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  try {
    const r = await pool.query(
      `SELECT id, username, display_name, active, must_change_password, created_at, updated_at, last_login_at
         FROM reporting_accounts ORDER BY created_at DESC, username ASC`
    );
    return res.json({ ok: true, accounts: r.rows.map(a => ({
      id: a.id, username: a.username, displayName: a.display_name, active: !!a.active,
      mustChangePassword: !!a.must_change_password, createdAt: Number(a.created_at),
      updatedAt: Number(a.updated_at), lastLoginAt: a.last_login_at == null ? null : Number(a.last_login_at)
    })) });
  } catch (e) { return res.status(500).json({ ok: false, error: 'account-list-failed' }); }
});

app.post('/admin/reporting-accounts', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  const username = normalizeReportingUsername(req.body?.username);
  const displayName = String(req.body?.displayName || '').trim().slice(0, 100) || username;
  const password = typeof req.body?.password === 'string' ? req.body.password : '';
  if (!/^[a-z0-9._-]{3,32}$/.test(username)) return res.status(400).json({ ok: false, error: 'username-must-be-3-to-32-valid-characters' });
  if (!validReportingPassword(password)) return res.status(400).json({ ok: false, error: 'password-must-be-12-to-128-characters' });
  try {
    const hash = await scryptReportingPassword(password);
    const id = crypto.randomUUID();
    const t = now();
    const r = await pool.query(
      `INSERT INTO reporting_accounts (id, username, display_name, password_salt, password_hash, active, must_change_password, created_at, updated_at)
       VALUES ($1,$2,$3,$4,$5,TRUE,TRUE,$6,$6) ON CONFLICT (username) DO NOTHING RETURNING id, username, display_name, active, created_at`,
      [id, username, displayName, hash.salt, hash.hash, t]
    );
    if (!r.rows.length) return res.status(409).json({ ok: false, error: 'username-already-exists' });
    await auditReportingAccount(username, 'account.created', req, { by: 'admin' });
    return res.status(201).json({ ok: true, account: { id, username, displayName, active: true, createdAt: t } });
  } catch (e) {
    console.error('[REPORTING-ACCOUNT-CREATE]', e.message);
    return res.status(500).json({ ok: false, error: 'account-create-failed' });
  }
});

app.post('/admin/reporting-accounts/:id/reset-password', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  const password = typeof req.body?.password === 'string' ? req.body.password : '';
  if (!validReportingPassword(password)) return res.status(400).json({ ok: false, error: 'password-must-be-12-to-128-characters' });
  const client = await pool.connect();
  try {
    const hash = await scryptReportingPassword(password);
    await client.query('BEGIN');
    const r = await client.query(
      `UPDATE reporting_accounts SET password_salt=$1, password_hash=$2, must_change_password=TRUE,
              failed_attempts=0, locked_until=0, updated_at=$3 WHERE id=$4 RETURNING username`,
      [hash.salt, hash.hash, now(), String(req.params.id)]
    );
    if (!r.rows.length) { await client.query('ROLLBACK'); return res.status(404).json({ ok: false, error: 'account-not-found' }); }
    await client.query('DELETE FROM reporting_sessions WHERE account_id=$1', [String(req.params.id)]);
    await client.query(
      `INSERT INTO reporting_account_audit (id, username, event, details, ip_hash, at) VALUES ($1,$2,'password.reset','{"by":"admin"}'::jsonb,$3,$4)`,
      [crypto.randomUUID(), r.rows[0].username, reportingIpHash(req), now()]
    );
    await client.query('COMMIT');
    return res.json({ ok: true, requirePasswordChange: true, sessionsRevoked: true });
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch {}
    console.error('[REPORTING-PASSWORD-RESET]', e.message);
    return res.status(500).json({ ok: false, error: 'password-reset-failed' });
  } finally { client.release(); }
});

app.post('/admin/reporting-accounts/:id/status', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  const active = req.body?.active === true;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const r = await client.query('UPDATE reporting_accounts SET active=$1, updated_at=$2 WHERE id=$3 RETURNING username', [active, now(), String(req.params.id)]);
    if (!r.rows.length) { await client.query('ROLLBACK'); return res.status(404).json({ ok: false, error: 'account-not-found' }); }
    if (!active) await client.query('DELETE FROM reporting_sessions WHERE account_id=$1', [String(req.params.id)]);
    await client.query(
      `INSERT INTO reporting_account_audit (id, username, event, details, ip_hash, at) VALUES ($1,$2,$3,$4::jsonb,$5,$6)`,
      [crypto.randomUUID(), r.rows[0].username, active ? 'account.activated' : 'account.deactivated', JSON.stringify({ by: 'admin' }), reportingIpHash(req), now()]
    );
    await client.query('COMMIT');
    return res.json({ ok: true, active, sessionsRevoked: !active });
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch {}
    console.error('[REPORTING-ACCOUNT-STATUS]', e.message);
    return res.status(500).json({ ok: false, error: 'account-status-failed' });
  } finally { client.release(); }
});

app.delete('/admin/reporting-accounts/:id', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const r = await client.query('SELECT username FROM reporting_accounts WHERE id=$1 FOR UPDATE', [String(req.params.id)]);
    if (!r.rows.length) { await client.query('ROLLBACK'); return res.status(404).json({ ok: false, error: 'account-not-found' }); }
    await client.query(
      `INSERT INTO reporting_account_audit (id, username, event, details, ip_hash, at) VALUES ($1,$2,'account.deleted','{"by":"admin"}'::jsonb,$3,$4)`,
      [crypto.randomUUID(), r.rows[0].username, reportingIpHash(req), now()]
    );
    await client.query('DELETE FROM reporting_accounts WHERE id=$1', [String(req.params.id)]);
    await client.query('COMMIT');
    return res.json({ ok: true, removed: true });
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch {}
    console.error('[REPORTING-ACCOUNT-DELETE]', e.message);
    return res.status(500).json({ ok: false, error: 'account-delete-failed' });
  } finally { client.release(); }
});

app.get('/user/notifications', async (req, res) => {
  const uid = sessionUid(req);
  if (!uid) return res.status(401).json({ ok: false, error: 'unauthorized' });
  try {
    const r = await pool.query('SELECT id, type, message, status, tx_type, tx_id, timestamp, read, dismiss_policy, broadcast_id FROM notifications WHERE username = $1 ORDER BY timestamp DESC LIMIT 50', [uid]);
    res.json({ ok: true, notifications: r.rows.map(n => ({ id: n.id, type: n.type, message: n.message, status: n.status, txType: n.tx_type, txId: n.tx_id, timestamp: Number(n.timestamp), read: n.read, dismissPolicy:n.dismiss_policy, broadcastId:n.broadcast_id })) });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
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
    const usersRes = await pool.query(`
      SELECT u.*,
        COALESCE(NULLIF(BTRIM(u.phone_number), ''), (
          SELECT NULLIF(BTRIM(t.phone_number), '')
          FROM transactions t
          WHERE (t.username = u.username OR (u.telegram_id IS NOT NULL AND t.telegram_id = u.telegram_id))
            AND NULLIF(BTRIM(t.phone_number), '') IS NOT NULL
          ORDER BY t.timestamp DESC NULLS LAST
          LIMIT 1
        ), '') AS linked_phone_number
      FROM users u
      ORDER BY u.updated_at DESC
    `);
    res.json({ ok: true, users: usersRes.rows.map(u => ({ uid: u.username, name: u.display_name, telegram_id: u.telegram_id, phone_number: u.linked_phone_number || '', main: Number(u.main_wallet), play: Number(u.play_wallet), pending: Number(u.pending_withdrawal), blocked: u.blocked, online: !!conns.get(u.username)?.size, last_seen: Number(u.last_seen_at||0), fraud_status:u.fraud_status, signup_bonus_expires_at:Number(u.signup_bonus_expires_at||0) })) });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.get('/admin/finance-summary', async (req,res)=>{
  if(!isAdmin(req)) return res.status(403).json({ok:false});
  try {
    const from = /^\d{4}-\d{2}-\d{2}$/.test(String(req.query.from||'')) ? String(req.query.from) : null;
    const to = /^\d{4}-\d{2}-\d{2}$/.test(String(req.query.to||'')) ? String(req.query.to) : null;
    const excluded = String(req.query.excludeUsers||'').split(',').map(x=>x.trim()).filter(Boolean).slice(0,500);
    const tx = await pool.query(`
      SELECT to_char(to_timestamp(timestamp/1000.0) AT TIME ZONE 'Africa/Addis_Ababa','YYYY-MM-DD') AS day_key,
        COALESCE(SUM(amount) FILTER(WHERE type='deposit' AND status='approved' AND NOT (COALESCE(username,uid,wallet_uid,'') = ANY($3::text[]))),0) deposits,
        COALESCE(SUM(amount) FILTER(WHERE type='withdraw' AND status='approved'),0) withdrawals
      FROM transactions
      WHERE ( $1::date IS NULL OR (to_timestamp(timestamp/1000.0) AT TIME ZONE 'Africa/Addis_Ababa')::date >= $1::date )
        AND ( $2::date IS NULL OR (to_timestamp(timestamp/1000.0) AT TIME ZONE 'Africa/Addis_Ababa')::date <= $2::date )
      GROUP BY 1 ORDER BY 1 DESC`, [from,to,excluded]);
    const fee = await pool.query(`
      SELECT day_key, SUM(human_prize)*0.20 AS fee FROM (
        SELECT to_char(to_timestamp(ended_at/1000.0) AT TIME ZONE 'Africa/Addis_Ababa','YYYY-MM-DD') AS day_key,
               COALESCE(human_win_amount,0) AS human_prize
          FROM round_snapshots
        UNION ALL
        SELECT to_char(to_timestamp(COALESCE(br.completed_at,br.ends_at)/1000.0) AT TIME ZONE 'Africa/Addis_Ababa','YYYY-MM-DD') AS day_key,
               COALESCE(SUM(res.prize),0) AS human_prize
          FROM bingo_rounds br
          LEFT JOIN bingo_results res ON res.round_id=br.id
         WHERE br.phase='results'
         GROUP BY br.id, br.completed_at, br.ends_at
      ) paid_human_prizes
      WHERE ($1::date IS NULL OR day_key::date >= $1::date)
        AND ($2::date IS NULL OR day_key::date <= $2::date)
      GROUP BY day_key ORDER BY day_key DESC`, [from,to]);
    const totals = await pool.query(`
      SELECT
        COALESCE(SUM(amount) FILTER(WHERE type='deposit' AND status='approved' AND NOT (COALESCE(username,uid,wallet_uid,'') = ANY($3::text[]))),0) deposits,
        COALESCE(SUM(amount) FILTER(WHERE type='withdraw' AND status='approved'),0) withdrawals
      FROM transactions
      WHERE ($1::date IS NULL OR (to_timestamp(timestamp/1000.0) AT TIME ZONE 'Africa/Addis_Ababa')::date >= $1::date)
        AND ($2::date IS NULL OR (to_timestamp(timestamp/1000.0) AT TIME ZONE 'Africa/Addis_Ababa')::date <= $2::date)`, [from,to,excluded]);
    const map=new Map();
    for(const r of tx.rows) map.set(r.day_key,{date:r.day_key,deposits:Number(r.deposits),withdrawals:Number(r.withdrawals),serviceFee:0});
    for(const r of fee.rows){const x=map.get(r.day_key)||{date:r.day_key,deposits:0,withdrawals:0,serviceFee:0};x.serviceFee=Number(r.fee);map.set(r.day_key,x);}
    const daily=[...map.values()].sort((a,b)=>b.date.localeCompare(a.date));
    const serviceFeeTotal=daily.reduce((sum,x)=>sum+Number(x.serviceFee||0),0);
    const todayRes=await pool.query("SELECT to_char(CURRENT_TIMESTAMP AT TIME ZONE 'Africa/Addis_Ababa','YYYY-MM-DD') AS day_key");
    const todayKey=todayRes.rows[0].day_key;
    res.json({ok:true,timezone:'Africa/Addis_Ababa',filters:{from,to,excludedUsers:excluded},
      totals:{deposits:Number(totals.rows[0].deposits),withdrawals:Number(totals.rows[0].withdrawals),serviceFee:serviceFeeTotal},
      today:daily.find(x=>x.date===todayKey)||{date:todayKey,deposits:0,withdrawals:0,serviceFee:0},daily});
  }catch(e){console.error('[FINANCE-SUMMARY]',e);res.status(500).json({ok:false,error:e.message});}
});

app.post('/admin/delete-user', async(req,res)=>{
  if(!isAdmin(req)) return res.status(403).json({ok:false});
  const uid=String(req.body?.userId||'').trim(); if(!uid)return res.status(400).json({ok:false,error:'userId required'});
  const c=await pool.connect(); try { await c.query('BEGIN');
    const exists=await c.query('SELECT username FROM users WHERE username=$1 FOR UPDATE',[uid]); if(!exists.rows.length){await c.query('ROLLBACK');return res.status(404).json({ok:false,error:'user-not-found'});}
    await c.query('DELETE FROM notifications WHERE username=$1',[uid]); await c.query('DELETE FROM transactions WHERE username=$1 OR wallet_uid=$1 OR uid=$1',[uid]);
    await c.query('DELETE FROM telegram_users WHERE uid=$1',[uid]);
    await c.query('DELETE FROM transaction_requests WHERE uid=$1',[uid]); await c.query('DELETE FROM deposit_attempts WHERE username=$1',[uid]); await c.query('DELETE FROM request_trace WHERE uid=$1',[uid]); await c.query('DELETE FROM used_references WHERE username=$1',[uid]); await c.query('DELETE FROM game_winners WHERE username=$1',[uid]);
    await c.query('DELETE FROM bingo_results WHERE username=$1',[uid]); await c.query('DELETE FROM bingo_entries WHERE username=$1',[uid]); await c.query('DELETE FROM bingo_wallet_transactions WHERE username=$1',[uid]);
    await c.query('DELETE FROM ip_history WHERE username=$1',[uid]); await c.query('DELETE FROM device_fp_history WHERE username=$1',[uid]);
    await c.query('DELETE FROM users WHERE username=$1',[uid]); await c.query('COMMIT');
    const sockets=conns.get(uid); if(sockets){for(const ws of sockets){try{ws.close(4003,'account-removed');}catch{}}conns.delete(uid);} res.json({ok:true});
  }catch(e){await c.query('ROLLBACK').catch(()=>{});res.status(500).json({ok:false,error:e.message});}finally{c.release();}
});

app.post('/admin/broadcast', async(req,res)=>{
  if(!isAdmin(req))return res.status(403).json({ok:false});
  const message=String(req.body?.message||'').trim(), target=String(req.body?.target||'all'), dismissPolicy=req.body?.dismissPolicy==='admin'?'admin':'user';
  if(!message)return res.status(400).json({ok:false,error:'message-required'});
  try { const users=target==='all' ? await pool.query('SELECT username FROM users') : await pool.query('SELECT username FROM users WHERE username=$1',[target]);
    if(!users.rows.length)return res.status(404).json({ok:false,error:'user-not-found'});
    const id=crypto.randomUUID(); for(const u of users.rows) await addNotification(u.username,{id:crypto.randomUUID(),type:dismissPolicy==='admin'?'sticky_broadcast':'broadcast',message,status:'info',dismissPolicy,createdBy:'admin',broadcastId:id});
    res.json({ok:true,sent:users.rows.length,broadcastId:id});
  }catch(e){res.status(500).json({ok:false,error:e.message});}
});

app.post('/admin/remove-broadcast', async(req,res)=>{
  if(!isAdmin(req))return res.status(403).json({ok:false});
  const id=String(req.body?.broadcastId||'').trim();if(!id)return res.status(400).json({ok:false,error:'broadcastId-required'});
  try{const r=await pool.query("DELETE FROM notifications WHERE broadcast_id=$1 AND dismiss_policy='admin'",[id]);
    for(const set of conns.values())for(const ws of set)if(ws.readyState===1)send(ws,{type:'sticky-broadcast-remove',broadcastId:id});
    res.json({ok:true,removed:r.rowCount});}catch(e){res.status(500).json({ok:false,error:e.message});}
});
app.get('/admin/security-scans',async(req,res)=>{if(!isAdmin(req))return res.status(403).json({ok:false});try{const [r,u]=await Promise.all([pool.query('SELECT scanned,flagged,findings,created_at FROM security_scan_logs ORDER BY created_at DESC LIMIT 30'),pool.query("SELECT username,display_name,block_signals,updated_at FROM users WHERE fraud_status='flagged' ORDER BY updated_at DESC")]);const flaggedUsers=u.rows.map(x=>{let parsed=x.block_signals||{};if(typeof parsed==='string'){try{parsed=JSON.parse(parsed);}catch{parsed={raw:parsed};}}return {uid:x.username,name:x.display_name,signals:Array.isArray(parsed?.signals)?parsed.signals:[],detectedAt:Number(parsed?.detectedAt||x.updated_at||0)};});res.json({ok:true,scans:r.rows,flaggedUsers});}catch(e){res.status(500).json({ok:false,error:e.message});}});

app.post('/admin/scan-suspicious', async(req,res)=>{
  if(!isAdmin(req))return res.status(403).json({ok:false});
  try { const r=await runSuspiciousAccountScan(); const f=await pool.query("SELECT username,display_name,block_signals,updated_at FROM users WHERE fraud_status='flagged' ORDER BY updated_at DESC"); const flaggedUsers=f.rows.map(x=>{let p=x.block_signals||{};if(typeof p==='string'){try{p=JSON.parse(p);}catch{p={};}}return {uid:x.username,name:x.display_name,signals:Array.isArray(p?.signals)?p.signals:[],detectedAt:Number(p?.detectedAt||x.updated_at||0)};});res.json({ok:true,...r,flaggedUsers}); }catch(e){res.status(500).json({ok:false,error:e.message});}
});

// ═══════════════════════════════════════════════════════════════════════════
// ENHANCED: three new admin endpoints for the security UI
// ═══════════════════════════════════════════════════════════════════════════
app.get('/admin/security-details', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  const uid = String(req.query.uid || '').trim();
  if (!uid) return res.status(400).json({ ok: false, error: 'uid required' });
  try {
    const u = await pool.query('SELECT * FROM users WHERE username = $1', [uid]);
    if (!u.rows.length) return res.status(404).json({ ok: false, error: 'not-found' });
    const user = u.rows[0];
    let parsed = { signals: [] };
    try { parsed = JSON.parse(user.block_signals || '{}'); } catch {}
    const signals = (parsed.signals || []).map(s => ({ ...s }));
    const since = now() - MULTI_ACCOUNT_HISTORY_DAYS * DAY;

    for (const sig of signals) {
      if (sig.kind === 'ip-cluster') {
        const rows = await pool.query(
          `SELECT h2.username, MAX(h2.seen_at) AS shared_at
             FROM ip_history h1
             JOIN ip_history h2 ON h1.ip_hash = h2.ip_hash
            WHERE h1.username = $1 AND h2.username <> $1
              AND h1.seen_at > $2 AND h2.seen_at > $2
            GROUP BY h2.username LIMIT 100`,
          [uid, since]
        );
        sig.relatedAccounts = rows.rows.map(r => ({
          uid: r.username, name: r.username, sharedAt: Number(r.shared_at), relation: 'same-ip',
        }));
      } else if (sig.kind === 'fingerprint-cluster') {
        const rows = await pool.query(
          `SELECT h2.username, MAX(h2.seen_at) AS shared_at
             FROM device_fp_history h1
             JOIN device_fp_history h2 ON h1.fingerprint = h2.fingerprint
            WHERE h1.username = $1 AND h2.username <> $1
              AND h1.seen_at > $2 AND h2.seen_at > $2
            GROUP BY h2.username LIMIT 100`,
          [uid, since]
        );
        sig.relatedAccounts = rows.rows.map(r => ({
          uid: r.username, name: r.username, sharedAt: Number(r.shared_at), relation: 'same-fingerprint',
        }));
      }
    }

    res.json({
      ok: true,
      user: {
        uid: user.username, name: user.display_name,
        fraud_status: user.fraud_status || null,
        blocked: !!user.blocked,
        fraud_flagged_at: Number(user.fraud_flagged_at || 0),
        detectedAt: Number(parsed.detectedAt || 0),
      },
      signals,
    });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.post('/admin/freeze-user', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  const uid = String(req.body?.userId || '').trim();
  if (!uid) return res.status(400).json({ ok: false, error: 'userId required' });
  const freeze = !!req.body?.freeze;
  try {
    await pool.query(
      `UPDATE users SET fraud_status = $1, blocked = $2, blocked_at = $3, block_reason = $4, updated_at = $5 WHERE username = $6`,
      [freeze ? 'frozen' : null, freeze, freeze ? now() : null, freeze ? 'Frozen by admin' : null, now(), uid]
    );
    if (freeze) {
      const sockets = conns.get(uid);
      if (sockets) for (const ws of sockets) try { ws.close(4003, 'account-frozen'); } catch {}
    }
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.post('/admin/clear-signals', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  const uid = String(req.body?.userId || '').trim();
  if (!uid) return res.status(400).json({ ok: false, error: 'userId required' });
  try {
    await pool.query(
      `UPDATE users SET fraud_status = NULL, block_signals = NULL, fraud_flagged_at = 0, updated_at = $1 WHERE username = $2`,
      [now(), uid]
    );
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
// ═══════════════════════════════════════════════════════════════════════════

app.get('/admin/user-details', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  const uid = String(req.query.uid || '').trim();
  if (!uid) return res.status(400).json({ ok: false, error: 'uid required' });
  try {
    const userRes = await pool.query('SELECT * FROM users WHERE username = $1', [uid]);
    if (userRes.rows.length === 0) return res.status(404).json({ ok: false, error: 'User not found' });
    const user = userRes.rows[0];
    const refsRes = await pool.query('SELECT username, display_name FROM users WHERE referred_by = $1', [uid]);
    res.json({ ok: true, user: {
      uid: user.username, name: user.display_name, telegram_id: user.telegram_id,
      main: Number(user.main_wallet), play: Number(user.play_wallet), pending: Number(user.pending_withdrawal),
      referral_earnings: Number(user.referral_earnings), referred_by: user.referred_by,
      blocked: user.blocked, block_reason: user.block_reason, fraud_status: user.fraud_status,
      online: !!conns.get(user.username)?.size, last_seen:Number(user.last_seen_at||0), signup_bonus_expires_at:Number(user.signup_bonus_expires_at||0),
      created_at: Number(user.created_at),
      referrals: refsRes.rows.map(r => ({ uid: r.username, name: r.display_name }))
    }});
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// Referral analytics for the admin dashboard. Read-only: this endpoint never
// changes balances, referral attribution, or transaction records.
app.get('/admin/analytics/referrals', reportingReadAuth, async (req, res) => {
  try {
    const result = await pool.query(`
      WITH referral_groups AS (
        SELECT
          referred_by AS referrer_uid,
          COUNT(*)::int AS referral_count,
          COALESCE(
            json_agg(
              json_build_object(
                'uid', referred.username,
                'name', COALESCE(NULLIF(referred.display_name, ''), referred.username),
                'createdAt', COALESCE(referred.created_at, 0)
              ) ORDER BY referred.created_at DESC NULLS LAST, referred.username ASC
            ),
            '[]'::json
          ) AS referred
        FROM users AS referred
        WHERE referred.referred_by IS NOT NULL
          AND BTRIM(referred.referred_by) <> ''
        GROUP BY referred_by
      )
      SELECT
        referrer.username AS uid,
        COALESCE(NULLIF(referrer.display_name, ''), referrer.username) AS name,
        COALESCE(groups.referral_count, 0)::int AS "referralCount",
        COALESCE(referrer.referral_earnings, 0)::numeric AS "referralEarnings",
        COALESCE(groups.referred, '[]'::json) AS referred
      FROM users AS referrer
      LEFT JOIN referral_groups AS groups
        ON groups.referrer_uid = referrer.username
      WHERE COALESCE(groups.referral_count, 0) > 0
         OR COALESCE(referrer.referral_earnings, 0) > 0
      ORDER BY COALESCE(groups.referral_count, 0) DESC,
               COALESCE(referrer.referral_earnings, 0) DESC,
               referrer.username ASC
    `);

    const referrers = result.rows.map(row => ({
      uid: String(row.uid),
      name: String(row.name || row.uid),
      referralCount: Number(row.referralCount || 0),
      referralEarnings: Number(row.referralEarnings || 0),
      referred: Array.isArray(row.referred) ? row.referred.map(user => ({
        uid: String(user.uid),
        name: String(user.name || user.uid),
        createdAt: Number(user.createdAt || 0)
      })) : []
    }));

    const totals = referrers.reduce((acc, referrer) => {
      acc.referrers += 1;
      acc.referredUsers += referrer.referralCount;
      acc.totalEarnings += referrer.referralEarnings;
      return acc;
    }, { referrers: 0, referredUsers: 0, totalEarnings: 0 });
    totals.avgPerReferrer = totals.referrers
      ? totals.totalEarnings / totals.referrers
      : 0;

    return res.json({ ok: true, totals, referrers });
  } catch (e) {
    console.error('[ADMIN/REFERRALS] Failed to load referral analytics:', e);
    return res.status(500).json({ ok: false, error: 'referral-analytics-failed' });
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
    await pool.query('UPDATE users SET blocked = $1, block_reason = $2, blocked_at = $3, updated_at = $4 WHERE username = $5', [isBlocked, isBlocked ? blockReason : null, blockAt, Date.now(), userId]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.post('/admin/send-message', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  const { userId, message, dismissPolicy } = req.body;
  if (!userId || !message) return res.status(400).json({ ok: false, error: 'userId and message required' });
  try { await addNotification(userId, { type: dismissPolicy === 'admin' ? 'sticky_broadcast' : 'broadcast', message: String(message), status: 'info', dismissPolicy: dismissPolicy === 'admin' ? 'admin' : 'user', createdBy:'admin' }); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.get('/admin/user-transactions', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  const uid = String(req.query.uid || '').trim();
  if (!uid) return res.status(400).json({ ok: false, error: 'uid required' });
  try {
    const r = await pool.query(
      `SELECT id, type, amount, status, timestamp, reference, reason, wallet_applied FROM transactions WHERE username = $1 OR wallet_uid = $1 ORDER BY timestamp DESC LIMIT 100`,
      [uid]
    );
    res.json({ ok: true, transactions: r.rows.map(t => ({ id: t.id, type: t.type, amount: Number(t.amount), status: t.status, timestamp: Number(t.timestamp), reference: t.reference, reason: t.reason, wallet_applied: Number(t.wallet_applied) })) });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.get('/admin/winners', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  try {
    const limit = Math.min(Number(req.query.limit) || 100, 500);
    const username = String(req.query.username || '').trim();
    const roomId = String(req.query.roomId || '').trim();
    const conds = []; const vals = []; let i = 1;
    if (username) { conds.push(`username = $${i++}`); vals.push(username); }
    if (roomId) { conds.push(`room_id = $${i++}`); vals.push(roomId); }
    vals.push(limit);
    const sql = `SELECT id, username, display_name, room_id, round, win_amount, winning_number, prize_pool, created_at FROM game_winners ${conds.length ? 'WHERE ' + conds.join(' AND ') : ''} ORDER BY created_at DESC LIMIT $${i}`;
    const r = await pool.query(sql, vals);
    res.json({ ok: true, count: r.rows.length, winners: r.rows.map(w => ({ id: w.id, username: w.username, displayName: w.display_name, roomId: w.room_id, round: Number(w.round), winAmount: Number(w.win_amount), winningNumber: w.winning_number == null ? null : Number(w.winning_number), prizePool: Number(w.prize_pool || 0), createdAt: Number(w.created_at) })) });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
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
      } catch (e) { errors++; await logAutoActivity('error', row.id, '', 0, '', e.message); }
    }
    res.json({ ok: true, approved, skipped, errors, scanned: smsRes.rows.length });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.get('/admin/sms', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  try {
    const smsRes = await pool.query('SELECT * FROM incoming_sms ORDER BY timestamp DESC LIMIT 50');
    const data = {}; smsRes.rows.forEach(r => { data[r.id] = { sender: r.sender, text: r.text, timestamp: Number(r.timestamp) }; });
    res.json({ ok: true, data });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.get('/admin/used-references', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  try { const r = await pool.query('SELECT * FROM used_references ORDER BY consumed_at DESC LIMIT 100'); res.json({ ok: true, references: r.rows }); }
  catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.get('/admin/activity', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  try { const r = await pool.query('SELECT * FROM auto_activity ORDER BY at DESC LIMIT 100'); res.json({ ok: true, activity: r.rows }); }
  catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.get('/admin/all-transactions', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  try {
    const limit = Math.min(Number(req.query.limit) || 100, 500);
    const r = await pool.query(`SELECT id, username, type, amount, status, timestamp, reference, reason, client_request_id, updated_at FROM transactions ORDER BY timestamp DESC LIMIT $1`, [limit]);
    res.json({ ok: true, transactions: r.rows.map(t => ({ ...t, amount: Number(t.amount), timestamp: Number(t.timestamp), updated_at: Number(t.updated_at || 0) })) });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.get('/admin/trace', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  try {
    const traceId = String(req.query.traceId || '').trim();
    const uid = String(req.query.uid || '').trim();
    const clientRequestId = String(req.query.clientRequestId || '').trim();
    const txId = String(req.query.txId || '').trim();
    const limit = Math.min(Number(req.query.limit) || 200, 1000);
    const conds = []; const vals = []; let i = 1;
    if (traceId) { conds.push(`trace_id = $${i++}`); vals.push(traceId); }
    if (uid) { conds.push(`uid = $${i++}`); vals.push(uid); }
    if (clientRequestId) { conds.push(`client_request_id = $${i++}`); vals.push(clientRequestId); }
    if (txId) { conds.push(`tx_id = $${i++}`); vals.push(txId); }
    if (!conds.length) return res.status(400).json({ ok: false, error: 'must supply traceId, uid, clientRequestId, or txId' });
    vals.push(limit);
    const r = await pool.query(`SELECT * FROM request_trace WHERE ${conds.join(' OR ')} ORDER BY at ASC LIMIT $${i}`, vals);
    res.json({ ok: true, count: r.rows.length, trace: r.rows.map(t => ({ ...t, amount: t.amount == null ? null : Number(t.amount), at: Number(t.at) })) });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ─── FIX C7: Addis timezone (UTC+03:00) for both date parsing and bucketing
function dayRangeToMs(fromDate, toDate) {
  const startMs = Date.parse(fromDate + 'T00:00:00+03:00');
  const endMs = Date.parse(toDate + 'T00:00:00+03:00') + 24 * 60 * 60 * 1000;
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || startMs >= endMs) return null;
  return { startMs, endMs };
}

app.get('/admin/analytics/games', reportingReadAuth, async (req, res) => {
  try {
    const from = String(req.query.from || '').trim(), to = String(req.query.to || '').trim(), room = String(req.query.room || 'both').trim();
    const rng = dayRangeToMs(from, to);
    if (!rng) return res.status(400).json({ ok: false, error: 'invalid-date-range' });
    const roomsWanted = room === 'both' ? ['room_15', 'room_30'] : [room];
    if (!roomsWanted.every(r => r === 'room_15' || r === 'room_30')) return res.status(400).json({ ok: false, error: 'invalid-room' });

    const snapRes = await pool.query(
      `SELECT human_count, human_picks, bot_count, bot_picks, total_picks, prize_pool,
              winner_count, human_winner_count, human_win_amount, human_player_ids, ended_at
         FROM round_snapshots
        WHERE ended_at >= $1 AND ended_at < $2 AND room_id = ANY($3::text[])
        ORDER BY ended_at ASC`,
      [rng.startMs, rng.endMs, roomsWanted]
    );

    const buckets = Array.from({ length: 24 }, (_, h) => ({
      hour: h, rounds: 0, playerSet: new Set(),
      totalHumans: 0, totalHumanPicks: 0, totalBots: 0, totalPicks: 0,
      totalPrizePool: 0, totalWinners: 0, humanWinners: 0, humanWinAmount: 0,
    }));
    const rangePlayerSet = new Set();

    // ─── FIX C7: bucket by Addis local hour (UTC+03:00) ──────────────────
    const ADDIS_OFFSET_MS = 3 * 60 * 60 * 1000;
    for (const row of snapRes.rows) {
      const h = new Date(Number(row.ended_at) + ADDIS_OFFSET_MS).getUTCHours();
      buckets[h].rounds += 1;
      buckets[h].totalHumans      += Number(row.human_count || 0);
      buckets[h].totalHumanPicks  += Number(row.human_picks || 0);
      buckets[h].totalBots        += Number(row.bot_count || 0);
      // Use human_picks rather than legacy total_picks, which may include bot picks.
      buckets[h].totalPicks       += Number(row.human_picks || 0);
      buckets[h].totalPrizePool   += Number(row.prize_pool || 0);
      buckets[h].totalWinners     += Number(row.winner_count || 0);
      buckets[h].humanWinners     += Number(row.human_winner_count || 0);
      buckets[h].humanWinAmount   += Number(row.human_win_amount || 0);

      if (Array.isArray(row.human_player_ids)) {
        for (const uid of row.human_player_ids) {
          const s = String(uid);
          buckets[h].playerSet.add(s);
          rangePlayerSet.add(s);
        }
      }
    }

    const hourly = buckets.map(b => {
      const players = b.playerSet.size;
      const avg     = b.rounds ? players / b.rounds : 0;
      return {
        hour: b.hour,
        rounds: b.rounds,
        uniquePlayers: players,
        average: avg,
        games: b.rounds,
        avgHumans: b.rounds ? b.totalHumans / b.rounds : 0,
        totalHumans: b.totalHumans,
        totalHumanPicks: b.totalHumanPicks,
        totalBots: b.totalBots,
        totalPicks: b.totalPicks,
        avgPrizePool: b.rounds ? b.totalPrizePool / b.rounds : 0,
        totalWinners: b.totalWinners,
        humanWinners: b.humanWinners,
        humanWinAmount: b.humanWinAmount,
      };
    });

    const hoursWithData     = hourly.filter(h => h.rounds > 0).length;
    const totalRounds       = hourly.reduce((s, h) => s + h.rounds, 0);
    const totalPlayerHours  = hourly.reduce((s, h) => s + h.uniquePlayers, 0);
    const peakHourlyAvg     = hourly.reduce((m, h) => Math.max(m, h.average), 0);
    const peakPlayersInHour = hourly.reduce((m, h) => Math.max(m, h.uniquePlayers), 0);
    const overallAvg        = totalRounds ? totalPlayerHours / totalRounds : 0;
    const avgRoundsPerHour  = hoursWithData ? totalRounds / hoursWithData : 0;
    const avgPlayersPerHour = hoursWithData ? totalPlayerHours / hoursWithData : 0;

    const totalHumans     = hourly.reduce((s, h) => s + h.totalHumans, 0);
    const totalHumanPicks = hourly.reduce((s, h) => s + h.totalHumanPicks, 0);
    const totalBots       = hourly.reduce((s, h) => s + h.totalBots, 0);
    const totalWinners    = hourly.reduce((s, h) => s + h.totalWinners, 0);
    const humanWinners    = hourly.reduce((s, h) => s + h.humanWinners, 0);
    const humanWinAmount  = hourly.reduce((s, h) => s + h.humanWinAmount, 0);

    res.json({
      ok: true, from, to, room, timezone: 'Africa/Addis_Ababa',
      summary: {
        overallAvgPlayersPerGame: overallAvg,
        peakHourlyAvg,
        roundsRecorded: totalRounds,
        hoursWithData,
        totalPlayerHours,
        distinctPlayersInRange: rangePlayerSet.size,
        peakPlayersInHour,
        avgRoundsPerHour,
        avgPlayersPerHour,
        dailyAvgHumansPerGame: hoursWithData
          ? hourly.reduce((s, h) => s + h.avgHumans, 0) / hoursWithData : 0,
        gamesRecorded: totalRounds,
        totalHumanParticipations: totalHumans,
        totalHumanPicks,
        totalBots,
        totalWinners,
        humanWinners,
        humanWinAmount,
      },
      hourly,
    });
  } catch (e) { console.error('[ANALYTICS/games]', e); res.status(500).json({ ok: false, error: e.message }); }
});

app.get('/admin/analytics/total-picks', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ ok: false });
  try {
    const from = String(req.query.from || '').trim();
    const to = String(req.query.to || '').trim();
    const room = String(req.query.room || 'both').trim();
    const rng = dayRangeToMs(from, to);
    if (!rng) return res.status(400).json({ ok: false, error: 'invalid-date-range' });
    const roomsWanted = room === 'both' ? ['room_15', 'room_30'] : [room];
    if (!roomsWanted.every(r => r === 'room_15' || r === 'room_30')) {
      return res.status(400).json({ ok: false, error: 'invalid-room' });
    }
    const result = await pool.query(
      `SELECT game, room_id, round_id, stake, total_picks, recorded_at
         FROM total_picks
        WHERE recorded_at >= $1 AND recorded_at < $2
          AND room_id = ANY($3::text[])
          AND game IN ('spin', 'bingo')
        ORDER BY recorded_at DESC, game ASC, room_id ASC
        LIMIT 5000`,
      [rng.startMs, rng.endMs, roomsWanted]
    );
    const summary = result.rows.reduce((acc, row) => {
      const count = Number(row.total_picks || 0);
      acc.totalPicks += count;
      if (row.game === 'spin') acc.spinPicks += count;
      if (row.game === 'bingo') acc.bingoPicks += count;
      acc.records += 1;
      return acc;
    }, { totalPicks: 0, spinPicks: 0, bingoPicks: 0, records: 0 });
    res.json({
      ok: true, from, to, room, timezone: 'Africa/Addis_Ababa', summary,
      records: result.rows.map(row => ({
        game: row.game, roomId: row.room_id, roundId: row.round_id,
        stake: Number(row.stake || 0), totalPicks: Number(row.total_picks || 0),
        recordedAt: Number(row.recorded_at || 0)
      }))
    });
  } catch (e) {
    console.error('[ANALYTICS/total-picks]', e);
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
      pool.query(`SELECT stage, status, COUNT(*)::int AS c FROM request_trace WHERE at > $1 AND status IN ('fail','error') GROUP BY stage, status ORDER BY c DESC LIMIT 20`, [since]),
    ]);
    const stuck = await pool.query(
      `SELECT t.id, t.username, t.amount, t.reference, t.timestamp FROM transactions t LEFT JOIN used_references u ON UPPER(REGEXP_REPLACE(t.reference, '[^A-Za-z0-9]', '', 'g')) = u.reference WHERE t.status = 'pending' AND t.type = 'deposit' AND t.timestamp < $1 AND u.reference IS NULL ORDER BY t.timestamp DESC LIMIT 50`,
      [now() - 30 * 60 * 1000]
    );
    const smsQueue = await pool.query(`SELECT id, sender, LEFT(text, 80) AS preview, timestamp FROM incoming_sms WHERE processed = false ORDER BY timestamp ASC LIMIT 20`);
    const recentFails = await pool.query(`SELECT trace_id, uid, stage, status, reason, at FROM request_trace WHERE at > $1 AND status IN ('fail','error') ORDER BY at DESC LIMIT 30`, [now() - DAY]);
    res.json({ ok: true, summary: { pendingTransactions: pendingTx.rows[0].c, unprocessedSms: unprocessedSms.rows[0].c, usedReferences: approvedRefs.rows[0].c, traced24h: traced24h.rows[0].c, signupBonus: 0 }, failedStagesLastHour: failedStages.rows, stuckDeposits: stuck.rows.map(r => ({ ...r, amount: Number(r.amount), timestamp: Number(r.timestamp) })), smsQueue: smsQueue.rows.map(r => ({ ...r, timestamp: Number(r.timestamp) })), recentFails: recentFails.rows.map(r => ({ ...r, at: Number(r.at) })) });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
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
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
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
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

async function expireSignupBonus(uid){
  const c=await pool.connect();
  try{
    await c.query('BEGIN');
    const r=await c.query('SELECT play_wallet,signup_bonus_remaining,signup_bonus_expires_at FROM users WHERE username=$1 FOR UPDATE',[uid]);
    if(!r.rows.length){await c.query('COMMIT');return false;}
    const u=r.rows[0], remaining=Number(u.signup_bonus_remaining||0), expires=Number(u.signup_bonus_expires_at||0), at=now();
    if(remaining<=0 || !expires || expires>at){await c.query('COMMIT');return false;}
    const deduction=Math.min(Math.max(0,Number(u.play_wallet||0)),remaining);
    await c.query('UPDATE users SET play_wallet=GREATEST(0,play_wallet-$1),signup_bonus_remaining=0,updated_at=$2,wallet_updated_at=$2 WHERE username=$3',[deduction,at,uid]);
    await c.query('COMMIT');
    await addNotification(uid,{type:'notification',message:'Your 15 Birr welcome bonus has expired. Any unused bonus amount has been removed from your play wallet.',status:'info'});
    return true;
  }catch(e){await c.query('ROLLBACK').catch(()=>{});throw e;}finally{c.release();}
}

async function sendPendingBroadcasts(uid, ws){
  const r=await pool.query("SELECT id,type,message,status,timestamp,dismiss_policy,broadcast_id FROM notifications WHERE username=$1 AND type IN ('broadcast','sticky_broadcast') ORDER BY timestamp ASC",[uid]);
  for(const n of r.rows){
    if(ws.readyState!==1)break;
    send(ws,{type:'notification',notificationType:n.type,id:n.id,message:n.message,status:n.status,timestamp:Number(n.timestamp||0),dismissPolicy:n.dismiss_policy||'user',broadcastId:n.broadcast_id||null});
  }
}

async function expireAllSignupBonuses(){
  const r=await pool.query('SELECT username FROM users WHERE signup_bonus_remaining>0 AND signup_bonus_expires_at>0 AND signup_bonus_expires_at<=$1',[now()]);
  for(const row of r.rows){try{await expireSignupBonus(row.username);await sendWallet(row.username);}catch(e){console.error('[BONUS-EXPIRY]',row.username,e.message);}}
}

// ═══════════════════════════════════════════════════════════════════════════
// ENHANCED: runSuspiciousAccountScan queries history tables directly
// ═══════════════════════════════════════════════════════════════════════════
async function runSuspiciousAccountScan(){
  const since = now() - MULTI_ACCOUNT_HISTORY_DAYS * DAY;

  const [ipClusters, fpClusters] = await Promise.all([
    pool.query(
      `SELECT ip_hash, array_agg(DISTINCT username) AS users, COUNT(DISTINCT username)::int AS c
         FROM ip_history
        WHERE seen_at > $1
        GROUP BY ip_hash
        HAVING COUNT(DISTINCT username) >= $2`,
      [since, MULTI_ACCOUNT_IP_THRESHOLD]
    ),
    pool.query(
      `SELECT fingerprint, array_agg(DISTINCT username) AS users, COUNT(DISTINCT username)::int AS c
         FROM device_fp_history
        WHERE seen_at > $1
        GROUP BY fingerprint
        HAVING COUNT(DISTINCT username) >= $2`,
      [since, MULTI_ACCOUNT_FP_THRESHOLD]
    ),
  ]);

  const userSignals = new Map();
  for (const row of ipClusters.rows) {
    for (const u of row.users) {
      if (!userSignals.has(u)) userSignals.set(u, []);
      userSignals.get(u).push({
        kind: 'ip-cluster', count: row.c,
        windowDays: MULTI_ACCOUNT_HISTORY_DAYS, threshold: MULTI_ACCOUNT_IP_THRESHOLD,
        lastSeen: now(),
      });
    }
  }
  for (const row of fpClusters.rows) {
    for (const u of row.users) {
      if (!userSignals.has(u)) userSignals.set(u, []);
      userSignals.get(u).push({
        kind: 'fingerprint-cluster', count: row.c,
        windowDays: MULTI_ACCOUNT_HISTORY_DAYS, threshold: MULTI_ACCOUNT_FP_THRESHOLD,
        lastSeen: now(),
      });
    }
  }

  const findings = [];
  let count = 0;
  const scanned = userSignals.size;

  for (const [uid, signals] of userSignals) {
    const r = await pool.query('SELECT fraud_status, blocked FROM users WHERE username = $1', [uid]);
    if (!r.rows.length) continue;
    const u = r.rows[0];
    if (u.blocked) continue;
    if (String(u.fraud_status || '').toLowerCase() === 'frozen') continue;
    const alreadyFlagged = String(u.fraud_status || '').toLowerCase() === 'flagged';
    if (!alreadyFlagged) count++;

    const scanAt = now();
    await pool.query(
      `UPDATE users
          SET fraud_status = 'flagged',
              block_signals = $1,
              fraud_flagged_at = CASE WHEN fraud_status = 'flagged' THEN fraud_flagged_at ELSE $2 END,
              updated_at = $2
        WHERE username = $3`,
      [JSON.stringify({ signals, detectedAt: scanAt }), scanAt, uid]
    );
    findings.push({ uid, signals });

    if (!alreadyFlagged) {
      try {
        await addNotification(uid, {
          type: 'notification',
          message: 'Your account has been flagged for a routine security review. Please contact support if you believe this is an error.',
          status: 'warning',
        });
      } catch (_) {}
    }
  }

  await pool.query(
    'INSERT INTO security_scan_logs(id,scanned,flagged,findings,created_at) VALUES($1,$2,$3,$4,$5)',
    [crypto.randomUUID(), scanned, count, JSON.stringify(findings), now()]
  );

  console.log(`[SECURITY-SCAN] scanned=${scanned} flagged=${count}`);
  return { scanned, flagged: count, findings };
}

async function pruneOldData() {
  const nowMs = now();
  try {
    const smsCutoff = nowMs - 30 * DAY;
    const attemptsCutoff = nowMs - 7 * DAY;
    const traceCutoff = nowMs - 30 * DAY;
    const activityCutoff = nowMs - 30 * DAY;
    const notifCutoff = nowMs - 90 * DAY;
    const historyCutoff = nowMs - 30 * DAY;
    const [a, b, c, d, e, f, g] = await Promise.all([
      pool.query('DELETE FROM incoming_sms WHERE processed = true AND timestamp < $1', [smsCutoff]),
      pool.query('DELETE FROM deposit_attempts WHERE created_at < $1', [attemptsCutoff]),
      pool.query('DELETE FROM request_trace WHERE at < $1', [traceCutoff]),
      pool.query('DELETE FROM auto_activity WHERE at < $1', [activityCutoff]),
      pool.query(`DELETE FROM notifications WHERE timestamp < $1 AND type IN ('payout_marker','referral_marker')`, [notifCutoff]),
      pool.query('DELETE FROM ip_history WHERE seen_at < $1', [historyCutoff]),
      pool.query('DELETE FROM device_fp_history WHERE seen_at < $1', [historyCutoff]),
    ]);
    console.log(`[PRUNE] sms=${a.rowCount} attempts=${b.rowCount} trace=${c.rowCount} activity=${d.rowCount} notif=${e.rowCount} ip=${f.rowCount} fp=${g.rowCount}`);
  } catch (err) { console.error('[PRUNE] failed:', err.message); }
}

const server = http.createServer(app);
server.on('upgrade', (req, socket, head) => {
  const origin = String(req.headers.origin || '').trim();
  if ((!origin && !WS_ALLOW_NO_ORIGIN) || (origin && !WS_ALLOWED_ORIGINS.has(origin))) {
    socket.write('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n'); socket.destroy(); return;
  }
  const forwarded = process.env.TRUST_PROXY_HEADERS === 'true' ? String(req.headers['x-forwarded-for'] || '').split(',')[0].trim() : '';
  const ip = forwarded || req.socket.remoteAddress || 'unknown';
  if (!allowWsConnection(ip)) {
    socket.write('HTTP/1.1 429 Too Many Requests\r\nConnection: close\r\n\r\n'); socket.destroy(); return;
  }
  wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Server listening on 0.0.0.0:${PORT}`);
  console.log('ℹ️ New-user signup bonus is disabled.');
  
  (async () => {
    try {
      await initDb();
      if (BOT_TOKEN) {
        setupTelegramBot();
        if (!TELEGRAM_WEBHOOK_SECRET || !/^[A-Za-z0-9_-]{1,256}$/.test(TELEGRAM_WEBHOOK_SECRET)) {
          console.error('[TELEGRAM WEBHOOK] Set TELEGRAM_WEBHOOK_SECRET to 1-256 characters using only letters, numbers, underscores or hyphens. Webhook registration skipped.');
        } else {
          const publicUrl = process.env.PUBLIC_URL || `http://localhost:${PORT}`;
          const webhookUrl = `${publicUrl.replace(/\/$/, '')}/api/telegram/webhook`;
          await bot.setWebHook(webhookUrl, {
            secret_token: TELEGRAM_WEBHOOK_SECRET,
            allowed_updates: ['message', 'edited_message', 'callback_query']
          });
          console.log(`✅ Telegram Webhook set to: ${webhookUrl}`);
        }
      }
      await restoreBingoRound();
      await loadRooms();
      startAutomaticDepositMonitor();
      runSuspiciousAccountScan().catch(e=>console.error('[SECURITY-SCAN]',e.message));
      expireAllSignupBonuses().catch(e=>console.error('[BONUS-EXPIRY]',e.message));
      setInterval(()=>expireAllSignupBonuses().catch(e=>console.error('[BONUS-EXPIRY]',e.message)),60000);
      setInterval(()=>runSuspiciousAccountScan().catch(e=>console.error('[SECURITY-SCAN]',e.message)), SCANNER_INTERVAL_MS);
      for (const room of Object.values(rooms)) {
        if (room.phase === 'selection' && room.endsAt <= now()) { room.endsAt = now() + SELECTION_SECONDS * 1000; room.dirty = true; }
        if (room.phase === 'selection') normalizeRoundBots(room);
        room.persist().catch(err => console.error('Room persist failed:', err));
        scheduleBots(room);
      }
      pruneOldData().catch(e => console.error('[PRUNE] initial', e.message));
      setInterval(() => pruneOldData().catch(e => console.error('[PRUNE]', e.message)), 6 * 60 * 60 * 1000);
      databaseReady = true;
      console.log('✅ Background initialization complete.');
    } catch (err) { console.error('⚠️ Background initialization failed (DB may be slow or unreachable):', err); }
  })();
});

// ─── FIX S4: await pool.end() + wss.close() on shutdown ────────────────────
async function gracefulShutdown(signal) {
  console.log(`[SHUTDOWN] ${signal}`);
  databaseReady = false;
  try { eventLoopDelay.disable(); } catch (_) {}
  for (const room of Object.values(rooms)) { clearBotTimers(room); }
  for (const ws of wss.clients) { try { ws.close(1001, 'server shutting down'); } catch {} }
  try { wss.close(); } catch (_) {}
  server.close(async () => {
    try { await pool.end(); } catch (_) {}
    console.log('[SHUTDOWN] Database pool closed. Exiting.');
    process.exit(0);
  });
  setTimeout(() => process.exit(0), 8000).unref();
}
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));
