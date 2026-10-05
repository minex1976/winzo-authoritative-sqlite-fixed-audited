# Winzo Authoritative Server V3.2

The authoritative server uses **Node.js + WebSocket + Firebase Realtime Database**. The previous SQLite documentation was stale and has been removed.

## Architecture

- `server.js` — authoritative game engine, WebSocket protocol, Telegram authentication, Firebase persistence, wallets, transactions, referrals, and bot scheduling.
- `index.html` — player Web App client. It never accesses Firebase directly.
- `admin.html` — admin transaction approval client.
- `render.yaml` — Render Web Service configuration.

## Production / Telegram authentication

Production player authentication requires a valid Telegram Mini App `initData`, verified server-side using `BOT_TOKEN`. The numeric Telegram user ID is bound to the Winzo account; the username is only display information.

Required server environment variables:

```text
NODE_ENV=production
PORT=10000
BOT_TOKEN=your_telegram_bot_token
ADMIN_KEY=your_long_random_admin_key
ALLOWED_ORIGINS=https://minex1976.github.io,https://web.telegram.org
FIREBASE_PROJECT_ID=your_project_id
FIREBASE_CLIENT_EMAIL=your_service_account_email
FIREBASE_PRIVATE_KEY=your_private_key
FIREBASE_DATABASE_URL=https://your-project-default-rtdb.firebaseio.com
```

Never commit real credentials. Rotate any secrets that were previously committed.

## Game behavior

- Two authoritative rooms: `room_15` and `room_30`.
- Maximum 3 picks per player per round.
- The server deducts the room bet for each successful pick and refunds valid unpicks/disconnections according to the round state.
- Bots use isolated internal IDs and are visual participants only: they do not fund the real prize pool, receive real payouts, or determine the winning number.
- The server controls round timing, winner selection, payouts, persistence, and broadcasts.

## Render deployment

Deploy the repository as a Render Web Service using the included `render.yaml`. The service listens on Render's `PORT` and binds to `0.0.0.0`.

The default production URL is expected to be:

`https://winzo-authoritative-server.onrender.com`

The player and admin clients use:

`wss://winzo-authoritative-server.onrender.com`

## Health check

Open `https://YOUR-SERVICE.onrender.com/health`. A successful response contains `ok: true`, `websocket: true`, database information, uptime, and room status.

## Local development

For local development, create an `.env` file from `.env.example` and provide Firebase credentials. To use username-only development authentication, set:

```text
NODE_ENV=development
DEV_ALLOW_ANY=true
ALLOWED_ORIGINS=http://localhost:3000
```

Never enable `DEV_ALLOW_ANY` in production.
