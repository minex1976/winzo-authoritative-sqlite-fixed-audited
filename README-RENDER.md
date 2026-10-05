# Winzo Authoritative Server — Render deployment (V3.2)

This project uses **Firebase Realtime Database** as its persistence layer. The old SQLite references have been removed because the running server is Firebase-based.

## Render Web Service

Use the included `render.yaml` as a Blueprint or create a Node Web Service manually.

Required Render secrets:

- `BOT_TOKEN` — Telegram bot token used to validate Mini App `initData`.
- `ADMIN_KEY` — long random secret used by the admin client.
- `FIREBASE_PROJECT_ID`
- `FIREBASE_CLIENT_EMAIL`
- `FIREBASE_PRIVATE_KEY`
- `FIREBASE_DATABASE_URL`

`ALLOWED_ORIGINS` is configured by the Blueprint for the GitHub Pages app and Telegram web client.

The service listens on Render's `PORT` and binds to `0.0.0.0`.

After deployment, the expected URL is:

`https://winzo-authoritative-server.onrender.com`

The player and admin WebSocket clients use:

`wss://winzo-authoritative-server.onrender.com`

## Health check

Open `https://YOUR-RENDER-SERVICE.onrender.com/health`. A successful response contains `ok: true`, `websocket: true`, database status, and room status.

## Telegram authentication

Production player authentication requires a valid Telegram Mini App `initData`. The server validates Telegram's HMAC signature and binds the numeric Telegram user ID to the Winzo account. Username is treated as display information, not as the permanent identity.

For local-only development, `DEV_ALLOW_ANY=true` may be enabled together with `NODE_ENV=development`. Never enable it in production.

## Security

Never commit real Firebase credentials, Telegram bot tokens, or `ADMIN_KEY`. Rotate any credentials that were previously committed to the repository.
