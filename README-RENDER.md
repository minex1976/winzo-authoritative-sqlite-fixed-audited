# Winzo Authoritative Server — Render deployment

This package keeps the existing authoritative game engine, WebSocket protocol, SQLite persistence, bot engine, wallets, transactions, and GitHub Pages client.

## Render Web Service

Create a **Web Service** from this repository. The included `render.yaml` is a Blueprint configuration for a paid Starter web service with a 1 GB persistent disk for SQLite.

Required secrets:

- `BOT_TOKEN`: Telegram bot token used to validate Telegram Mini App `initData`.
- `ADMIN_KEY`: private key used by the admin client.

The service uses:

- `PORT=10000`
- `NODE_ENV=production`
- `ALLOWED_ORIGIN=https://minex1976.github.io`
- `SQLITE_DB_PATH=/var/data/winzo.sqlite`

After deployment, Render provides a URL such as:

`https://winzo-authoritative-server.onrender.com`

The player client must use the corresponding WebSocket URL:

`wss://winzo-authoritative-server.onrender.com`

Replace `REPLACE_WITH_RENDER_SERVICE` in `index.html` with the actual Render service name, then commit/push the frontend to GitHub Pages.

## Health check

Open:

`https://YOUR-RENDER-SERVICE.onrender.com/health`

A successful response contains `ok: true` and room status.

## Important

Do not use `DEV_ALLOW_ANY=true` or `ALLOWED_ORIGIN=*` in production. Telegram authentication remains authoritative in production.
