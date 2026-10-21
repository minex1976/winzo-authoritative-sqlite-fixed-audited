# Winzo anti-fraud patch bundle — extended integrations

Files: server.js, index.html, Admin.html, fraud-audit.js, privacy-migrate-ip.js, firebase-rules-block.json.

## Security compatibility
The existing Telegram initData HMAC validation, 300-second freshness limit, authoritative WebSocket authentication, constant-time ADMIN_KEY comparison, account blocking, wallet transaction reservations, idempotent transaction ledger, deposit/withdrawal approval flow, and server-authoritative game logic are retained. New checks are added as gates around bonus issuance and withdrawals; they do not block login. Do not deploy by replacing unrelated application rules.

## Added protections
- Persistent 10-minute SHA-256 initData replay cache (raw initData is not stored).
- Salted IP and ASN rate limits: 3 bonuses per IPv4 /24 or IPv6 /64 per 24h; 5 per ASN per 24h when IPinfo lookup is configured. Raw IPs and raw User-Agent strings are not persisted in anti-fraud telemetry.
- Optional IPinfo ASN/VPN/proxy/hosting detection via IPINFO_TOKEN. Lookup has a short timeout and fails open as a risk signal only; login remains allowed.
- Optional Telegram account-age provider via TELEGRAM_AGE_LOOKUP_URL (HTTPS only) and optional TELEGRAM_AGE_LOOKUP_KEY. Telegram initData itself does NOT expose account creation time, and there is no official Telegram Bot API field for this. Configure only a trusted provider; returned estimates are not authoritative.
- Every new account has a backend-enforced 24-hour withdrawal hold from local account creation. If a configured age provider verifies the Telegram account is younger than 24 hours, pending-verification is recorded; the 24-hour Winzo-account hold still applies. Deposits and login remain allowed.
- Hashed IP-prefix and User-Agent cluster audit. These are evidence only; shared Wi-Fi and common browsers can create false positives.
- Bonus reversal now requires a provable signup_bonus_remaining ledger of at least 30 Birr and at least 30 play-wallet balance. New accounts have this ledger and game wagers consume the tracked bonus portion first. Legacy accounts without bonus provenance are deliberately NOT debited; their mixed play balance cannot safely prove which money is the original bonus. Reversal record is written before the wallet transaction.

## Environment variables
Required as before: FIREBASE_DATABASE_URL and Firebase service-account credentials, BOT_TOKEN, ADMIN_KEY.
Recommended: IP_HASH_SALT (strong random secret, rotate on a planned schedule), IPINFO_TOKEN. Set TRUST_PROXY_HEADERS=true only when the deployment platform's proxy headers are trusted and sanitized by that platform. Optional: TELEGRAM_AGE_LOOKUP_URL (HTTPS endpoint accepting ?telegram_id= and returning {accountAgeHours:number} or {accountCreatedAt:unix-ms-or-seconds}), TELEGRAM_AGE_LOOKUP_KEY. Do not put any secrets in HTML.

## Important account-age limitation
Telegram does not provide a verified account-created timestamp in Mini App initData. The external age provider is optional and should be treated as an estimate. The reliable safety control included regardless of provider is the 24-hour withdrawal hold from the user's local Winzo account creation time.

## Migration checklist
1. Back up the Realtime Database and test in a staging project first.
2. Set environment variables above; redeploy backend.
3. Merge firebase-rules-block.json into existing rules. Do not replace the complete ruleset. Both /abuse_signals and /abuse_review require .read=false and .write=false for clients; Admin SDK bypasses these rules.
4. Run `privacy-migrate-ip.js` once with the same FIREBASE_DATABASE_URL, service account and IP_HASH_SALT to hash and remove legacy `users/*/last_ip` fields. Back up first.
5. Deploy updated index.html.
6. The supplied Admin.html was originally titled “Winzo Games Room” and appears to be a player page, not the dedicated admin page. Merge its Fraud Review panel into the actual admin page before relying on it.
7. In Fraud Review, click Run audit, refresh flags, expand evidence, then use Confirm fraud only for confirmed cases, False positive for verified legitimate users, or Defer for ambiguous cases. Confirm fraud asks for exact UID and a reason.
8. Review legacy accounts with missing signup_bonus_remaining manually; the endpoint refuses unsafe debits instead of taking a player's own funds.
9. Test: normal login, new bonus grant, same-device second account, rate limits, deposit, withdrawal hold, withdrawal approval after 24 hours, false positive, defer, and reversal failure handling.

## Firebase protection block
Merge the following under the root rules (preserve all existing rules):
{
  "abuse_signals": { ".read": false, ".write": false },
  "abuse_review": { ".read": false, ".write": false }
}
