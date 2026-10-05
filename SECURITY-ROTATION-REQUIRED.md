# SECURITY ACTION REQUIRED

The original project contained credentials in `.env.example`. They were removed from this rebuilt package, but removing them from a new ZIP does not invalidate credentials that were already exposed.

Before putting V3.2 into production:

1. Revoke/rotate the Telegram bot token used by the original project.
2. Create a new Firebase service-account key and revoke the old key.
3. Generate a new long random `ADMIN_KEY`.
4. Remove the old secret file/commit from any public Git repository history where possible.
5. Add the new values only to Render Environment Variables.
6. Verify `/health` before opening the player app.

Do not copy the old secrets from the original ZIP into Render.
