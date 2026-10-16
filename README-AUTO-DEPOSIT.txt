WINZO AUTO-DEPOSIT UPDATE — CORRECTED PACKAGE

Files:
- admin.html: Preserves the existing Winzo Admin Dashboard, including the SMS Inbox,
  sender filter controls, transaction table, and review actions.
- server.js: Includes the automatic deposit matching monitor for Firebase /incoming_sms.

Important path distinction:
- The preserved admin SMS Inbox currently reads from Firebase /sms, matching the
  original admin dashboard behavior.
- Automatic deposit verification monitors Firebase /incoming_sms, as configured
  for the new server-side verification logic.
These paths are distinct. If your SMS Forwarder writes only to /sms, configure the
server monitor to use that same path before expecting automatic approvals.

Before deployment:
1. Back up your current production files.
2. Confirm the incoming SMS record fields/text match the parser in server.js.
3. Test with a controlled pending deposit before relying on automatic approval.
4. Keep Firebase service-account credentials and ADMIN_KEY secret.

Validation performed: node --check server.js passed. This is a syntax check, not
a live Firebase integration test.
