import fs from 'fs';
import pg from 'pg';
const { Pool } = pg;

// We use the External Access IP and Port you provided, 
// because you are running this script from your personal computer.
const pool = new Pool({
  host: '196.190.212.135', // Your External Access IP
  port: 54390,             // Your External Access Port
  database: 'winzo_sql',
  user: 'admin_winzo_sql',
  password: 'avR9@t+ItYe-Mqa3Ulmb2Wk7U:D:Qsla', 
});

async function migrate() {
  // 1. Check if the export file exists
  if (!fs.existsSync('./firebase-export.json')) {
    console.error('❌ ERROR: firebase-export.json not found!');
    console.error('Please export your Firebase database and place it in this folder.');
    process.exit(1);
  }

  const firebaseData = JSON.parse(fs.readFileSync('./firebase-export.json', 'utf8'));
  const client = await pool.connect();

  try {
    await client.query('BEGIN');
    console.log('Starting Migration from Firebase to AletCloud...');

    // 2. Migrate Users
    if (firebaseData.users) {
      console.log('Migrating Users...');
      for (const [uid, u] of Object.entries(firebaseData.users)) {
        await client.query(`
          INSERT INTO users (
            username, display_name, telegram_id, telegram_username, main_wallet, play_wallet, 
            pending_withdrawal, referral_earnings, created_at, updated_at
          )
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
          ON CONFLICT (username) DO NOTHING
        `, [
          uid, 
          u.display_name || uid, 
          u.telegram_id || null, 
          u.telegram_username || null,
          Number(u.main_wallet || 0), 
          Number(u.play_wallet || 0), 
          Number(u.pending_withdrawal || 0), 
          Number(u.referral_earnings || 0), 
          Number(u.created_at || Date.now()), 
          Number(u.updated_at || Date.now())
        ]);
      }
      console.log(`✅ Migrated ${Object.keys(firebaseData.users).length} users.`);
    }

    // 3. Migrate Transactions
    if (firebaseData.transactions) {
      console.log('Migrating Transactions...');
      for (const [id, t] of Object.entries(firebaseData.transactions)) {
        await client.query(`
          INSERT INTO transactions (
            id, username, wallet_uid, type, amount, status, timestamp, reference, 
            full_name, phone_number, reason
          )
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
          ON CONFLICT (id) DO NOTHING
        `, [
          id, 
          t.username || '', 
          t.wallet_uid || t.uid || t.username || '', 
          t.type || '', 
          Number(t.amount || 0), 
          t.status || 'pending', 
          Number(t.timestamp || Date.now()), 
          t.reference || '', 
          t.full_name || '', 
          t.phone_number || '', 
          t.reason || ''
        ]);
      }
      console.log(`✅ Migrated ${Object.keys(firebaseData.transactions).length} transactions.`);
    }

    // 4. Migrate Rooms
    if (firebaseData.rooms) {
      console.log('Migrating Rooms...');
      for (const [id, r] of Object.entries(firebaseData.rooms)) {
        await client.query(`
          INSERT INTO rooms (
            id, bet, phase, round, ends_at, winning_number, winners_json, 
            prize_pool, last_result_json, taken_json, players_json, updated_at
          )
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
          ON CONFLICT (id) DO NOTHING
        `, [
          id, 
          Number(r.bet || 0), 
          r.phase || 'selection', 
          Number(r.round || 1), 
          Number(r.ends_at || 0), 
          r.winning_number || null, 
          r.winners_json || '[]', 
          Number(r.prize_pool || 0), 
          r.last_result_json || null, 
          r.taken_json || '{}', 
          r.players_json || '{}', 
          Number(r.updated_at || Date.now())
        ]);
      }
      console.log(`✅ Migrated rooms.`);
    }

    await client.query('COMMIT');
    console.log('🎉 Migration Complete! Your data is now in AletCloud PostgreSQL.');
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('❌ Migration failed! Error details:', e);
  } finally {
    client.release();
    pool.end();
  }
}

migrate();
