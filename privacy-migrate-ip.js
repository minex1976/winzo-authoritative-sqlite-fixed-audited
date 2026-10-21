#!/usr/bin/env node
/** One-time privacy migration: replace legacy raw users/<uid>/last_ip with a salted hash. */
import admin from 'firebase-admin';
import crypto from 'node:crypto';
const url=process.env.FIREBASE_DATABASE_URL;
const salt=String(process.env.IP_HASH_SALT||'').trim();
if(!url||!process.env.FIREBASE_SERVICE_ACCOUNT_JSON||!salt){console.error('Set FIREBASE_DATABASE_URL, FIREBASE_SERVICE_ACCOUNT_JSON, and a strong IP_HASH_SALT before running.');process.exit(1);}
admin.initializeApp({credential:admin.credential.cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT_JSON)),databaseURL:url});
const db=admin.database();
const hashIp=ip=>crypto.createHash('sha256').update(`${salt}|${new Date().toISOString().slice(0,10)}|${String(ip||'').trim()}`).digest('hex');
const snap=await db.ref('users').once('value');
const users=snap.val()||{};const updates={};let migrated=0;
for(const [uid,u] of Object.entries(users)){
 if(!u||typeof u!=='object'||!u.last_ip)continue;
 updates[`users/${uid}/last_ip_hash`]=hashIp(u.last_ip);
 updates[`users/${uid}/last_ip`]=null;
 migrated++;
}
if(Object.keys(updates).length) await db.ref().update(updates);
console.log(JSON.stringify({ok:true,migratedRawIpFields:migrated,rawIpFieldsRemoved:true},null,2));
await admin.app().delete();
