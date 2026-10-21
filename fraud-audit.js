#!/usr/bin/env node
/**
 * Winzo retroactive fraud audit. Read-only with respect to wallets/accounts.
 * Run: FIREBASE_DATABASE_URL=... FIREBASE_SERVICE_ACCOUNT_JSON='...' node fraud-audit.js
 * Findings are written only under abuse_review/.
 */
import admin from 'firebase-admin';
import crypto from 'node:crypto';
const url=process.env.FIREBASE_DATABASE_URL;
if(!url||!process.env.FIREBASE_SERVICE_ACCOUNT_JSON){console.error('Set FIREBASE_DATABASE_URL and FIREBASE_SERVICE_ACCOUNT_JSON');process.exit(1);}
admin.initializeApp({credential:admin.credential.cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT_JSON)),databaseURL:url});
const db=admin.database(), now=Date.now();
const hash=s=>crypto.createHash('sha256').update(String(s)).digest('hex').slice(0,18);
const key=s=>String(s||'unknown').replace(/[.#$\\[\\]\\/]/g,'_').slice(0,180);
const users=(await db.ref('users').once('value')).val()||{};
const whitelistAtStart=(await db.ref('abuse_signals/whitelist').once('value')).val()||{};
const rows=Object.entries(users).filter(([uid,u])=>u&&typeof u==='object'&&!uid.startsWith('__bot_')&&!whitelistAtStart[uid]);
const score=new Map(), reasons=new Map(), updates={};
const add=(uid,weight,code,detail)=>{score.set(uid,(score.get(uid)||0)+weight);if(!reasons.has(uid))reasons.set(uid,[]);reasons.get(uid).push({code,weight,detail});};
const batches={},loops={},phones={},references={},bursts={};
const byId=rows.map(([uid,u])=>({uid,u,id:Number(u.telegram_id),time:Number(u.created_at||u.createdAt||u.firstSeenAt||0)})).filter(x=>x.id>0).sort((a,b)=>a.id-b.id);
for(let i=0;i<byId.length;i++){const g=byId.filter(x=>x.id>=byId[i].id&&x.id-byId[i].id<=500&&Math.abs(x.time-byId[i].time)<=1800000);if(g.length>=3){const id='batch_'+hash(g.map(x=>x.uid).sort().join('|'));batches[id]={uids:[...new Set(g.map(x=>x.uid))],uidsRange:[Math.min(...g.map(x=>x.id)),Math.max(...g.map(x=>x.id))],timeRange:[Math.min(...g.map(x=>x.time)),Math.max(...g.map(x=>x.time))]};g.forEach(x=>add(x.uid,3,'ID-batch',id));}}
const edges={};for(const [uid,u] of rows){const ref=String(u.referred_by||u.referredBy||'');if(ref)edges[uid]=ref;}
const activeUids=new Set(rows.map(([uid])=>uid));
for(const treeName of ['referralData','referralTree']){const tree=(await db.ref(treeName).once('value')).val()||{};for(const [keyName,value] of Object.entries(tree)){if(typeof value==='string'){if(activeUids.has(keyName))edges[keyName]=value;}else if(value&&typeof value==='object'){const child=String(value.referredUid||value.referred_uid||value.uid||keyName);const parent=String(value.referrerUid||value.referrer_uid||value.referrer||value.referredBy||value.referred_by||'');if(activeUids.has(child)&&parent)edges[child]=parent;for(const [childKey,childValue] of Object.entries(value)){if(childValue&&typeof childValue==='object'){const c=String(childValue.uid||childValue.referredUid||childKey);const r=String(childValue.referrerUid||childValue.referrer||childValue.referredBy||keyName);if(activeUids.has(c)&&r)edges[c]=r;}}}}}
for(const start of Object.keys(edges)){let path=[],cur=start;for(let d=0;d<6&&edges[cur];d++){if(path.includes(cur)){const chain=path.slice(path.indexOf(cur)).concat(cur),id='loop_'+hash(chain.join('|'));loops[id]={chain,type:'cycle'};chain.forEach(uid=>add(uid,3,'referral-loop',id));break;}path.push(cur);cur=edges[cur];}}
const ts=rows.map(([uid,u])=>({uid,time:Number(u.created_at||u.createdAt||u.firstSeenAt||0)})).filter(x=>x.time>0).sort((a,b)=>a.time-b.time);
for(let i=0;i<ts.length;i++){const g=[];for(let j=i;j<ts.length&&ts[j].time-ts[i].time<=60000;j++)g.push(ts[j]);if(g.length>=3){const id='burst_'+hash(g.map(x=>x.uid).join('|'));bursts[id]={uids:g.map(x=>x.uid),timeRange:[g[0].time,g[g.length-1].time]};g.forEach(x=>add(x.uid,1,'signup-burst',id));}}
const tx=(await db.ref('transactions').once('value')).val()||{};
for(const t of Object.values(tx)){if(!t||typeof t!=='object')continue;const uid=String(t.wallet_uid||t.uid||'');if(t.type==='withdraw'){const p=String(t.phoneNumber||t.phone_number||t.phone||'').trim();if(p)(phones[p]||=[]).push({uid,amount:Number(t.amount||0)});}if(t.type==='deposit'){const r=String(t.reference||t.referenceCode||t.ref||'').trim();if(r)(references[r]||=[]).push(uid);}}
for(const [p,arr] of Object.entries(phones)){const u=[...new Set(arr.map(x=>x.uid).filter(Boolean))];if(u.length>=2){const id=key(p);updates['shared_phones/'+id]={phone:p,uids:u.map(uid=>({uid,amount:arr.filter(x=>x.uid===uid).reduce((n,x)=>n+x.amount,0)}))};u.forEach(uid=>add(uid,2,'shared-phone',id));}}
for(const [r,arr] of Object.entries(references)){const u=[...new Set(arr.filter(Boolean))];if(u.length>=2){const id=key(r);const combined=[...new Set([...(updates['shared_references/'+id]?.uids||[]),...u])];updates['shared_references/'+id]={reference:r,uids:combined};u.forEach(uid=>add(uid,2,'shared-reference',id));}}
for(const tree of ['used_references','deposit_reference_claims']){const data=(await db.ref(tree).once('value')).val()||{};for(const [r,v] of Object.entries(data)){const u=[...new Set((Array.isArray(v)?v:Object.entries(v||{}).map(([k,x])=>x?.uid||k)).map(x=>typeof x==='string'?x:String(x?.uid||x?.wallet_uid||'')).filter(Boolean))];if(u.length>=2){const id=key(r);updates['shared_references/'+id]={reference:r,uids:u};u.forEach(uid=>add(uid,2,'shared-reference',id));}}}
const telemetry=(await db.ref('abuse_signals/telemetry').once('value')).val()||{};
const ipClusters={},uaClusters={};
for(const [uid,row] of Object.entries(telemetry)){if(row?.ipPrefixHash)(ipClusters[row.ipPrefixHash]||=[]).push(uid);if(row?.userAgentHash)(uaClusters[row.userAgentHash]||=[]).push(uid);}
const ipClusterRecords={},uaClusterRecords={};
for(const [cluster,members] of Object.entries(ipClusters)){const u=[...new Set(members)];if(u.length>=3){ipClusterRecords[cluster]={ipPrefixHash:cluster,uids:u,note:'Hashed IP-prefix cluster; legitimate shared Wi-Fi may also match.'};u.forEach(uid=>add(uid,1,'shared-ip-cluster',cluster));}}
for(const [cluster,members] of Object.entries(uaClusters)){const u=[...new Set(members)];if(u.length>=5)uaClusterRecords[cluster]={userAgentHash:cluster,uids:u,note:'Hashed User-Agent cluster; common browsers may match.'};}
const whitelist=(await db.ref('abuse_signals/whitelist').once('value')).val()||{};
for(const [path,obj] of [['batches',batches],['referral_loops',loops],['bursts',bursts],['ip_clusters',ipClusterRecords],['user_agent_clusters',uaClusterRecords]])for(const [k,v]of Object.entries(obj))updates[path+'/'+k]=v;
const ranked=[...score.entries()].filter(([uid])=>!whitelist[uid]).sort((a,b)=>b[1]-a[1]).slice(0,500);
for(const [uid,value] of ranked){const u=users[uid]||{},ut=Object.values(tx).filter(t=>String(t?.wallet_uid||t?.uid||'')===uid);updates['flagged/'+uid]={uid,score:value,reasons:reasons.get(uid)||[],firstSeenAt:Number(u.created_at||u.createdAt||0),username:u.username||u.telegram_username||uid,totalBonusClaimed:Number(u.signup_bonus_claimed_at?30:0),totalWithdrawn:ut.filter(t=>t.type==='withdraw'&&t.status!=='rejected').reduce((n,t)=>n+Number(t.amount||0),0),referredBy:u.referred_by||null,referredCount:rows.filter(([,x])=>String(x.referred_by||'')===uid).length,wallet:{main:Number(u.main_wallet||0),play:Number(u.play_wallet||0),pending:Number(u.pending_withdrawal||0)},evidence:{telegramId:u.telegram_id,createdAt:u.created_at,ipUaNote:'Historical IP/User-Agent data was not available in this audit script; shared Wi-Fi can also match legitimate families.'}};}
await db.ref('abuse_review').update(updates);
console.log(JSON.stringify({ok:true,batches:Object.keys(batches).length,referral_loops:Object.keys(loops).length,shared_phones:Object.keys(phones).length,shared_references:Object.keys(references).length,bursts:Object.keys(bursts).length,ip_clusters:Object.keys(ipClusterRecords).length,user_agent_clusters:Object.keys(uaClusterRecords).length,flagged:ranked.length},null,2));
await admin.app().delete();
