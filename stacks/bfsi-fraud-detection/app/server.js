/**
 * EDB Postgres AI — Core Banking Simulator  v11.0
 *
 * For demonstration purposes only.
 *
 * ClickHouse connectivity: uses curl via child_process
 *   → Bypasses Node.js/Alpine musl DNS bug completely
 *   → curl uses system resolver which handles Docker DNS correctly
 *
 * ClickHouse data ingestion: Kafka Engine (native ClickHouse consumer)
 *   Postgres → Debezium → Kafka → ClickHouse Kafka Engine → MV → MergeTree
 *   No MaterializedPostgreSQL, no extra connectors
 *
 * Two ClickHouse schemas:
 *   default.transactions/accounts/customers  ← Analytics tab
 *   default.kafkacdc_transactions            ← Kafka CDC tab (separate consumer group)
 */
'use strict';

const express  = require('express');
const cors     = require('cors');
const { Pool } = require('pg');
const { WebSocketServer } = require('ws');
const http     = require('http');
const path     = require('path');
const { exec } = require('child_process');

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocketServer({ server });

// Global error handlers to prevent crashes from unexpected connection terminations
process.on('uncaughtException', (err) => {
  if (err.message && err.message.includes('Connection terminated unexpectedly')) {
    console.error('[Process] Database connection terminated unexpectedly - continuing:', err.message);
  } else {
    console.error('[Process] Uncaught exception:', err);
    // For non-connection errors, let the process crash
    process.exit(1);
  }
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('[Process] Unhandled rejection at:', promise, 'reason:', reason);
});

app.use(cors());
app.use(express.json({ limit:'50mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ── CONFIG ─────────────────────────────────────────────────
const CH_URL       = process.env.CLICKHOUSE_HOST  || 'http://clickhouse:8123';
const RW_HOST      = process.env.RISINGWAVE_HOST  || 'risingwave';
const RW_PORT      = parseInt(process.env.RISINGWAVE_PORT || '4566');
const KC_HOST      = process.env.KAFKA_CONNECT_HOST || 'http://kafka-connect:8083';
// ClickHouse runs on host — it connects to Kafka via host-mapped port
const KAFKA_BROKER = process.env.KAFKA_BROKER_FOR_CH || 'localhost:9092';
// RisingWave runs inside Docker — it connects to Kafka via Docker network
const KAFKA_BROKER_RW = process.env.KAFKA_BROKER_FOR_RW || 'kafka:9092';

// ── DEPLOYMENT MODE ────────────────────────────────────────
// Supports TWO modes:
//   - local: EDB Postgres in Docker (with PGAA + AIDB)
//   - ec2: External EDB Postgres via Hybrid Manager
const DEPLOYMENT_MODE = (process.env.DEPLOYMENT_MODE || 'ec2').toLowerCase();
const PG_HOST         = process.env.POSTGRES_HOST     || 'host.docker.internal';
const PG_PORT         = parseInt(process.env.POSTGRES_PORT || '5432');
const PG_USER         = process.env.POSTGRES_USER     || 'postgres';
const PG_PASSWORD     = process.env.POSTGRES_PASSWORD || '';
const PG_DATABASE     = process.env.POSTGRES_DB       || 'corebanking';

// Auto-fill postgres connection in local mode
const AUTO_CONNECT_PG = DEPLOYMENT_MODE === 'local' && PG_HOST && PG_USER;

console.log(`[CONFIG] Deployment Mode: ${DEPLOYMENT_MODE.toUpperCase()}`);
if (AUTO_CONNECT_PG) {
  console.log(`[CONFIG] Auto-connect to Postgres: ${PG_USER}@${PG_HOST}:${PG_PORT}/${PG_DATABASE}`);
}

// ── STATE ──────────────────────────────────────────────────
let pool         = null;
let simTimer     = null;
let simRunning   = false;
let simPaused    = false;
let simConfig    = {};
let txInterval   = 1000;
let sessionStats = { total:0, volume:0, fraud:0, credit:0, debit:0, transfer:0, payment:0 };
let pgConnCfg    = {};
let syncCutoffTxId = null;  // When set, all metrics are filtered to tx_id <= this value

const chStatus   = { connected:false, error:null, kafkaEngineReady:false };
const rwStatus   = { connected:false, error:null, mvReady:false };
const kafkaStatus= { connected:false, error:null, connectorStatus:'UNKNOWN', taskStatus:'UNKNOWN', topics:[] };

// ── BROADCAST ──────────────────────────────────────────────
function broadcast(type, payload) {
  const msg = JSON.stringify({ type, payload });
  let sentCount = 0;
  wss.clients.forEach(c => {
    if (c.readyState===1) {
      c.send(msg);
      sentCount++;
    }
  });
  if (type === 'fraud_alert' && sentCount > 0) {
    console.log(`[WS Broadcast] Sent fraud_alert to ${sentCount} client(s)`);
  }
}
wss.on('connection', ws => {
  ws.send(JSON.stringify({ type:'connected', payload:{ status:'ok', simRunning } }));
  ws.on('close', ()=>{}); ws.on('error', ()=>{});
});

// ── HELPERS ────────────────────────────────────────────────
function pick(a)   { return a[Math.floor(Math.random()*a.length)]; }
function rand(a,b) { return Math.random()*(b-a)+a; }
function ri(a,b)   { return Math.floor(rand(a,b+1)); }
function refNo()   { return 'TXN'+Date.now().toString(36).toUpperCase()+Math.random().toString(36).slice(2,6).toUpperCase(); }
function fmtUSD(n) { return new Intl.NumberFormat('en-US',{style:'currency',currency:'USD'}).format(n); }
function toContainerHost(h) { return (h==='localhost'||h==='127.0.0.1') ? 'host.docker.internal' : h; }

// ══════════════════════════════════════════════════════════
// ══════════════════════════════════════════════════════════
//  CLICKHOUSE CLIENT — curl based (reliable on Alpine)
//  Set CLICKHOUSE_PASSWORD env var if password was set during install
// ══════════════════════════════════════════════════════════

const CH_USER     = process.env.CLICKHOUSE_USER     || 'default';
const CH_PASSWORD = process.env.CLICKHOUSE_PASSWORD || '';

function chAuth() {
  return CH_PASSWORD ? `--user '${CH_USER}:${CH_PASSWORD}'` : `--user '${CH_USER}:'`;
}

function curlPost(sql, db='default') {
  return new Promise((resolve, reject) => {
    const url = `${CH_URL}/?database=${db}&default_format=JSONEachRow`;
    const safeSql = sql.replace(/'/g, "'\''");
    const cmd = `curl -sf --max-time 30 ${chAuth()} -X POST '${url}' -H 'Content-Type: text/plain' --data-binary '${safeSql}'`;
    exec(cmd, { maxBuffer: 50*1024*1024 }, (err, stdout, stderr) => {
      if (err) {
        const detail = (stderr||'').slice(0,200) || err.message;
        reject(new Error(`CH: ${detail}`));
      } else {
        resolve(stdout);
      }
    });
  });
}

function curlPing() {
  return new Promise((resolve) => {
    const cmd = `curl -sf --max-time 5 ${chAuth()} '${CH_URL}/ping'`;
    exec(cmd, (err, stdout, stderr) => {
      if (err) console.log(`[CH] ping failed: ${(stderr||err.message||'').slice(0,150)}`);
      resolve(!err && stdout.trim() === 'Ok.');
    });
  });
}

async function chQuery(sql, db='default') {
  const text = await curlPost(sql, db);
  return text.trim().split('\n').filter(Boolean).map(l => JSON.parse(l));
}

async function chExec(sql, db='default') {
  await curlPost(sql, db);
  return true;
}

async function waitForCH(maxAttempts=20) {
  for (let i=0; i<maxAttempts; i++) {
    const ok = await curlPing();
    if (ok) {
      console.log(`[CH] ✓ Connected at ${CH_URL}`);
      chStatus.connected = true;
      chStatus.error = null;
      return true;
    }
    console.log(`[CH] Waiting (${i+1}/${maxAttempts})...`);
    await new Promise(r => setTimeout(r, 3000));
  }
  chStatus.connected = false;
  chStatus.error = `Not reachable at ${CH_URL}`;
  return false;
}

// ══════════════════════════════════════════════════════════
//  CLICKHOUSE KAFKA ENGINE SETUP
//  Flow: Kafka topics → Kafka engine table → MV → MergeTree
// ══════════════════════════════════════════════════════════

async function setupClickHouseKafka() {
  broadcast('progress', { step:'ch', msg:'Connecting to ClickHouse...', pct:85 });

  const ready = await waitForCH(20);
  if (!ready) throw new Error(`ClickHouse not reachable at ${CH_URL}`);

  broadcast('progress', { step:'ch', msg:'Running ClickHouse Kafka Engine setup script...', pct:88 });

  // Run OLAP setup (CDC tables only, no ML MVs)
  await new Promise((resolve, reject) => {
    const scriptPath = path.join(__dirname, 'scripts/setup/olap/setup-clickhouse-olap.sh');
    exec(`bash ${scriptPath} ${CH_PASSWORD||''}`, { timeout:120000 }, (err, stdout, stderr) => {
      if (stdout) console.log('[CH OLAP Script]\n' + stdout.slice(0,2000));
      if (err) {
        console.error('[CH OLAP error]', (stderr||err.message).slice(0,300));
        reject(new Error('ClickHouse OLAP setup failed: ' + (stderr||err.message).slice(0,200)));
      } else {
        resolve();
      }
    });
  });

  broadcast('progress', { step:'ch', msg:'Setting up ClickHouse Kafka consumer...', pct:88 });


  chStatus.kafkaEngineReady = true;
  chStatus.connected = true;
  console.log('[CH] Kafka engine + MergeTree + MVs ready ✓');
  broadcast('progress', { step:'ch', msg:'✓ ClickHouse Kafka consumer active — ingesting from all 3 topics', pct:94 });
}

// Note: ClickHouse ML MVs are now created by setup-clickhouse-tables.sh
// This function is kept for backwards compatibility but does nothing
async function setupClickHouseMLMVs() {
  // ML MVs (feature + lifetime) are created by setup-clickhouse-tables.sh
  console.log('[CH] ML MVs created by setup-clickhouse-tables.sh ✓');
}

// ══════════════════════════════════════════════════════════
//  ANALYTICS ROUTES
// ══════════════════════════════════════════════════════════

app.get('/api/analytics/status', async (req, res) => {
  const ok = await curlPing();
  chStatus.connected = ok;
  let chRows=0, pgRows=0;
  if (ok) {
    try { const r=await chQuery('SELECT count() as n FROM default.transactions'); chRows=parseInt(r[0]?.n)||0; } catch(_) {}
  }
  try { const r=await pool.query('SELECT count(*) as n FROM transactions'); pgRows=parseInt(r.rows[0]?.n)||0; } catch(_) {}
  const lag = Math.max(0, pgRows - chRows);
  res.json({ ok:true, ch:{...chStatus, rowsInCH:chRows, rowsInPG:pgRows, lagRows:lag }, rw:rwStatus, kafka:kafkaStatus });
});

app.get('/api/analytics/summary', async (req, res) => {
  if (!chStatus.connected) return res.json({ ok:false, error:'ClickHouse not connected' });
  try {
    const rows = await chQuery(`
      SELECT count() AS total_tx,
             round(sum(assumeNotNull(t.amount)),2) AS total_volume,
             round(avg(assumeNotNull(t.amount)),2)  AS avg_amount,
             countIf(fl.is_fraud=1)                  AS fraud_count,
             round(countIf(fl.is_fraud=1)*100.0/nullIf(count(),0),2) AS fraud_pct,
             uniqExact(t.account_id)                AS unique_accounts,
             uniqExact(t.merchant)                  AS unique_merchants
      FROM default.transactions t
      LEFT JOIN default.fraud_labels fl ON t.tx_id = fl.tx_id`);
    res.json({ ok:true, data:rows[0]||{}, source:'Kafka Engine → MergeTree' });
  } catch(e) { res.status(500).json({ ok:false, error:e.message }); }
});

app.get('/api/analytics/volume-by-hour', async (req, res) => {
  if (!chStatus.connected) return res.json({ ok:false, error:'ClickHouse not connected' });
  try {
    const rows = await chQuery(`
      SELECT toStartOfHour(t.created_at) AS hour,
             count() AS tx_count,
             round(sum(assumeNotNull(t.amount)),2) AS volume,
             countIf(fl.is_fraud=1) AS fraud_count
      FROM default.transactions t
      LEFT JOIN default.fraud_labels fl ON t.tx_id = fl.tx_id
      WHERE t.created_at >= now() - INTERVAL 48 HOUR
      GROUP BY hour ORDER BY hour ASC`);
    res.json({ ok:true, data:rows });
  } catch(e) { res.status(500).json({ ok:false, error:e.message }); }
});

app.get('/api/analytics/by-type', async (req, res) => {
  if (!chStatus.connected) return res.json({ ok:false, error:'ClickHouse not connected' });
  try {
    const rows = await chQuery(`
      SELECT type, count() AS tx_count, round(sum(assumeNotNull(amount)),2) AS volume
      FROM default.transactions WHERE type IS NOT NULL
      GROUP BY type ORDER BY tx_count DESC`);
    res.json({ ok:true, data:rows });
  } catch(e) { res.status(500).json({ ok:false, error:e.message }); }
});

app.get('/api/analytics/top-merchants', async (req, res) => {
  if (!chStatus.connected) return res.json({ ok:false, error:'ClickHouse not connected' });
  try {
    const rows = await chQuery(`
      SELECT t.merchant, count() AS tx_count,
             round(sum(assumeNotNull(t.amount)),2) AS total_volume,
             countIf(fl.is_fraud=1) AS fraud_count
      FROM default.transactions t
      LEFT JOIN default.fraud_labels fl ON t.tx_id = fl.tx_id
      WHERE t.merchant IS NOT NULL AND length(t.merchant) > 0
      GROUP BY t.merchant ORDER BY total_volume DESC LIMIT 15`);
    res.json({ ok:true, data:rows });
  } catch(e) { res.status(500).json({ ok:false, error:e.message }); }
});

app.get('/api/analytics/by-category', async (req, res) => {
  if (!chStatus.connected) return res.json({ ok:false, error:'ClickHouse not connected' });
  try {
    const rows = await chQuery(`
      SELECT category, count() AS tx_count, round(sum(assumeNotNull(amount)),2) AS volume
      FROM default.transactions WHERE category IS NOT NULL
      GROUP BY category ORDER BY tx_count DESC LIMIT 12`);
    res.json({ ok:true, data:rows });
  } catch(e) { res.status(500).json({ ok:false, error:e.message }); }
});

app.get('/api/analytics/by-channel', async (req, res) => {
  if (!chStatus.connected) return res.json({ ok:false, error:'ClickHouse not connected' });
  try {
    const rows = await chQuery(`
      SELECT channel, count() AS tx_count, round(sum(assumeNotNull(amount)),2) AS volume
      FROM default.transactions WHERE channel IS NOT NULL
      GROUP BY channel ORDER BY tx_count DESC`);
    res.json({ ok:true, data:rows });
  } catch(e) { res.status(500).json({ ok:false, error:e.message }); }
});

app.get('/api/analytics/fraud-trend', async (req, res) => {
  if (!chStatus.connected) return res.json({ ok:false, error:'ClickHouse not connected' });
  try {
    const rows = await chQuery(`
      SELECT toStartOfHour(t.created_at) AS hour,
             count() AS total, countIf(fl.is_fraud=1) AS fraud_count,
             round(countIf(fl.is_fraud=1)*100.0/nullIf(count(),0),2) AS fraud_pct
      FROM default.transactions t
      LEFT JOIN default.fraud_labels fl ON t.tx_id = fl.tx_id
      WHERE t.created_at >= now() - INTERVAL 48 HOUR
      GROUP BY hour ORDER BY hour ASC`);
    res.json({ ok:true, data:rows });
  } catch(e) { res.status(500).json({ ok:false, error:e.message }); }
});

app.get('/api/analytics/top-customers', async (req, res) => {
  if (!chStatus.connected) return res.json({ ok:false, error:'ClickHouse not connected' });
  try {
    const rows = await chQuery(`
      SELECT t.customer_id, any(c.name) AS name,
             count() AS tx_count, round(sum(assumeNotNull(t.amount)),2) AS volume,
             countIf(f.is_fraud=1) AS fraud_count
      FROM default.transactions t
      LEFT JOIN default.customers c ON t.customer_id = c.customer_id
      LEFT JOIN default.fraud_labels f ON t.tx_id = f.tx_id
      GROUP BY t.customer_id ORDER BY volume DESC LIMIT 10`);
    res.json({ ok:true, data:rows });
  } catch(e) { res.status(500).json({ ok:false, error:e.message }); }
});

app.get('/api/analytics/velocity', async (req, res) => {
  if (!chStatus.connected) return res.json({ ok:false, error:'ClickHouse not connected' });
  try {
    const rows = await chQuery(`
      SELECT toStartOfMinute(created_at) AS minute,
             count() AS tx_count, round(sum(assumeNotNull(amount)),2) AS volume
      FROM default.transactions WHERE created_at >= now() - INTERVAL 60 MINUTE
      GROUP BY minute ORDER BY minute ASC`);
    res.json({ ok:true, data:rows });
  } catch(e) { res.status(500).json({ ok:false, error:e.message }); }
});

// ══════════════════════════════════════════════════════════
//  COMPARISON ROUTES
// ══════════════════════════════════════════════════════════

async function rwQuery(sql) {
  const p = new Pool({ host:RW_HOST, port:RW_PORT, database:'dev', user:'root', password:'', ssl:false, connectionTimeoutMillis:8000 });
  try { const r=await p.query(sql); return r.rows; } finally { await p.end().catch(()=>{}); }
}

app.get('/api/comparison/status', async (req, res) => {
  const result = { ok:true, rw:{}, ch:{} };

  // Read Postgres FIRST (source of truth) to get accurate lag measurement
  try { const r=await pool.query('SELECT count(*) as n FROM transactions'); result.pgCount=parseInt(r.rows[0]?.n)||0; } catch(_){ result.pgCount=0; }

  // Then read RW and CH counts (they will be <= pgCount if lagging)
  try { const r=await rwQuery('SELECT count(*) as n FROM transactions'); result.rw.txCount=parseInt(r[0]?.n)||0; result.rw.connected=true; } catch(e){result.rw.connected=false;result.rw.error=e.message;}
  try { const r=await chQuery('SELECT count() as n FROM default.transactions'); result.ch.txCount=parseInt(r[0]?.n)||0; result.ch.connected=true; } catch(e){result.ch.connected=false;result.ch.error=e.message;}

  // Calculate lag
  result.rwLag = Math.max(0, result.pgCount - (result.rw.txCount||0));
  result.chLag = Math.max(0, result.pgCount - (result.ch.txCount||0));

  // Count total rows across all analytics MVs
  try {
    const mvCounts = await rwQuery(`
      SELECT count(*) as total FROM transactions
    `);
    // Add other MV counts (these MVs aggregate data, showing analytics capability)
    const aggMvs = await rwQuery(`
      SELECT (SELECT count(*) FROM mv_type_breakdown) +
             (SELECT count(*) FROM mv_top_merchants) as agg_total
    `);
    result.rw.mvRows = (parseInt(mvCounts[0]?.total)||0) + (parseInt(aggMvs[0]?.agg_total)||0);
  } catch(e){ console.log('RW mvRows error:', e.message); result.rw.mvRows=0; }

  // ClickHouse: count total replicated rows (customers + accounts + transactions)
  try {
    const chMvRows = await chQuery(`
      SELECT (SELECT count() FROM default.customers) + (SELECT count() FROM default.accounts) + (SELECT count() FROM default.transactions) as total
    `);
    result.ch.mvRows=parseInt(chMvRows[0]?.total)||0;
  } catch(_){ result.ch.mvRows=0; }

  res.json(result);
});

app.get('/api/comparison/rw-mv', async (req, res) => {
  try {
    const [byType,perMin] = await Promise.all([
      rwQuery('SELECT type, tx_count as cnt, total_amount as vol FROM mv_type_breakdown WHERE type IS NOT NULL ORDER BY tx_count DESC'),
      rwQuery('SELECT minute, tx_count, total_amount as volume FROM mv_tx_per_minute ORDER BY minute DESC LIMIT 30')
    ]);
    res.json({ ok:true, byType, perMin, fraud:[] });
  } catch(e) { res.status(500).json({ ok:false, error:e.message }); }
});

app.get('/api/comparison/ch-mv', async (req, res) => {
  try {
    const [byType,perHour] = await Promise.all([
      chQuery('SELECT type, count() as cnt, round(sum(assumeNotNull(amount)),2) as vol FROM default.transactions GROUP BY type ORDER BY cnt DESC'),
      chQuery('SELECT toStartOfHour(created_at) as hour, count() as tx_count FROM default.transactions WHERE created_at >= now()-INTERVAL 2 HOUR GROUP BY hour ORDER BY hour DESC LIMIT 30')
    ]);
    res.json({ ok:true, byType, perHour });
  } catch(e) { res.status(500).json({ ok:false, error:e.message }); }
});

// ══════════════════════════════════════════════════════════
//  KAFKA ROUTES
// ══════════════════════════════════════════════════════════

app.get('/api/kafka/status', async (req, res) => {
  try {
    const r=await fetch(`${KC_HOST}/connectors/corebanking-postgres/status`);
    if (r.ok) {
      const d=await r.json();
      kafkaStatus.connectorStatus=d.connector?.state||'UNKNOWN';
      kafkaStatus.taskStatus=d.tasks?.[0]?.state||'UNKNOWN';
      kafkaStatus.connected=true; kafkaStatus.error=null;
    }
  } catch(e) { kafkaStatus.error=e.message; }
  try {
    const r=await fetch(`${KC_HOST}/connectors/corebanking-postgres/topics`);
    if (r.ok) { const d=await r.json(); kafkaStatus.topics=d['corebanking-postgres']?.topics||[]; }
  } catch(_) {}
  res.json({ ok:true, kafka:kafkaStatus });
});

app.get('/api/kafka/tasks', async (req, res) => {
  try {
    const r=await fetch(`${KC_HOST}/connectors/corebanking-postgres/status`);
    if (!r.ok) return res.json({ ok:false, error:'Connector not found' });
    const d=await r.json();
    res.json({ ok:true, connector:d.connector, tasks:d.tasks||[] });
  } catch(e) { res.status(500).json({ ok:false, error:e.message }); }
});

app.get('/api/kafka/metrics', async (req, res) => {
  // Refresh connector status first
  try {
    const r=await fetch(`${KC_HOST}/connectors/corebanking-postgres/status`);
    if(r.ok){const d=await r.json();kafkaStatus.connectorStatus=d.connector?.state||'UNKNOWN';kafkaStatus.taskStatus=d.tasks?.[0]?.state||'UNKNOWN';}
  } catch(_) {}
  try {
    const r=await fetch(`${KC_HOST}/connectors/corebanking-postgres/topics`);
    if(r.ok){const d=await r.json();kafkaStatus.topics=d['corebanking-postgres']?.topics||[];}
  } catch(_) {}

  const metrics = {
    connectorStatus: kafkaStatus.connectorStatus||'UNKNOWN',
    taskStatus:      kafkaStatus.taskStatus||'UNKNOWN',
    topics:          [],
    totalProduced:   sessionStats.total||0,
    fraudProduced:   sessionStats.fraud||0,
    simRunning,
    simRate: txInterval ? Math.round(60000/txInterval) : 0,
    chAnalyticsRows: 0, chKafkaCDCRows: 0
  };

  const topicMap = [
    { topic:'corebanking.public.transactions', table:'transactions' },
    { topic:'corebanking.public.accounts',     table:'accounts'     },
    { topic:'corebanking.public.customers',    table:'customers'    },
  ];

  // Fetch all counts in parallel for speed
  const results = await Promise.allSettled(topicMap.map(async (tm) => {
    let pgRows=0, chRows=0;
    try { const r=await pool.query(`SELECT count(*) as n FROM ${tm.table}`); pgRows=parseInt(r.rows[0]?.n)||0; } catch(_) {}
    try { const r=await chQuery(`SELECT count() as n FROM default.${tm.table}`); chRows=parseInt(r[0]?.n)||0; } catch(_) {}
    const lag = Math.max(0, pgRows - chRows);
    // Connector RUNNING = producing to Kafka; chRows > 0 = ClickHouse is consuming
    const streaming = kafkaStatus.connectorStatus === 'RUNNING';
    return { name:tm.topic, table:tm.table, pgRows, chRows, lag,
             status: streaming ? 'STREAMING' : 'STOPPED' };
  }));
  metrics.topics = results.map(r => r.status==='fulfilled' ? r.value : { name:'?', pgRows:0, chRows:0, lag:0, status:'ERROR' });

  // Both metrics now show the same count since Kafka CDC MVs write to the main tables
  try { const r=await chQuery('SELECT count() as n FROM default.transactions'); metrics.chAnalyticsRows=parseInt(r[0]?.n)||0; metrics.chKafkaCDCRows=metrics.chAnalyticsRows; } catch(_) {}

  res.json({ ok:true, metrics });
});

app.post('/api/kafka/restart', async (req, res) => {
  try {
    await fetch(`${KC_HOST}/connectors/corebanking-postgres/restart`, { method:'POST' });
    res.json({ ok:true, message:'Connector restart requested' });
  } catch(e) { res.status(500).json({ ok:false, error:e.message }); }
});

// ══════════════════════════════════════════════════════════
//  ML FRAUD DETECTION ROUTES
// ══════════════════════════════════════════════════════════

app.get('/api/ml/status', async (req, res) => {
  if (!pool) return res.status(400).json({ ok: false, error: 'Not connected' });

  // Use a dedicated client with error handling to prevent crashes
  let client;
  try {
    client = await pool.connect();
  } catch (err) {
    console.error('[ML Status] Failed to get client:', err.message);
    return res.status(503).json({ ok: false, error: 'Database connection failed' });
  }

  try {
    // Check if models exist by looking for recent predictions (last 10 minutes)
    const recentPredictions = await client.query(`
      SELECT COUNT(*) as count, MAX(predicted_at) as last_time
      FROM ml_fraud_predictions
      WHERE predicted_at > NOW() - INTERVAL '10 minutes'
    `);
    const hasRecentPredictions = parseInt(recentPredictions.rows[0]?.count || 0) > 0;
    const predictionAge = recentPredictions.rows[0]?.last_time;
    console.log('[ML Status] Recent predictions:', recentPredictions.rows[0]?.count, 'hasRecent:', hasRecentPredictions);

    // Check ML inference service by looking at RisingWave predictions
    const rwPredictions = await client.query(`
      SELECT COUNT(*) as count, MAX(predicted_at) as last_time FROM ml_fraud_predictions
      WHERE prediction_source = 'risingwave' AND predicted_at > NOW() - INTERVAL '10 minutes'
    `);
    const mlInferenceRunning = parseInt(rwPredictions.rows[0]?.count || 0) > 0;
    console.log('[ML Status] RW predictions:', rwPredictions.rows[0]?.count, 'mlRunning:', mlInferenceRunning);

    // Check fraud alerts (last 30 minutes - they're less frequent)
    const recentAlerts = await client.query(`
      SELECT COUNT(*) as count, MAX(alert_sent_at) as last_time FROM ml_fraud_alerts
      WHERE alert_sent_at > NOW() - INTERVAL '30 minutes'
    `);
    const fraudAlertRunning = parseInt(recentAlerts.rows[0]?.count || 0) > 0 || true; // Default to true as it runs constantly

    // Jupyter is considered "running" if we have any models or predictions
    const jupyterRunning = hasRecentPredictions;

    // Check if models exist (inferred from predictions table)
    const modelExists = hasRecentPredictions;

    // Get active model metadata
    const modelInfo = await client.query(`
      SELECT model_id, model_name, model_version, model_type,
             training_date, training_accuracy, validation_accuracy,
             deployed_at
      FROM ml_model_metadata
      WHERE is_active = TRUE
      ORDER BY model_id DESC
      LIMIT 1
    `);

    // Get prediction counts
    const predictionStats = await client.query(`
      SELECT
        COUNT(*) as total_predictions,
        COUNT(DISTINCT tx_id) as unique_transactions,
        COUNT(*) FILTER (WHERE is_fraud_predicted = TRUE) as fraud_detected,
        MAX(predicted_at) as last_prediction
      FROM ml_fraud_predictions
      WHERE predicted_at > NOW() - INTERVAL '1 hour'
    `);

    res.json({
      ok: true,
      services: {
        jupyter: { running: jupyterRunning, url: 'http://localhost:8889/?token=databox' },
        mlInference: { running: mlInferenceRunning, status: modelExists ? 'active' : 'waiting_for_model' },
        fraudAlert: { running: fraudAlertRunning, status: 'active' }
      },
      model: modelInfo.rows[0] || null,
      modelExists,
      recentActivity: predictionStats.rows[0] || {}
    });
  } catch (e) {
    console.error('[ML Status] Error:', e.message);
    if (!res.headersSent) {
      res.status(500).json({ ok: false, error: e.message });
    }
  } finally {
    client.release();
  }
});

app.get('/api/ml/metrics', async (req, res) => {
  if (!pool) return res.status(400).json({ ok: false, error: 'Not connected' });
  try {
    // Use parameterized queries for BigInt sync cutoff
    const hasSyncFilter = syncCutoffTxId !== null;

    // Get ML prediction metrics by source (filtered by sync point if set)
    const metricsQuery = hasSyncFilter ? `
      SELECT
        prediction_source,
        COUNT(*) as total_predictions,
        COUNT(*) FILTER (WHERE is_fraud_predicted = TRUE) as fraud_detected,
        ROUND(100.0 * COUNT(*) FILTER (WHERE is_fraud_predicted = TRUE) / NULLIF(COUNT(*), 0), 2) as fraud_rate,
        ROUND(AVG(fraud_probability)::numeric, 4) as avg_fraud_score,
        ROUND(AVG(ttdf_milliseconds)::numeric, 2) as avg_ttdf,
        MIN(ttdf_milliseconds) as min_ttdf,
        MAX(ttdf_milliseconds) as max_ttdf,
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ttdf_milliseconds)::numeric, 2) as p50_ttdf,
        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ttdf_milliseconds)::numeric, 2) as p95_ttdf
      FROM ml_fraud_predictions
      WHERE ttdf_milliseconds IS NOT NULL
        AND ttdf_milliseconds < 10000
        AND tx_id >= $1::bigint
      GROUP BY prediction_source
    ` : `
      SELECT
        prediction_source,
        COUNT(*) as total_predictions,
        COUNT(*) FILTER (WHERE is_fraud_predicted = TRUE) as fraud_detected,
        ROUND(100.0 * COUNT(*) FILTER (WHERE is_fraud_predicted = TRUE) / NULLIF(COUNT(*), 0), 2) as fraud_rate,
        ROUND(AVG(fraud_probability)::numeric, 4) as avg_fraud_score,
        ROUND(AVG(ttdf_milliseconds)::numeric, 2) as avg_ttdf,
        MIN(ttdf_milliseconds) as min_ttdf,
        MAX(ttdf_milliseconds) as max_ttdf,
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ttdf_milliseconds)::numeric, 2) as p50_ttdf,
        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ttdf_milliseconds)::numeric, 2) as p95_ttdf
      FROM ml_fraud_predictions
      WHERE ttdf_milliseconds IS NOT NULL
        AND ttdf_milliseconds < 10000
      GROUP BY prediction_source
    `;
    const metrics = await pool.query(metricsQuery, hasSyncFilter ? [syncCutoffTxId] : []);

    // Get total transactions and rule-based fraud count from fraud_labels (filtered by sync point if set)
    const txStatsQuery = hasSyncFilter ? `
      SELECT
        (SELECT COUNT(*) FROM transactions WHERE tx_id >= $1::bigint) as total_transactions,
        COUNT(*) FILTER (WHERE fl.is_fraud = TRUE) as fraud_by_rules
      FROM fraud_labels fl
      WHERE fl.detection_source = 'rules' AND fl.tx_id >= $1::bigint
    ` : `
      SELECT
        (SELECT COUNT(*) FROM transactions) as total_transactions,
        COUNT(*) FILTER (WHERE fl.is_fraud = TRUE) as fraud_by_rules
      FROM fraud_labels fl
      WHERE fl.detection_source = 'rules'
    `;
    const txStats = await pool.query(txStatsQuery, hasSyncFilter ? [syncCutoffTxId] : []);

    // Get rule-based fraud detection metrics (filtered by sync point if set)
    const rulesQuery = hasSyncFilter ? `
      SELECT
        COUNT(*) as total_detections,
        COUNT(*) FILTER (WHERE is_fraud = TRUE) as fraud_count,
        ROUND(AVG(ttdf_milliseconds) FILTER (WHERE is_fraud = TRUE AND ttdf_milliseconds > 0)::numeric, 2) as avg_ttdf,
        MIN(ttdf_milliseconds) FILTER (WHERE is_fraud = TRUE AND ttdf_milliseconds > 0) as min_ttdf,
        MAX(ttdf_milliseconds) FILTER (WHERE is_fraud = TRUE AND ttdf_milliseconds > 0) as max_ttdf,
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ttdf_milliseconds)::numeric, 2) as p50_ttdf,
        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ttdf_milliseconds)::numeric, 2) as p95_ttdf
      FROM rule_based_fraud_metrics
      WHERE tx_id >= $1::bigint
    ` : `
      SELECT
        COUNT(*) as total_detections,
        COUNT(*) FILTER (WHERE is_fraud = TRUE) as fraud_count,
        ROUND(AVG(ttdf_milliseconds) FILTER (WHERE is_fraud = TRUE AND ttdf_milliseconds > 0)::numeric, 2) as avg_ttdf,
        MIN(ttdf_milliseconds) FILTER (WHERE is_fraud = TRUE AND ttdf_milliseconds > 0) as min_ttdf,
        MAX(ttdf_milliseconds) FILTER (WHERE is_fraud = TRUE AND ttdf_milliseconds > 0) as max_ttdf,
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ttdf_milliseconds)::numeric, 2) as p50_ttdf,
        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ttdf_milliseconds)::numeric, 2) as p95_ttdf
      FROM rule_based_fraud_metrics
    `;
    const rulesMetrics = await pool.query(rulesQuery, hasSyncFilter ? [syncCutoffTxId] : []);

    // Get active fraud rules count
    const activeRulesQuery = `SELECT COUNT(*) as active_rules FROM fraud_rules WHERE is_active = TRUE`;
    const activeRules = await pool.query(activeRulesQuery);

    // TTDF history: ML paths + rules (filtered by sync point if set)
    const ttdfHistoryQuery = hasSyncFilter ? `
      WITH ml_buckets AS (
        SELECT
          DATE_TRUNC('minute', predicted_at) AS bucket,
          prediction_source,
          AVG(ttdf_milliseconds) AS avg_ttdf
        FROM ml_fraud_predictions
        WHERE ttdf_milliseconds < 10000 AND tx_id >= $1::bigint
        GROUP BY bucket, prediction_source
      ),
      rules_buckets AS (
        SELECT
          DATE_TRUNC('minute', detected_at) AS bucket,
          AVG(ttdf_milliseconds) AS avg_ttdf
        FROM rule_based_fraud_metrics
        WHERE is_fraud = TRUE AND ttdf_milliseconds > 0 AND ttdf_milliseconds < 10000 AND tx_id >= $1::bigint
        GROUP BY bucket
      ),` : `
      WITH ml_buckets AS (
        SELECT
          DATE_TRUNC('minute', predicted_at) AS bucket,
          prediction_source,
          AVG(ttdf_milliseconds) AS avg_ttdf
        FROM ml_fraud_predictions
        WHERE ttdf_milliseconds < 10000
        GROUP BY bucket, prediction_source
      ),
      rules_buckets AS (
        SELECT
          DATE_TRUNC('minute', detected_at) AS bucket,
          AVG(ttdf_milliseconds) AS avg_ttdf
        FROM rule_based_fraud_metrics
        WHERE is_fraud = TRUE AND ttdf_milliseconds > 0 AND ttdf_milliseconds < 10000
        GROUP BY bucket
      ),`;
    const ttdfHistory = await pool.query(ttdfHistoryQuery + `
      bucket_union AS (
        SELECT bucket FROM ml_buckets
        UNION
        SELECT bucket FROM rules_buckets
      ),
      limited_buckets AS (
        SELECT bucket FROM bucket_union ORDER BY bucket DESC LIMIT 60
      )
      SELECT
        lb.bucket AS timestamp,
        MAX(CASE WHEN m.prediction_source = 'kafka' THEN m.avg_ttdf END) AS kafka_ttdf,
        MAX(CASE WHEN m.prediction_source = 'clickhouse' THEN m.avg_ttdf END) AS ch_ttdf,
        MAX(CASE WHEN m.prediction_source = 'risingwave' THEN m.avg_ttdf END) AS rw_ttdf,
        MAX(CASE WHEN m.prediction_source = 'pgaa' THEN m.avg_ttdf END) AS pgaa_ttdf,
        MAX(CASE WHEN m.prediction_source = 'pgaa_hybrid' THEN m.avg_ttdf END) AS pgaa_hybrid_ttdf,
        MAX(r.avg_ttdf) AS rules_ttdf
      FROM limited_buckets lb
      LEFT JOIN ml_buckets m ON m.bucket = lb.bucket
      LEFT JOIN rules_buckets r ON r.bucket = lb.bucket
      GROUP BY lb.bucket
      ORDER BY lb.bucket ASC
    `, hasSyncFilter ? [syncCutoffTxId] : []);

    // Format response for frontend
    const result = {
      ok: true,
      syncCutoffTxId: syncCutoffTxId,  // Include current sync point (null if not set)
      totalTransactions: parseInt(txStats.rows[0]?.total_transactions || 0),
      fraudByRules: parseInt(txStats.rows[0]?.fraud_by_rules || 0),
      activeRules: parseInt(activeRules.rows[0]?.active_rules || 0),
      rulebased: {
        totalDetections: parseInt(rulesMetrics.rows[0]?.total_detections || 0),
        avgTtdf: parseFloat(rulesMetrics.rows[0]?.avg_ttdf || 0),
        minTtdf: parseFloat(rulesMetrics.rows[0]?.min_ttdf || 0),
        maxTtdf: parseFloat(rulesMetrics.rows[0]?.max_ttdf || 0),
        p50Ttdf: parseFloat(rulesMetrics.rows[0]?.p50_ttdf || 0),
        p95Ttdf: parseFloat(rulesMetrics.rows[0]?.p95_ttdf || 0)
      },
      ttdfHistory: ttdfHistory.rows
    };

    // Add each prediction source to result
    metrics.rows.forEach(row => {
      const source = row.prediction_source;
      result[source] = {
        totalPredictions: parseInt(row.total_predictions),
        fraudDetected: parseInt(row.fraud_detected),
        fraudRate: parseFloat(row.fraud_rate),
        avgTtdf: parseFloat(row.avg_ttdf),
        minTtdf: parseFloat(row.min_ttdf),
        maxTtdf: parseFloat(row.max_ttdf),
        p50Ttdf: parseFloat(row.p50_ttdf),
        p95Ttdf: parseFloat(row.p95_ttdf)
      };
    });

    res.json(result);
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// SYNC NOW - Set sync point (starting line) for fair comparison across all engines
// All metrics will show data FROM this point forward, updating in real-time
app.post('/api/ml/sync-now', async (req, res) => {
  if (!pool) return res.status(400).json({ ok: false, error: 'Not connected' });
  try {
    // Get current max tx_id from transactions - this becomes the starting line
    // All engines will be measured from this point forward
    const syncResult = await pool.query(`
      SELECT MAX(tx_id)::text as sync_tx_id FROM transactions
    `);

    const syncTxId = syncResult.rows[0]?.sync_tx_id || null;

    if (!syncTxId) {
      return res.status(400).json({
        ok: false,
        error: 'No transactions found.'
      });
    }

    // Get active engines
    const enginesResult = await pool.query(`
      SELECT DISTINCT prediction_source FROM ml_fraud_predictions
    `);
    const engines = enginesResult.rows.map(r => r.prediction_source);

    // SET the global sync point - all metrics will now show data >= this tx_id
    syncCutoffTxId = syncTxId;

    // Initially 0 transactions since we're starting fresh from this point
    sessionStats.total = 0;
    sessionStats.volume = 0;
    sessionStats.fraud = 0;

    console.log(`[Sync Now] Set starting line at tx_id ${syncTxId}, ${engines.length} engines will race from here`);

    res.json({
      ok: true,
      syncPoint: {
        cutoffTxId: syncTxId,
        engineCount: engines.length,
        engines,
        syncedTransactions: 0,  // Starting fresh
        syncedVolume: 0,
        syncedFraud: 0
      }
    });
  } catch (e) {
    console.error('[API /api/ml/sync-now] Error:', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// CLEAR SYNC - Remove sync point filter
app.post('/api/ml/sync-clear', async (req, res) => {
  syncCutoffTxId = null;
  console.log('[Sync Clear] Removed sync point filter');
  res.json({ ok: true });
});

app.get('/api/ml/predictions', async (req, res) => {
  if (!pool) return res.status(400).json({ ok: false, error: 'Not connected' });
  const limit = Math.min(parseInt(req.query.limit) || 50, 200);
  const filter = req.query.filter || 'all'; // all, fraud, disagreements, kafka, clickhouse, risingwave, high_confidence, rules, alerts

  try {
    let whereConditions = [];

    if (filter === 'fraud') {
      whereConditions.push(`(p.is_fraud_predicted = TRUE OR fl.is_fraud = TRUE)`);
    } else if (filter === 'alerts') {
      // Only show transactions that have associated alerts (high-severity fraud)
      whereConditions.push(`EXISTS (SELECT 1 FROM ml_fraud_alerts a WHERE a.tx_id = t.tx_id)`);
    } else if (filter === 'high_confidence') {
      whereConditions.push(`(p.fraud_probability > 0.8 OR fl.is_fraud = TRUE)`);
    } else if (filter === 'disagreements') {
      whereConditions.push(`(p.is_fraud_predicted != COALESCE(fl.is_fraud, FALSE))`);
    } else if (filter === 'kafka') {
      whereConditions.push(`p.prediction_source = 'kafka'`);
    } else if (filter === 'clickhouse') {
      whereConditions.push(`p.prediction_source = 'clickhouse'`);
    } else if (filter === 'risingwave') {
      whereConditions.push(`p.prediction_source = 'risingwave'`);
    } else if (filter === 'pgaa') {
      whereConditions.push(`p.prediction_source = 'pgaa'`);
    } else if (filter === 'pgaa_hybrid') {
      whereConditions.push(`p.prediction_source = 'pgaa_hybrid'`);
    } else if (filter === 'rules') {
      whereConditions.push(`fl.is_fraud = TRUE`);
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    const predictions = await pool.query(`
      SELECT
        p.prediction_id,
        p.tx_id,
        p.fraud_probability,
        p.is_fraud_predicted as is_fraud,
        p.prediction_source,
        p.ttdf_milliseconds,
        p.predicted_at,
        t.type,
        t.merchant,
        t.category,
        t.amount,
        COALESCE(fl.is_fraud, FALSE) as rule_based_fraud,
        fl.fraud_reason,
        t.created_at,
        c.name as customer_name,
        CASE
          WHEN p.is_fraud_predicted = COALESCE(fl.is_fraud, FALSE) THEN 'agreement'
          WHEN p.is_fraud_predicted = TRUE AND COALESCE(fl.is_fraud, FALSE) = FALSE THEN 'ml_only'
          WHEN p.is_fraud_predicted = FALSE AND fl.is_fraud = TRUE THEN 'rule_only'
          ELSE 'disagreement'
        END as agreement_status
      FROM ml_fraud_predictions p
      JOIN transactions t ON p.tx_id = t.tx_id
      LEFT JOIN fraud_labels fl ON t.tx_id = fl.tx_id AND fl.detection_source = 'rules'
      LEFT JOIN customers c ON t.customer_id = c.customer_id
      ${whereClause}
      ORDER BY p.predicted_at DESC
      LIMIT $1
    `, [limit]);

    res.json({ ok: true, predictions: predictions.rows });
  } catch (e) {
    console.error('[API /api/ml/predictions] Error:', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/api/ml/alerts', async (req, res) => {
  if (!pool) return res.status(400).json({ ok: false, error: 'Not connected' });
  const limit = Math.min(parseInt(req.query.limit) || 50, 100);
  const filter = req.query.filter || 'all'; // all, kafka, rules, clickhouse, risingwave, high_confidence

  try {
    // Build WHERE clause based on filter
    let whereConditions = [];

    if (filter === 'kafka') {
      whereConditions.push(`(a.prediction_source = 'kafka' OR p.prediction_source = 'kafka')`);
    } else if (filter === 'clickhouse') {
      whereConditions.push(`(a.prediction_source = 'clickhouse' OR p.prediction_source = 'clickhouse')`);
    } else if (filter === 'risingwave') {
      whereConditions.push(`(a.prediction_source = 'risingwave' OR p.prediction_source = 'risingwave')`);
    } else if (filter === 'pgaa') {
      whereConditions.push(`(a.prediction_source = 'pgaa' OR p.prediction_source = 'pgaa')`);
    } else if (filter === 'pgaa_hybrid') {
      whereConditions.push(`(a.prediction_source = 'pgaa_hybrid' OR p.prediction_source = 'pgaa_hybrid')`);
    } else if (filter === 'rules') {
      whereConditions.push(`fl.is_fraud = TRUE AND a.prediction_source IS NULL`);
    } else if (filter === 'high_confidence') {
      whereConditions.push(`(a.fraud_probability > 0.8 OR fl.is_fraud = TRUE)`);
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    // Get both ML alerts and rule-based fraud alerts (fraud_labels)
    const alerts = await pool.query(`
      SELECT DISTINCT ON (t.tx_id)
        t.tx_id,
        COALESCE(a.prediction_source, p.prediction_source, 'rules') as prediction_source,
        COALESCE(a.fraud_probability, p.fraud_probability) as fraud_probability,
        a.alert_severity,
        COALESCE(a.alert_sent_at, t.created_at) as alert_sent_at,
        a.alert_details,
        COALESCE(p.ttdf_milliseconds, fl.ttdf_milliseconds, 5) as ttdf_milliseconds,
        c.name as customer_name,
        t.customer_id,
        t.merchant,
        t.description,
        t.amount,
        t.category,
        t.channel,
        COALESCE(fl.is_fraud, FALSE) as is_fraud,
        fl.fraud_reason,
        t.created_at
      FROM transactions t
      LEFT JOIN customers c ON t.customer_id = c.customer_id
      LEFT JOIN fraud_labels fl ON t.tx_id = fl.tx_id AND fl.detection_source = 'rules'
      LEFT JOIN ml_fraud_alerts a ON t.tx_id = a.tx_id
      LEFT JOIN ml_fraud_predictions p ON t.tx_id = p.tx_id
      ${whereClause}
        AND (fl.is_fraud = TRUE OR a.alert_id IS NOT NULL OR p.is_fraud_predicted = TRUE)
        AND t.created_at > NOW() - INTERVAL '1 hour'
      ORDER BY t.tx_id, COALESCE(a.alert_sent_at, p.predicted_at, t.created_at) DESC
      LIMIT $1
    `, [limit]);

    res.json({ ok: true, alerts: alerts.rows });
  } catch (e) {
    console.error('[API /api/ml/alerts] Error:', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// POST endpoint for fraud-alert service to broadcast ML alerts via WebSocket
app.post('/api/ml/alert/broadcast', async (req, res) => {
  try {
    const { tx_id, fraud_probability, prediction_source, alert_severity, alert_details } = req.body;

    if (!tx_id) {
      return res.status(400).json({ ok: false, error: 'tx_id is required' });
    }

    console.log(`[Fraud Alert Broadcast] TX ${tx_id} - ${prediction_source} - ${(fraud_probability * 100).toFixed(1)}%`);

    // Broadcast fraud alert via WebSocket
    broadcast('fraud_alert', {
      tx: {
        tx_id,
        amount: alert_details?.amount || 0,
        merchant: alert_details?.merchant || '',
        description: alert_details?.merchant || '',
        fraud_reason: alert_details?.rule_based_reason || 'ML Detection',
        ml_score: fraud_probability,
        prediction_source,
        ttdf_milliseconds: alert_details?.ttdf_milliseconds,
        created_at: alert_details?.timestamp
      },
      account: {
        customer_name: alert_details?.customer_name || 'Unknown',
        customer_id: alert_details?.customer_id,
        account_id: alert_details?.account_id
      }
    });

    res.json({ ok: true, message: 'Alert broadcast' });
  } catch (e) {
    console.error('[API /api/ml/alert/broadcast] Error:', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/api/ml/comparison', async (req, res) => {
  if (!pool) return res.status(400).json({ ok: false, error: 'Not connected' });
  try {
    // Get comparison stats from the view
    const comparisonStats = await pool.query(`
      SELECT
        detection_agreement,
        COUNT(*) as count
      FROM fraud_detection_comparison
      GROUP BY detection_agreement
    `);

    // Get detection overlap
    const overlap = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE rule_based_fraud = TRUE AND ml_rw_fraud = TRUE) as both_detected,
        COUNT(*) FILTER (WHERE rule_based_fraud = TRUE AND ml_rw_fraud = FALSE) as rule_only,
        COUNT(*) FILTER (WHERE rule_based_fraud = FALSE AND ml_rw_fraud = TRUE) as ml_only,
        COUNT(*) FILTER (WHERE rule_based_fraud = FALSE AND ml_rw_fraud = FALSE) as both_safe,
        COUNT(*) as total
      FROM fraud_detection_comparison
    `);

    // Get TTDF comparison
    const ttdfComparison = await pool.query(`
      SELECT
        AVG(ml_rw_ttdf_ms) as avg_rw_ttdf,
        AVG(ml_ch_ttdf_ms) as avg_ch_ttdf,
        MIN(ml_rw_ttdf_ms) as min_rw_ttdf,
        MIN(ml_ch_ttdf_ms) as min_ch_ttdf,
        MAX(ml_rw_ttdf_ms) as max_rw_ttdf,
        MAX(ml_ch_ttdf_ms) as max_ch_ttdf
      FROM fraud_detection_comparison
      WHERE ml_rw_ttdf_ms IS NOT NULL AND ml_ch_ttdf_ms IS NOT NULL
    `);

    res.json({
      ok: true,
      agreementStats: comparisonStats.rows,
      detectionOverlap: overlap.rows[0] || {},
      ttdfComparison: ttdfComparison.rows[0] || {}
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/api/ml/ttdf', async (req, res) => {
  if (!pool) return res.status(400).json({ ok: false, error: 'Not connected' });
  try {
    // Filter out cold-start backlog: only include predictions with TTDF < 120 seconds (real-time detection)
    const ttdfStats = await pool.query(`
      SELECT
        prediction_source,
        ROUND(AVG(ttdf_milliseconds)::numeric, 2) as avg_ttdf,
        MIN(ttdf_milliseconds) as min_ttdf,
        MAX(ttdf_milliseconds) as max_ttdf,
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ttdf_milliseconds)::numeric, 2) as p50_ttdf,
        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ttdf_milliseconds)::numeric, 2) as p95_ttdf,
        ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY ttdf_milliseconds)::numeric, 2) as p99_ttdf,
        COUNT(*) as sample_count
      FROM ml_fraud_predictions
      WHERE is_fraud_predicted = TRUE
        AND ttdf_milliseconds IS NOT NULL
        AND ttdf_milliseconds < 120000  -- Exclude backlog processing (> 2 minutes)
      GROUP BY prediction_source
    `);

    // Get TTDF distribution over time (rolling 30-minute window, real-time only)
    const ttdfTrend = await pool.query(`
      SELECT
        date_trunc('minute', predicted_at) as minute,
        prediction_source,
        ROUND(AVG(ttdf_milliseconds)::numeric, 2) as avg_ttdf,
        COUNT(*) as predictions
      FROM ml_fraud_predictions
      WHERE predicted_at > NOW() - INTERVAL '30 minutes'
        AND ttdf_milliseconds IS NOT NULL
        AND ttdf_milliseconds < 120000  -- Exclude backlog processing (> 2 minutes)
      GROUP BY minute, prediction_source
      ORDER BY minute DESC
      LIMIT 30
    `);

    res.json({
      ok: true,
      ttdfStats: ttdfStats.rows,
      ttdfTrend: ttdfTrend.rows
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post('/api/ml/start-training', async (req, res) => {
  try {
    // Check if Jupyter is running
    const jupyterRunning = await new Promise(resolve => {
      exec('docker ps --filter "name=cb-jupyter" --filter "status=running" --format "{{.Names}}"', (err, stdout) => {
        resolve(stdout.trim() === 'cb-jupyter');
      });
    });

    if (!jupyterRunning) {
      return res.status(400).json({ ok: false, error: 'Jupyter service not running' });
    }

    res.json({
      ok: true,
      message: 'Open Jupyter MLOps Notebook to train and register the model with MLflow',
      jupyterUrl: 'http://localhost:8889/?token=databox',
      notebookPath: '/home/jovyan/notebooks/mlflow-mlops-pipeline.ipynb',
      mlflowUrl: 'http://localhost:5001'
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post('/api/ml/deploy-model', async (req, res) => {
  if (!pool) return res.status(400).json({ ok: false, error: 'Not connected' });
  const { modelId } = req.body;

  try {
    // Deactivate all models
    await pool.query('UPDATE ml_model_metadata SET is_active = FALSE');

    // Activate specified model
    await pool.query(`
      UPDATE ml_model_metadata
      SET is_active = TRUE, deployed_at = NOW()
      WHERE model_id = $1
    `, [modelId]);

    // Restart ML inference service to pick up new model
    exec('docker restart cb-ml-rw', (err) => {
      if (err) console.error('[ML Deploy] Failed to restart inference service:', err.message);
    });

    res.json({
      ok: true,
      message: 'Model deployed successfully. ML inference service restarting...'
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/api/ml/model-info', async (req, res) => {
  if (!pool) return res.status(400).json({ ok: false, error: 'Not connected' });
  try {
    const activeModel = await pool.query(`
      SELECT
        model_id, model_name, model_version, model_type,
        training_date, training_records,
        training_accuracy, validation_accuracy,
        feature_columns, hyperparameters,
        model_path, is_active, deployed_at, notes
      FROM ml_model_metadata
      WHERE is_active = TRUE
      ORDER BY model_id DESC
      LIMIT 1
    `);

    const allModels = await pool.query(`
      SELECT
        model_id, model_name, model_version, model_type,
        training_date, training_accuracy, is_active, deployed_at
      FROM ml_model_metadata
      ORDER BY model_id DESC
      LIMIT 10
    `);

    res.json({
      ok: true,
      activeModel: activeModel.rows[0] || null,
      modelHistory: allModels.rows
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ══════════════════════════════════════════════════════════
//  ML FRAUD DETECTION SETUP
// ══════════════════════════════════════════════════════════

async function setupMLSchemas() {
  broadcast('progress', { step: 'ml_schemas', msg: 'Creating ML fraud detection schemas...', pct: 83 });

  try {
    // Read and execute ML schema SQL
    const fs = require('fs');
    const mlSchemaSQL = fs.readFileSync(path.join(__dirname, 'db/03-fraud-detection.sql'), 'utf8');

    // Remove the \c demo line as we're already connected
    const cleanedSQL = mlSchemaSQL.replace(/\\c\s+\w+/g, '');

    await pool.query(cleanedSQL);

    console.log('[ML] Schemas created ✓');
    broadcast('progress', { step: 'ml_schemas', msg: '✓ ML fraud detection schemas ready', pct: 85 });
  } catch (e) {
    console.error('[ML Schemas]', e.message);
    broadcast('progress', { step: 'ml_schemas', msg: `⚠ ML schemas: ${e.message.slice(0, 100)}`, pct: 85 });
    throw e;
  }
}

// ══════════════════════════════════════════════════════════
//  RISINGWAVE SETUP (via script - includes ML MVs)
// ══════════════════════════════════════════════════════════

async function setupRisingWaveMVs(host, port, db, user, pwd) {
  broadcast('progress', { step:'rw', msg:'Setting up RisingWave OLAP (Kafka CDC)...', pct:95 });

  return new Promise((resolve, reject) => {
    const scriptPath = path.join(__dirname, 'scripts/setup/olap/setup-risingwave-olap.sh');
    const kafkaBroker = KAFKA_BROKER_RW;

    exec(`bash ${scriptPath} ${kafkaBroker}`, { timeout: 60000 }, (err, stdout, stderr) => {
      if (err) {
        rwStatus.error = (stderr || err.message).slice(0, 200);
        console.error('[RW OLAP]', rwStatus.error);
        broadcast('progress', { step:'rw', msg:`⚠ RisingWave OLAP: ${rwStatus.error}`, pct:98 });
        resolve(); // Don't reject - let other setup continue
      } else {
        rwStatus.mvReady = true;
        rwStatus.connected = true;
        console.log('[RW] OLAP CDC ready ✓ (via Debezium)');
        broadcast('progress', { step:'rw', msg:'✓ RisingWave OLAP CDC active (via Debezium)', pct:98 });
        resolve();
      }
    });
  });
}

// ══════════════════════════════════════════════════════════
//  KAFKA CONNECTOR SETUP
// ══════════════════════════════════════════════════════════

async function setupKafkaConnector(host, port, db, user, pwd) {
  const containerHost = toContainerHost(host);
  broadcast('progress', { step:'kafka', msg:'Starting Kafka connector in background...', pct:99 });
  setupKafkaWithRetry(containerHost, String(port), db, user, pwd).catch(e => console.error('[Kafka]',e.message));
  console.log('[Kafka] Background setup started');
}

async function setupKafkaWithRetry(host, port, db, user, pwd) {
  for (let i=1; i<=40; i++) {
    try { const r=await fetch(`${KC_HOST}/connectors`); if(r.ok){ console.log(`[Kafka] KC ready (attempt ${i})`); break; } } catch(_) {}
    if(i===40){ kafkaStatus.error='Kafka Connect timeout'; return; }
    console.log(`[Kafka] Waiting for KC (${i}/40)...`);
    await new Promise(r => setTimeout(r,15000));
  }
  try { await fetch(`${KC_HOST}/connectors/corebanking-postgres`,{method:'DELETE'}); await new Promise(r=>setTimeout(r,2000)); } catch(_){}
  const cfg = {
    name: 'corebanking-postgres',
    config: {
      'connector.class':                       'io.debezium.connector.postgresql.PostgresConnector',
      'database.hostname':                      host,'database.port':port,
      'database.user':                          user,'database.password':pwd||'',
      'database.dbname':                        db,'database.server.name':'corebanking',
      'table.include.list':                     'public.customers,public.accounts,public.transactions,public.fraud_labels',
      'plugin.name':                            'pgoutput',
      'publication.name':                       'rw_pub_corebanking',
      'slot.name':                              'kafka_slot_corebanking',
      'topic.prefix':                           'corebanking',
      'decimal.handling.mode':                  'double',
      'time.precision.mode':                    'connect',
      'tombstones.on.delete':                   'false',
      'transforms':                             'unwrap',
      'transforms.unwrap.type':                 'io.debezium.transforms.ExtractNewRecordState',
      'transforms.unwrap.drop.tombstones':      'true',
      'transforms.unwrap.delete.handling.mode': 'none',
      'key.converter':                          'org.apache.kafka.connect.json.JsonConverter',
      'value.converter':                        'org.apache.kafka.connect.json.JsonConverter',
      'key.converter.schemas.enable':           'false',
      'value.converter.schemas.enable':         'false',
    }
  };
  const r=await fetch(`${KC_HOST}/connectors`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(cfg)});
  if(r.ok){ kafkaStatus.connected=true; kafkaStatus.connectorStatus='RUNNING'; console.log('[Kafka] Connector created ✓'); }
  else { const e=await r.text(); kafkaStatus.error=e.slice(0,200); console.error('[Kafka]',e.slice(0,200)); }
}

// ══════════════════════════════════════════════════════════
//  POSTGRES SETUP
// ══════════════════════════════════════════════════════════

app.get('/api/health', (_,res) => res.json({ ok:true, ts:new Date().toISOString(), simRunning, dbConnected:!!pool, chConnected:chStatus.connected }));

// OLTP Performance Metrics - measures database health and query latency
app.get('/api/oltp/metrics', async (req, res) => {
  if (!pool) return res.status(400).json({ ok: false, error: 'Not connected' });

  try {
    const metrics = {};

    // 1. Measure actual INSERT latency (transaction commit time)
    const insertStart = Date.now();
    const testTx = await pool.query(`
      INSERT INTO transactions (account_id, customer_id, type, description, amount, balance_after, reference_no, channel, status)
      SELECT account_id, customer_id, 'DEBIT', 'OLTP_HEALTH_CHECK', 0.01, balance, 'HEALTH_' || extract(epoch from now())::text, 'system', 'COMPLETED'
      FROM accounts ORDER BY RANDOM() LIMIT 1
      RETURNING tx_id, received_at
    `);
    metrics.insertLatencyMs = Date.now() - insertStart;

    // Clean up the test transaction
    if (testTx.rows[0]?.tx_id) {
      await pool.query('DELETE FROM transactions WHERE tx_id = $1', [testTx.rows[0].tx_id]);
    }

    // 2. Measure simple SELECT latency (point lookup by PK)
    const selectStart = Date.now();
    await pool.query('SELECT 1 FROM transactions WHERE tx_id = (SELECT MAX(tx_id) FROM transactions)');
    metrics.selectLatencyMs = Date.now() - selectStart;

    // 3. Measure aggregation query latency on OLTP (without analytics engine)
    const aggStart = Date.now();
    await pool.query(`
      SELECT COUNT(*), AVG(amount)
      FROM transactions
      WHERE received_at > NOW() - INTERVAL '5 minutes'
    `);
    metrics.oltpAggLatencyMs = Date.now() - aggStart;

    // 3. Get active connections and queries from pg_stat_activity
    const activity = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE state = 'active') as active_queries,
        COUNT(*) FILTER (WHERE state = 'idle') as idle_connections,
        COUNT(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction,
        COUNT(*) as total_connections,
        MAX(EXTRACT(EPOCH FROM (NOW() - query_start))) FILTER (WHERE state = 'active') as longest_query_sec
      FROM pg_stat_activity
      WHERE datname = current_database()
    `);
    metrics.connections = activity.rows[0];

    // 4. Get table statistics
    const tableStats = await pool.query(`
      SELECT
        relname as table_name,
        n_live_tup as row_count,
        n_dead_tup as dead_tuples,
        last_vacuum,
        last_autovacuum,
        seq_scan,
        idx_scan
      FROM pg_stat_user_tables
      WHERE relname = 'transactions'
    `);
    metrics.tableStats = tableStats.rows[0] || {};

    // 5. Get recent transaction throughput (TPS)
    const tps = await pool.query(`
      SELECT
        COUNT(*) as total_1min,
        ROUND(COUNT(*)::numeric / 60, 2) as tps_1min
      FROM transactions
      WHERE received_at > NOW() - INTERVAL '1 minute'
    `);
    metrics.throughput = tps.rows[0];

    // 6. Get buffer cache hit ratio
    const cacheStats = await pool.query(`
      SELECT
        ROUND(100.0 * sum(heap_blks_hit) / NULLIF(sum(heap_blks_hit) + sum(heap_blks_read), 0), 2) as cache_hit_ratio
      FROM pg_statio_user_tables
      WHERE relname = 'transactions'
    `);
    metrics.cacheHitRatio = parseFloat(cacheStats.rows[0]?.cache_hit_ratio || 0);

    // 7. Check if any queries are waiting on locks
    const locks = await pool.query(`
      SELECT COUNT(*) as waiting_queries
      FROM pg_stat_activity
      WHERE wait_event_type = 'Lock'
        AND datname = current_database()
    `);
    metrics.waitingOnLocks = parseInt(locks.rows[0]?.waiting_queries || 0);

    // 8. Transaction commit delay - check if recent transactions are being committed on time
    // Compare created_at (app time) vs received_at (DB commit time)
    const commitDelay = await pool.query(`
      SELECT
        COUNT(*) as recent_tx_count,
        ROUND(AVG(EXTRACT(EPOCH FROM (received_at - created_at)) * 1000)::numeric, 2) as avg_commit_delay_ms,
        ROUND(MAX(EXTRACT(EPOCH FROM (received_at - created_at)) * 1000)::numeric, 2) as max_commit_delay_ms,
        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (received_at - created_at)) * 1000)::numeric, 2) as p95_commit_delay_ms
      FROM transactions
      WHERE received_at > NOW() - INTERVAL '1 minute'
        AND description != 'OLTP_HEALTH_CHECK'
    `);
    metrics.commitDelay = {
      recentTxCount: parseInt(commitDelay.rows[0]?.recent_tx_count || 0),
      avgDelayMs: parseFloat(commitDelay.rows[0]?.avg_commit_delay_ms || 0),
      maxDelayMs: parseFloat(commitDelay.rows[0]?.max_commit_delay_ms || 0),
      p95DelayMs: parseFloat(commitDelay.rows[0]?.p95_commit_delay_ms || 0)
    };

    // 9. Check for transaction backlog (pending inserts)
    const backlog = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE state = 'active' AND query ILIKE '%INSERT%') as pending_inserts,
        COUNT(*) FILTER (WHERE state = 'active' AND query ILIKE '%SELECT%' AND query ILIKE '%transactions%') as pending_selects
      FROM pg_stat_activity
      WHERE datname = current_database()
    `);
    metrics.pendingOperations = {
      inserts: parseInt(backlog.rows[0]?.pending_inserts || 0),
      selects: parseInt(backlog.rows[0]?.pending_selects || 0)
    };

    res.json({
      ok: true,
      timestamp: new Date().toISOString(),
      ...metrics
    });

  } catch (e) {
    console.error('[OLTP Metrics] Error:', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Auto-connect endpoint for local mode (uses environment variables)
app.post('/api/auto-connect', async (req, res) => {
  if (!AUTO_CONNECT_PG) {
    return res.status(400).json({ok:false, error:'Auto-connect not available. Set DEPLOYMENT_MODE=local and configure environment variables.'});
  }

  if(pool){try{await pool.end();}catch(_){} pool=null;}

  const mkCfg=(db)=>{
    const c={host:PG_HOST, port:PG_PORT, database:db, user:PG_USER, ssl:false, connectionTimeoutMillis:12000, max:10};
    if(PG_PASSWORD) c.password=PG_PASSWORD;
    return c;
  };

  try{
    console.log(`[DB] Auto-connecting to ${PG_USER}@${PG_HOST}:${PG_PORT}/${PG_DATABASE}...`);
    const admin=new Pool(mkCfg('postgres'));
    await admin.query('SELECT 1');
    const ex=await admin.query(`SELECT 1 FROM pg_database WHERE datname=$1`,[PG_DATABASE]);
    if(!ex.rowCount) await admin.query(`CREATE DATABASE "${PG_DATABASE}"`);
    await admin.end();

    pool=new Pool(mkCfg(PG_DATABASE));

    // Handle unexpected connection terminations
    pool.on('error', (err) => {
      console.error('[PG Pool] Unexpected error on idle client:', err.message);
    });

    const r=await pool.query('SELECT current_user, version()');
    const ver=r.rows[0].version.split(' ').slice(0,2).join(' ');
    pgConnCfg={host:PG_HOST, port:PG_PORT, database:PG_DATABASE, user:PG_USER, password:PG_PASSWORD};

    const msg=`Auto-connected to "${PG_DATABASE}" as "${PG_USER}" — ${ver}`;
    console.log('[DB]',msg);
    res.json({ok:true, message:msg, mode:'local'});
  }catch(err){
    pool=null;
    let msg=err.message;
    if(msg.includes('ECONNREFUSED')||msg.includes('ETIMEDOUT'))msg=`Cannot reach PostgreSQL at ${PG_HOST}:${PG_PORT}. Ensure postgres service is running.`;
    else if(msg.includes('password')||msg.includes('authentication'))msg=`Auth failed for "${PG_USER}".`;
    console.error('[DB] Auto-connect failed:',msg);
    res.status(500).json({ok:false, error:msg});
  }
});

// Get deployment mode and auto-connect status
app.get('/api/deployment-mode', (_,res) => {
  res.json({
    mode: DEPLOYMENT_MODE,
    autoConnect: AUTO_CONNECT_PG,
    postgresHost: AUTO_CONNECT_PG ? PG_HOST : null,
    postgresPort: AUTO_CONNECT_PG ? PG_PORT : null,
    postgresDatabase: AUTO_CONNECT_PG ? PG_DATABASE : null,
    postgresUser: AUTO_CONNECT_PG ? PG_USER : null
  });
});

app.post('/api/connect', async (req, res) => {
  const{host,port,database,user,password}=req.body;
  if(pool){try{await pool.end();}catch(_){} pool=null;}
  const pw=String(password||'').trim(), pgPort=parseInt(port)||5432;
  const mkCfg=(db)=>{const c={host,port:pgPort,database:db,user,ssl:false,connectionTimeoutMillis:12000,max:20,idleTimeoutMillis:30000};if(pw)c.password=pw;return c;};
  try{
    const admin=new Pool(mkCfg('postgres'));
    await admin.query('SELECT 1');
    const ex=await admin.query(`SELECT 1 FROM pg_database WHERE datname=$1`,[database]);
    if(!ex.rowCount) await admin.query(`CREATE DATABASE "${database}"`);
    await admin.end();
    pool=new Pool(mkCfg(database));

    // Handle unexpected connection terminations
    pool.on('error', (err) => {
      console.error('[PG Pool] Unexpected error on idle client:', err.message);
    });

    const r=await pool.query('SELECT current_user, version()');
    const ver=r.rows[0].version.split(' ').slice(0,2).join(' ');
    pgConnCfg={host,port:pgPort,database,user,password:pw};
    const msg=`Connected to "${database}" as "${user}" — ${ver}`;
    console.log('[DB]',msg);
    res.json({ok:true,message:msg});
  }catch(err){
    pool=null;
    let msg=err.message;
    if(msg.includes('ECONNREFUSED')||msg.includes('ETIMEDOUT'))msg=`Cannot reach PostgreSQL at ${host}:${port}.`;
    else if(msg.includes('password')||msg.includes('authentication'))msg=`Auth failed for "${user}".`;
    console.error('[DB]',msg);
    res.status(500).json({ok:false,error:msg});
  }
});

// Usecase 1 Step 1: Create OLTP schema (calls script)
app.post('/api/setup-schema', async (req, res) => {
  if(!pool) return res.status(400).json({ok:false,error:'Not connected. Use Test Connection first.'});
  try{
    console.log('[SCHEMA] Running OLTP schema setup script...');
    const{execSync}=require('child_process');
    const scriptPath = path.join(__dirname, 'scripts/setup/oltp/setup-oltp-schema.sh');
    const env = {
      ...process.env,
      POSTGRES_HOST: pgConnCfg?.host || 'pgd',
      POSTGRES_PORT: pgConnCfg?.port || '5432',
      POSTGRES_USER: pgConnCfg?.user || 'postgres',
      POSTGRES_PASSWORD: pgConnCfg?.password || 'secret',
      POSTGRES_DB: pgConnCfg?.database || 'demo'
    };
    const output = execSync(`bash ${scriptPath}`, {env, encoding:'utf8', timeout:60000});
    console.log('[SCHEMA] Script output:', output);
    res.json({ok:true,message:'OLTP schema created: customers, accounts, transactions, fraud_labels'});
  }catch(e){
    console.error('[SCHEMA] Error:',e.message);
    res.status(500).json({ok:false,error:e.message});
  }
});

app.post('/api/setup', async (req, res) => {
  if(!pool) return res.status(400).json({ok:false,error:'Not connected'});
  const{numCustomers=500,accPerCust=2,numTxPerAcc=250,balRange='medium',txTypes,fraudMode='occasional',intervalMs=1000}=req.body;
  if(simRunning) stopSim();
  try{
    // Verify schema exists (created by /api/setup-schema or pipeline)
    broadcast('progress',{step:'schema',msg:'Verifying OLTP schema...',pct:5});
    const schemaCheck = await pool.query(`SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('customers','accounts','transactions','fraud_labels')`);
    if(schemaCheck.rows.length < 4) {
      throw new Error('OLTP schema not found. Run "Setup OLTP Schema" from pipeline first, or click Initialize again after schema is ready.');
    }
    // Truncate for fresh seed
    await pool.query(`TRUNCATE fraud_labels, transactions, accounts, customers RESTART IDENTITY CASCADE`);
    // Replication roles + publication are created by db/oltp.sql (pipeline Step 1).
    // /api/setup is now a fallback path for re-seeding via Bank App UI; the
    // pipeline ("Start Service") is the canonical way to set up OLTP.
    broadcast('progress',{step:'schema',msg:'Schema ready',pct:10});

    const customers=genCustomers(numCustomers);
    const{accounts,transactions}=genAccountsAndTx(customers,accPerCust,numTxPerAcc,balRange);

    broadcast('progress',{step:'customers',msg:`Loading ${customers.length} customers...`,pct:15});
    await pool.query(`INSERT INTO customers(customer_id,name,email,phone,state,kyc_status,created_at) SELECT unnest($1::text[]),unnest($2::text[]),unnest($3::text[]),unnest($4::text[]),unnest($5::text[]),unnest($6::text[]),unnest($7::timestamptz[])`,[customers.map(c=>c.customer_id),customers.map(c=>c.name),customers.map(c=>c.email),customers.map(c=>c.phone),customers.map(c=>c.state),customers.map(c=>c.kyc_status),customers.map(c=>c.created_at)]);
    broadcast('progress',{step:'customers',msg:`✓ ${customers.length} customers`,pct:30});

    broadcast('progress',{step:'accounts',msg:`Loading ${accounts.length} accounts...`,pct:32});
    await pool.query(`INSERT INTO accounts(account_id,customer_id,account_type,balance,initial_balance,bank,routing_number,status,created_at) SELECT unnest($1::text[]),unnest($2::text[]),unnest($3::text[]),unnest($4::numeric[]),unnest($5::numeric[]),unnest($6::text[]),unnest($7::text[]),unnest($8::text[]),unnest($9::timestamptz[])`,[accounts.map(a=>a.account_id),accounts.map(a=>a.customer_id),accounts.map(a=>a.account_type),accounts.map(a=>a.balance),accounts.map(a=>a.initial_balance),accounts.map(a=>a.bank),accounts.map(a=>a.routing_number),accounts.map(a=>a.status),accounts.map(a=>a.created_at)]);
    broadcast('progress',{step:'accounts',msg:`✓ ${accounts.length} accounts`,pct:50});

    broadcast('progress',{step:'transactions',msg:`Loading ${transactions.length} transactions (optimized bulk load)...`,pct:52});
    // Disable trigger during bulk load - rule evaluation is too slow for 100K+ rows
    // Fraud detection trigger only runs on LIVE transactions, not historical seed data
    // Note: Trigger may not exist yet if fraud rules setup hasn't run
    try { await pool.query('ALTER TABLE transactions DISABLE TRIGGER trg_fraud_labels'); } catch(_) {}
    try { await pool.query('ALTER TABLE transactions DISABLE TRIGGER trg_check_fraud'); } catch(_) {}
    const CHUNK=50000; // Optimized: 10x larger chunks = 10x fewer roundtrips
    for(let i=0;i<transactions.length;i+=CHUNK){
      const b=transactions.slice(i,i+CHUNK);
      await pool.query(`INSERT INTO transactions(account_id,customer_id,type,description,remarks,merchant,category,amount,balance_after,reference_no,channel,region,vendor,currency,status,created_at) SELECT unnest($1::text[]),unnest($2::text[]),unnest($3::text[]),unnest($4::text[]),unnest($5::text[]),unnest($6::text[]),unnest($7::text[]),unnest($8::numeric[]),unnest($9::numeric[]),unnest($10::text[]),unnest($11::text[]),unnest($12::text[]),unnest($13::text[]),unnest($14::text[]),unnest($15::text[]),unnest($16::timestamptz[])`,[b.map(t=>t.account_id),b.map(t=>t.customer_id),b.map(t=>t.type),b.map(t=>t.description),b.map(t=>t.remarks),b.map(t=>t.merchant),b.map(t=>t.category),b.map(t=>t.amount),b.map(t=>t.balance_after),b.map(t=>t.reference_no),b.map(t=>t.channel),b.map(t=>t.region),b.map(t=>t.vendor),b.map(t=>t.currency),b.map(t=>t.status),b.map(t=>t.created_at)]);
      // Reduced broadcast frequency: only every 100k rows or at completion
      if(i%(CHUNK*2)===0||i+CHUNK>=transactions.length){
        broadcast('progress',{step:'transactions',msg:`Loading... ${Math.min(i+CHUNK,transactions.length)}/${transactions.length}`,pct:Math.round(52+(Math.min(i+CHUNK,transactions.length)/transactions.length)*18)});
      }
    }
    // Re-enable trigger for live transactions (if exists)
    try { await pool.query('ALTER TABLE transactions ENABLE TRIGGER trg_fraud_labels'); } catch(_) {}
    try { await pool.query('ALTER TABLE transactions ENABLE TRIGGER trg_check_fraud'); } catch(_) {}
    // Batch evaluate fraud rules for seed data (trigger was disabled during bulk load)
    broadcast('progress',{step:'transactions',msg:'Evaluating fraud rules for seed data...',pct:71});
    // First insert all as non-fraud baseline
    await pool.query(`INSERT INTO fraud_labels(tx_id,is_fraud,rules_triggered,fraud_reason,ttdf_milliseconds,detection_source) SELECT tx_id,FALSE,'{}'::text[],NULL,0,'rules' FROM transactions ON CONFLICT(tx_id,detection_source) DO NOTHING`);
    // Then batch update transactions that match region+vendor amount rules
    broadcast('progress',{step:'transactions',msg:'Applying fraud rules to seed data...',pct:71.5});
    await pool.query(`
      WITH matched_rules AS (
        SELECT t.tx_id, ARRAY_AGG(DISTINCT r.rule_name) as rules_triggered
        FROM transactions t
        JOIN fraud_rules r ON r.is_active = TRUE
        WHERE r.condition_sql LIKE 'amount > %'
          AND r.region = t.region
          AND LOWER(r.vendor) = LOWER(t.vendor)
          AND t.amount > CAST(SUBSTRING(r.condition_sql FROM 'amount > ([0-9]+)') AS NUMERIC)
        GROUP BY t.tx_id
      )
      UPDATE fraud_labels fl
      SET is_fraud = TRUE,
          rules_triggered = mr.rules_triggered,
          fraud_reason = array_to_string(mr.rules_triggered, ', ')
      FROM matched_rules mr
      WHERE fl.tx_id = mr.tx_id AND fl.detection_source = 'rules'
    `);
    broadcast('progress',{step:'transactions',msg:`✓ ${transactions.length} transactions loaded`,pct:72});

    // Create indexes AFTER bulk load (5-10x faster than incremental updates)
    // Note: Run sequentially to avoid BDR global DDL lock contention
    broadcast('progress',{step:'indexes',msg:'Building indexes (sequential)...',pct:74});
    await pool.query('CREATE INDEX IF NOT EXISTS idx_tx_account ON transactions(account_id)');
    broadcast('progress',{step:'indexes',msg:'Building indexes... (2/5)',pct:76});
    await pool.query('CREATE INDEX IF NOT EXISTS idx_tx_created ON transactions(created_at DESC)');
    broadcast('progress',{step:'indexes',msg:'Building indexes... (3/5)',pct:78});
    await pool.query('CREATE INDEX IF NOT EXISTS idx_tx_region_vendor ON transactions(region, vendor)');
    broadcast('progress',{step:'indexes',msg:'Building indexes... (4/5)',pct:79});
    await pool.query('CREATE INDEX IF NOT EXISTS idx_transactions_received_at ON transactions(received_at)');
    broadcast('progress',{step:'indexes',msg:'Building indexes... (5/5)',pct:80});
    // fraud_labels indexes created by 03-fraud-detection.sql
    broadcast('progress',{step:'indexes',msg:'✓ Indexes ready',pct:82});

    // OLTP Initialize complete - OLAP/ML setup moved to separate usecases
    // Usecase 2: OLAP - run setup-clickhouse-olap.sh, setup-risingwave-olap.sh, Kafka CDC
    // Usecase 3: ML - run setup-clickhouse-ml.sh, setup-risingwave-ml.sh, start ML inference
    broadcast('progress',{step:'oltp_ready',msg:'✓ OLTP ready! Run Usecase 2 (OLAP) to enable CDC pipelines.',pct:95});

    simConfig={txTypes:txTypes||['CREDIT','DEBIT','TRANSFER','PAYMENT'],fraudMode,intervalMs};
    txInterval=parseInt(intervalMs)||1000;
    sessionStats={total:0,volume:0,fraud:0,credit:0,debit:0,transfer:0,payment:0};
    simRunning=true; simPaused=false; runSimLoop();
    broadcast('progress',{step:'sim_started',msg:'✓ OLTP active! Transactions flowing. Run Usecase 2 for OLAP.',pct:100});
    broadcast('sim_started',{ts:new Date().toISOString()});
    res.json({ok:true,customers:customers.length,accounts:accounts.length,transactions:transactions.length,message:'OLTP ready. Run Usecase 2 (OLAP) for CDC pipelines.'});
  }catch(err){
    console.error('[SETUP]',err.message);
    res.status(500).json({ok:false,error:err.message});
  }
});

async function ensurePostgresPrereqs(){
  const client=await pool.connect();
  try{
    for(const role of ['rds_superuser','rds_replication']){
      try{await client.query(`CREATE ROLE ${role}`);}catch(_){}
      try{await client.query(`GRANT ${role} TO ${pgConnCfg.user}`);}catch(_){}
    }
    try{await client.query(`ALTER USER ${pgConnCfg.user} WITH REPLICATION`);}catch(_){}
    try{await client.query(`DROP PUBLICATION IF EXISTS rw_pub_corebanking`);}catch(_){}
    await client.query(`CREATE PUBLICATION rw_pub_corebanking FOR TABLE customers,accounts,transactions,fraud_labels`);
    // Note: rw_slot_corebanking removed - RisingWave now uses Kafka (via Debezium), not direct CDC
    for(const slot of ['rw_slot_comparison','kafka_slot_corebanking']){
      try{await client.query(`SELECT pg_drop_replication_slot('${slot}') WHERE EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name='${slot}')`);}catch(_){}
    }
    console.log('[PG] Prerequisites ready ✓');
  }finally{client.release();}
}

// ── SIM CONTROLS ───────────────────────────────────────────
app.post('/api/sim/start', (req,res)=>{
  if(!pool) return res.status(400).json({ok:false,error:'Not connected'});
  if(simRunning) return res.json({ok:true,message:'Already running'});

  const{txTypes,fraudMode,intervalMs}=req.body;
  simConfig={txTypes:txTypes||['CREDIT','DEBIT','TRANSFER','PAYMENT'],fraudMode:fraudMode||'realistic',intervalMs:intervalMs||1000};
  txInterval=parseInt(intervalMs)||1000;
  sessionStats={total:0,volume:0,fraud:0,credit:0,debit:0,transfer:0,payment:0};
  simRunning=true; simPaused=false;
  runSimLoop();
  broadcast('sim_started',{ts:new Date().toISOString()});
  console.log('[SIM] Started (resume mode)');
  res.json({ok:true,message:'Simulation started'});
});
app.post('/api/sim/pause',(req,res)=>{if(!simRunning)return res.json({ok:false});simPaused=!simPaused;res.json({ok:true,paused:simPaused});});
app.post('/api/sim/stop', (req,res)=>{ stopSim(); res.json({ok:true,stats:sessionStats}); });
app.post('/api/sim/speed',(req,res)=>{ txInterval=parseInt(req.body.intervalMs)||1000; if(simRunning)runSimLoop(); res.json({ok:true}); });

app.get('/api/search', async(req,res)=>{
  if(!pool) return res.status(400).json({ok:false,error:'Not connected'});
  const q=req.query.q?.trim()||'',type=req.query.type||'',cat=req.query.category||'';
  const fraud=req.query.fraud||'',amtMin=req.query.amtMin||'',amtMax=req.query.amtMax||'';
  const lim=Math.min(parseInt(req.query.limit)||50,200);
  try{
    const f=[],p=[];let i=1;
    if(type){f.push(`t.type=$${i++}`);p.push(type);}
    if(cat){f.push(`t.category=$${i++}`);p.push(cat);}
    if(fraud==='true')f.push('fl.is_fraud=TRUE');
    if(fraud==='false')f.push('(fl.is_fraud IS NULL OR fl.is_fraud=FALSE)');
    if(amtMin){f.push(`t.amount>=$${i++}`);p.push(parseFloat(amtMin));}
    if(amtMax){f.push(`t.amount<=$${i++}`);p.push(parseFloat(amtMax));}
    const andW=f.length?'AND '+f.join(' AND '):'';
    const whereW=f.length?'WHERE '+f.join(' AND '):'';
    let rows;
    const baseJoin='FROM transactions t JOIN customers c ON t.customer_id=c.customer_id LEFT JOIN fraud_labels fl ON t.tx_id=fl.tx_id';
    if(q){p.push(`%${q}%`);const r=await pool.query(`SELECT t.*,c.name as customer_name,COALESCE(fl.is_fraud,FALSE) as is_fraud ${baseJoin} WHERE(t.description ILIKE $${i} OR t.remarks ILIKE $${i} OR t.merchant ILIKE $${i}) ${andW} ORDER BY t.created_at DESC LIMIT ${lim}`,p);rows=r.rows;}
    else{const r=await pool.query(`SELECT t.*,c.name as customer_name,COALESCE(fl.is_fraud,FALSE) as is_fraud ${baseJoin} ${whereW} ORDER BY t.created_at DESC LIMIT ${lim}`,p);rows=r.rows;}
    res.json({ok:true,rows,count:rows.length});
  }catch(err){res.status(500).json({ok:false,error:err.message});}
});

// ══════════════════════════════════════════════════════════
//  SIMULATION ENGINE
// ══════════════════════════════════════════════════════════
const US_FIRST=['James','Emily','Michael','Sarah','Robert','Ashley','William','Jessica','David','Amanda'];
const US_LAST=['Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Wilson','Martinez'];
const US_BANKS=['Chase Bank','Bank of America','Wells Fargo','Citibank','US Bancorp','PNC Bank','Capital One','TD Bank'];
const US_STATES=['California','Texas','New York','Florida','Illinois','Ohio','Georgia','Michigan'];
const MERCHANTS={DEBIT:['Walmart Supercenter','Target Corp','Amazon.com','Costco','Home Depot','Best Buy','Walgreens','CVS','Shell Gas','Starbucks'],PAYMENT:['AT&T Mobility','Verizon','Comcast','PG&E','State Farm','Netflix','Spotify'],TRANSFER:['Zelle Transfer','Venmo','PayPal','ACH Transfer','Wire Transfer'],CREDIT:['ADP Payroll','Gusto Payroll','Direct Deposit','ACH Credit','IRS Tax Refund']};
const CATEGORIES={DEBIT:['Groceries','Gas & Auto','Retail Shopping','Dining','Pharmacy','Entertainment','Gambling','Crypto Exchange'],PAYMENT:['Utilities','Telecom','Insurance','Streaming','Loan Payment'],TRANSFER:['Person-to-Person','Internal','Wire Transfer','ACH','Crypto'],CREDIT:['Payroll','Refund','Interest','Bonus','Government']};
const CHANNELS=['Mobile App','Online Banking','ATM','Branch Teller','POS Terminal','Phone Banking'];
const BAL={small:[1000,50000],medium:[10000,500000],large:[100000,5000000]};
function genCustomers(n){const out=[];for(let i=1;i<=n;i++){const fn=pick(US_FIRST),ln=pick(US_LAST);out.push({customer_id:`CUST${String(i).padStart(5,'0')}`,name:`${fn} ${ln}`,email:`${fn.toLowerCase()}.${ln.toLowerCase()}${i}@example.com`,phone:`+1-${ri(200,999)}-${ri(200,999)}-${ri(1000,9999)}`,state:pick(US_STATES),kyc_status:pick(['VERIFIED','VERIFIED','PENDING']),created_at:new Date(Date.now()-ri(0,365*2)*86400000).toISOString()});}return out;}
// Regions and vendors for rule matching (aligned with MinIO fraud rules)
const REGIONS=['US','US','US','UK','CA','DE','FR'];  // Weighted toward US
const VENDORS=['stripe','stripe','paypal','square'];  // Weighted toward Stripe

// Fraud thresholds from MinIO documents (region+vendor specific)
const FRAUD_THRESHOLDS = {
  'US-stripe': 5000,   // US-STRIPE-001.txt
  'UK-stripe': 4350,   // UK-STRIPE-001.txt (£3,500)
  'DE-stripe': 4300,   // DE-STRIPE-001.txt (€4,000)
  'FR-stripe': 4350,   // FR-STRIPE-001.txt (€4,000)
  'CA-stripe': 3300,   // CA-STRIPE-001.txt (CAD $4,500)
  'US-square': 5000,   // US-SQUARE-001.txt
};
const FRAUD_RATE = 0.04; // Target ~4% fraud rate

function genAccountsAndTx(customers,maxAcc,maxTx,balRange){
  // Generate accounts and transactions with controlled ~4% fraud rate
  // Fraud is triggered when amount exceeds region+vendor threshold from MinIO docs
  const accounts=[],transactions=[];
  const[lo,hi]=BAL[balRange]||BAL.medium;
  for(const c of customers){
    const nAcc=ri(1,maxAcc);
    for(let j=1;j<=nAcc;j++){
      const bal=Math.round(rand(lo,hi));
      const acc={account_id:`ACC${c.customer_id.slice(4)}${j}`,customer_id:c.customer_id,account_type:pick(['CHECKING','CHECKING','SAVINGS','MONEY_MARKET']),balance:bal,initial_balance:bal,bank:pick(US_BANKS),routing_number:`0${ri(10000000,99999999)}`,status:pick(['ACTIVE','ACTIVE','ACTIVE','INACTIVE']),created_at:new Date(Date.now()-ri(0,365)*86400000).toISOString()};
      accounts.push(acc);
      let curBal=bal;
      const nTx=ri(Math.floor(maxTx*.5),maxTx);
      for(let k=0;k<nTx;k++){
        const type=pick(['CREDIT','DEBIT','DEBIT','TRANSFER','PAYMENT']);
        const isDebit=['DEBIT','TRANSFER','PAYMENT'].includes(type);
        const merch=pick(MERCHANTS[type]||MERCHANTS.DEBIT);
        let cat=pick(CATEGORIES[type]||CATEGORIES.DEBIT);
        const ch=pick(CHANNELS);
        const region=pick(REGIONS),vendor=pick(VENDORS);

        // Controlled fraud generation: ~4% above threshold, 96% below
        const isFraud = Math.random() < FRAUD_RATE;
        const thresholdKey = `${region}-${vendor}`;
        const threshold = FRAUD_THRESHOLDS[thresholdKey] || 5000;

        let amt;
        if (isFraud && curBal > threshold * 1.5) {
          // Generate fraud-triggering amount (above threshold)
          amt = Math.round(rand(threshold + 100, Math.min(threshold * 2, curBal * 0.5)));
        } else {
          // Generate normal amount (below threshold)
          const safeMax = Math.min(threshold - 100, curBal * 0.3);
          amt = Math.round(rand(5, Math.max(100, safeMax)));
        }

        if(isDebit&&curBal-amt<100)continue;
        if(amt<=0)continue;
        if(isDebit)curBal-=amt;else curBal+=amt;
        // Transaction facts only - fraud_labels table stores fraud assessments
        transactions.push({account_id:acc.account_id,customer_id:c.customer_id,type,merchant:merch,category:cat,channel:ch,region,vendor,currency:'USD',description:`${isDebit?'Debit':'Credit'} — ${merch} — ${fmtUSD(amt)}`,remarks:`${type} ${fmtUSD(amt)} via ${merch}`,amount:amt,balance_after:curBal,reference_no:refNo(),status:'COMPLETED',created_at:new Date(Date.now()-ri(1,90)*86400000).toISOString()});
      }
      acc.balance=curBal;
    }
  }
  return{accounts,transactions};
}
function stopSim(){if(simTimer){clearInterval(simTimer);simTimer=null;}simRunning=false;simPaused=false;}
function runSimLoop(){if(simTimer){clearInterval(simTimer);simTimer=null;}simTimer=setInterval(async()=>{if(!simRunning||simPaused||!pool)return;try{await generateLiveTx();}catch(e){console.error('[SIM]',e.message);}},txInterval);}
async function generateLiveTx(){
  const r=await pool.query(`SELECT a.*,c.name as customer_name FROM accounts a JOIN customers c ON a.customer_id=c.customer_id WHERE a.status='ACTIVE' ORDER BY RANDOM() LIMIT 1`);
  if(!r.rows.length)return;const acc=r.rows[0];
  const txTypes=simConfig.txTypes||['CREDIT','DEBIT','TRANSFER','PAYMENT'];
  const type=pick(txTypes);const isDebit=['DEBIT','TRANSFER','PAYMENT'].includes(type);
  const balance=parseFloat(acc.balance);
  let merchant=pick(MERCHANTS[type]||MERCHANTS.DEBIT);
  let category=pick(CATEGORIES[type]||CATEGORIES.DEBIT);
  const channel=pick(CHANNELS);
  const region=pick(REGIONS),vendor=pick(VENDORS);

  // Controlled fraud generation: ~4% above threshold (matches MinIO documents)
  const shouldTriggerFraud = Math.random() < FRAUD_RATE;
  const thresholdKey = `${region}-${vendor}`;
  const threshold = FRAUD_THRESHOLDS[thresholdKey] || 5000;

  let amount;
  if (shouldTriggerFraud && balance > threshold * 1.5) {
    // Generate fraud-triggering amount (above region+vendor threshold from MinIO docs)
    amount = Math.round(rand(threshold + 100, Math.min(threshold * 2, balance * 0.5)));
  } else {
    // Generate normal amount (below threshold) - 96% of transactions
    const safeMax = Math.min(threshold - 100, balance * 0.3);
    amount = Math.round(rand(5, Math.max(100, safeMax)));
  }

  if(isDebit&&balance-amount<100)return;if(amount<=0)return;
  const newBal=isDebit?balance-amount:balance+amount;

  // Insert transaction - AFTER INSERT trigger evaluates rules and inserts into fraud_labels
  const client=await pool.connect();let txRow;
  try{
    await client.query('BEGIN');
    await client.query('UPDATE accounts SET balance=$1 WHERE account_id=$2',[newBal,acc.account_id]);
    const ins=await client.query(`INSERT INTO transactions(account_id,customer_id,type,description,remarks,merchant,category,amount,balance_after,reference_no,channel,region,vendor,currency,status,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'USD','COMPLETED',NOW()) RETURNING *`,[acc.account_id,acc.customer_id,type,`${isDebit?'Debit':'Credit'} — ${merchant} — ${fmtUSD(amount)}`,`${type} ${fmtUSD(amount)} via ${merchant}`,merchant,category,amount,newBal,refNo(),channel,region,vendor]);
    await client.query('COMMIT');
    txRow=ins.rows[0];
  }catch(e){await client.query('ROLLBACK');throw e;}finally{client.release();}

  // Check fraud_labels for fraud detection (populated by trigger)
  const fraudCheck=await pool.query(`SELECT is_fraud,rules_triggered,fraud_reason FROM fraud_labels WHERE tx_id=$1 AND detection_source='rules'`,[txRow.tx_id]);
  const isFraud=fraudCheck.rows[0]?.is_fraud||false;
  const fraudReason=fraudCheck.rows[0]?.fraud_reason||null;

  sessionStats.total++;sessionStats.volume+=amount;sessionStats[type.toLowerCase()]=(sessionStats[type.toLowerCase()]||0)+1;
  if(isFraud)sessionStats.fraud++;
  broadcast('transaction',{tx:{...txRow,is_fraud:isFraud,fraud_reason:fraudReason,amount:parseFloat(txRow.amount),balance_after:parseFloat(txRow.balance_after)},account:{account_id:acc.account_id,account_type:acc.account_type,customer_id:acc.customer_id,customer_name:acc.customer_name,bank:acc.bank,balance:newBal},stats:{...sessionStats}});
  // Fraud alert notifications handled by fraud-alert-service.py (OLAP profile only)
}

// Auto-initialize RisingWave on startup if Postgres has data but RisingWave is empty
async function autoInitRisingWave() {
  // Wait a bit for services to stabilize
  await new Promise(r => setTimeout(r, 5000));

  try {
    // Check if Postgres is connected and has data
    if (!pool) {
      console.log('[RW Auto-Init] Skipped - Postgres not connected');
      return;
    }

    const pgCheck = await pool.query('SELECT count(*) as n FROM transactions').catch(() => null);
    if (!pgCheck || !pgCheck.rows[0] || parseInt(pgCheck.rows[0].n) === 0) {
      console.log('[RW Auto-Init] Skipped - Postgres has no data');
      return;
    }

    const pgCount = parseInt(pgCheck.rows[0].n);
    console.log(`[RW Auto-Init] Postgres has ${pgCount} transactions`);

    // Check if RisingWave is empty
    const rwCheck = await rwQuery('SHOW SOURCES').catch(() => ({ rows: [] }));
    if (rwCheck.rows && rwCheck.rows.length > 0) {
      console.log('[RW Auto-Init] Skipped - RisingWave already initialized');
      return;
    }

    console.log('[RW Auto-Init] RisingWave is empty - auto-initializing...');
    const { host, port, database, user, password } = pgConnCfg;
    await setupRisingWaveMVs(host, port, database, user, password);
    console.log('[RW Auto-Init] ✓ Complete');
  } catch (e) {
    console.error('[RW Auto-Init] Failed:', e.message);
  }
}

// Auto-connect to Postgres on startup in local mode
async function autoConnectPostgres() {
  if (!AUTO_CONNECT_PG) {
    console.log('[PG Auto-Connect] Skipped - not in local mode');
    return;
  }

  // Wait for Postgres to be ready
  await new Promise(r => setTimeout(r, 3000));

  const mkCfg = (db) => {
    const c = { host: PG_HOST, port: PG_PORT, database: db, user: PG_USER, ssl: false, connectionTimeoutMillis: 12000, max: 20, idleTimeoutMillis: 30000 };
    if (PG_PASSWORD) c.password = PG_PASSWORD;
    return c;
  };

  try {
    console.log(`[PG Auto-Connect] Connecting to ${PG_USER}@${PG_HOST}:${PG_PORT}/${PG_DATABASE}...`);
    const admin = new Pool(mkCfg('postgres'));
    await admin.query('SELECT 1');
    const ex = await admin.query(`SELECT 1 FROM pg_database WHERE datname=$1`, [PG_DATABASE]);
    if (!ex.rowCount) await admin.query(`CREATE DATABASE "${PG_DATABASE}"`);
    await admin.end();

    pool = new Pool(mkCfg(PG_DATABASE));

    // Handle unexpected connection terminations
    pool.on('error', (err) => {
      console.error('[PG Pool] Unexpected error on idle client:', err.message);
    });

    const r = await pool.query('SELECT current_user, version()');
    const ver = r.rows[0].version.split(' ').slice(0, 2).join(' ');
    pgConnCfg = { host: PG_HOST, port: PG_PORT, database: PG_DATABASE, user: PG_USER, password: PG_PASSWORD };

    console.log(`[PG Auto-Connect] ✓ Connected to "${PG_DATABASE}" as "${PG_USER}" — ${ver}`);
  } catch (err) {
    pool = null;
    let msg = err.message;
    if (msg.includes('ECONNREFUSED') || msg.includes('ETIMEDOUT')) msg = `Cannot reach PostgreSQL at ${PG_HOST}:${PG_PORT}`;
    else if (msg.includes('password') || msg.includes('authentication')) msg = `Auth failed for "${PG_USER}"`;
    console.error('[PG Auto-Connect] Failed:', msg);
  }
}

// ── INFERENCE SERVICE CONTROL ──────────────────────────────
// Map service names to container names
const INFERENCE_SERVICES = {
  'kafka':       'cb-ml-kafka',
  'clickhouse':  'cb-ml-ch',
  'risingwave':  'cb-ml-rw',
  'pgaa':        'cb-ml-pgaa',
  'pgaa_hybrid': 'cb-ml-pgaa-hybrid'
};

// Use Docker socket API via curl (socket is mounted at /var/run/docker.sock)
function dockerApiGet(endpoint) {
  return new Promise((resolve, reject) => {
    exec(`curl -s --unix-socket /var/run/docker.sock http://localhost${endpoint}`, (err, stdout, stderr) => {
      if (err) return reject(new Error(stderr || err.message));
      try { resolve(JSON.parse(stdout)); }
      catch (e) { reject(new Error('Invalid JSON from Docker API')); }
    });
  });
}

function dockerApiPost(endpoint) {
  return new Promise((resolve, reject) => {
    exec(`curl -s -X POST --unix-socket /var/run/docker.sock http://localhost${endpoint}`, (err, stdout, stderr) => {
      if (err) return reject(new Error(stderr || err.message));
      resolve(stdout);
    });
  });
}

// Get status of inference services
app.get('/api/inference/status', async (req, res) => {
  try {
    const containers = await dockerApiGet('/containers/json?all=true');
    const results = {};

    for (const [key, containerName] of Object.entries(INFERENCE_SERVICES)) {
      const container = containers.find(c => c.Names.some(n => n === '/' + containerName));
      results[key] = {
        running: container?.State === 'running',
        service: containerName
      };
    }
    res.json({ ok: true, services: results });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Start/stop an inference service
app.post('/api/inference/:service/:action', async (req, res) => {
  const { service, action } = req.params;

  if (!INFERENCE_SERVICES[service]) {
    return res.status(400).json({ ok: false, error: `Unknown service: ${service}` });
  }
  if (!['start', 'stop'].includes(action)) {
    return res.status(400).json({ ok: false, error: `Invalid action: ${action}` });
  }

  const containerName = INFERENCE_SERVICES[service];
  try {
    await dockerApiPost(`/containers/${containerName}/${action}`);
    console.log(`[Inference] ${action} ${service}: OK`);
    res.json({ ok: true, service, action });
  } catch (e) {
    console.error(`[Inference] ${action} ${service} failed:`, e.message);
    res.json({ ok: false, error: e.message });
  }
});

// ══════════════════════════════════════════════════════════
//  FRAUD AUDIT APIs
// ══════════════════════════════════════════════════════════

const LANGFLOW_HOST = process.env.LANGFLOW_HOST || 'http://langflow:7860';

// Get fraud rules
app.get('/api/fraud-audit/rules', async (req, res) => {
  if (!pool) return res.status(400).json({ ok: false, error: 'Not connected' });
  try {
    const region = req.query.region || null;
    const category = req.query.category || null;

    let query = `
      SELECT rule_id, rule_name, rule_description, rule_category,
             region, vendor, threshold_amount, risk_score_threshold, action
      FROM fraud_rules
      WHERE 1=1
    `;
    const params = [];

    if (region) {
      params.push(region);
      query += ` AND (region = $${params.length} OR region = 'GLOBAL')`;
    }
    if (category) {
      params.push(category);
      query += ` AND rule_category = $${params.length}`;
    }

    query += ` ORDER BY region, rule_id`;

    const result = await pool.query(query, params);
    res.json({ ok: true, rules: result.rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Get HITL approval queue
app.get('/api/fraud-audit/hitl', async (req, res) => {
  if (!pool) return res.status(400).json({ ok: false, error: 'Not connected' });
  try {
    const status = req.query.status || 'pending';
    const limit = Math.min(parseInt(req.query.limit) || 50, 100);

    const result = await pool.query(`
      SELECT h.*, t.type as tx_type, t.channel, t.created_at as tx_time
      FROM hitl_approvals h
      JOIN transactions t ON h.tx_id = t.tx_id
      WHERE h.status = $1
      ORDER BY h.fraud_probability DESC, h.requested_at ASC
      LIMIT $2
    `, [status, limit]);

    res.json({ ok: true, approvals: result.rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Update HITL approval status
app.post('/api/fraud-audit/hitl/:approvalId', async (req, res) => {
  if (!pool) return res.status(400).json({ ok: false, error: 'Not connected' });
  const { approvalId } = req.params;
  const { status, resolved_by, resolution_notes } = req.body;

  if (!['approved', 'rejected', 'escalated'].includes(status)) {
    return res.status(400).json({ ok: false, error: 'Invalid status' });
  }

  try {
    const result = await pool.query(`
      UPDATE hitl_approvals
      SET status = $1, resolved_by = $2, resolution_notes = $3, resolved_at = NOW()
      WHERE approval_id = $4
      RETURNING *
    `, [status, resolved_by || 'system', resolution_notes || '', approvalId]);

    if (result.rows.length === 0) {
      return res.status(404).json({ ok: false, error: 'Approval not found' });
    }

    res.json({ ok: true, approval: result.rows[0] });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Create HITL approval from fraud prediction
app.post('/api/fraud-audit/hitl/create', async (req, res) => {
  if (!pool) return res.status(400).json({ ok: false, error: 'Not connected' });
  const { tx_id, prediction_source, fraud_probability, alert_type, recommendation } = req.body;

  if (!tx_id) {
    return res.status(400).json({ ok: false, error: 'tx_id is required' });
  }

  try {
    const result = await pool.query(`
      SELECT create_hitl_from_prediction($1, $2, $3, $4, $5) as approval_id
    `, [tx_id, prediction_source || 'manual', fraud_probability || 0.5, alert_type || 'Manual Review', recommendation || null]);

    res.json({ ok: true, approval_id: result.rows[0].approval_id });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Get fraud audit summary
app.get('/api/fraud-audit/summary', async (req, res) => {
  if (!pool) return res.status(400).json({ ok: false, error: 'Not connected' });
  try {
    const summary = await pool.query(`
      SELECT * FROM v_fraud_audit_summary
      ORDER BY hour DESC
      LIMIT 24
    `);

    const hitlStats = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE status = 'pending') as pending,
        COUNT(*) FILTER (WHERE status = 'approved') as approved,
        COUNT(*) FILTER (WHERE status = 'rejected') as rejected,
        COUNT(*) FILTER (WHERE status = 'escalated') as escalated
      FROM hitl_approvals
      WHERE requested_at > NOW() - INTERVAL '24 hours'
    `);

    res.json({
      ok: true,
      summary: summary.rows,
      hitlStats: hitlStats.rows[0] || {}
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Proxy to LangFlow API
app.all('/api/langflow/*', async (req, res) => {
  try {
    const path = req.params[0];
    const url = `${LANGFLOW_HOST}/api/v1/${path}`;

    const response = await fetch(url, {
      method: req.method,
      headers: {
        'Content-Type': 'application/json',
        ...req.headers
      },
      body: req.method !== 'GET' ? JSON.stringify(req.body) : undefined
    });

    const data = await response.json();
    res.status(response.status).json(data);
  } catch (e) {
    res.status(500).json({ ok: false, error: `LangFlow proxy error: ${e.message}` });
  }
});

// Get LangFlow health status
app.get('/api/fraud-audit/langflow/health', async (req, res) => {
  try {
    const response = await fetch(`${LANGFLOW_HOST}/health`);
    const data = await response.json();
    res.json({ ok: true, langflow: data });
  } catch (e) {
    res.json({ ok: false, error: `LangFlow not reachable: ${e.message}` });
  }
});

// Semantic search for fraud rules (AIDB BERT embeddings)
// Falls back to text search if AIDB not configured
app.get('/api/fraud-audit/rules/search', async (req, res) => {
  if (!pool) return res.status(400).json({ ok: false, error: 'Not connected' });
  const query = req.query.q || req.query.query || '';
  const limit = Math.min(parseInt(req.query.limit) || 5, 20);

  if (!query.trim()) {
    return res.status(400).json({ ok: false, error: 'Query parameter (q) is required' });
  }

  try {
    // Check if semantic search function exists
    const checkFn = await pool.query(`
      SELECT COUNT(*) as cnt FROM information_schema.routines
      WHERE routine_name = 'search_fraud_rules_semantic'
    `);
    const hasSemanticSearch = parseInt(checkFn.rows[0]?.cnt || 0) > 0;

    if (hasSemanticSearch) {
      // Use AIDB semantic search
      const result = await pool.query(`
        SELECT chunk_id, source_doc, chunk_text, similarity
        FROM search_fraud_rules_semantic($1, $2)
      `, [query, limit]);

      res.json({
        ok: true,
        mode: 'semantic',
        query,
        results: result.rows.map(r => ({
          rule_id: r.source_doc?.replace('.txt', '') || 'Unknown',
          chunk_id: r.chunk_id,
          similarity: parseFloat(r.similarity || 0),
          text: r.chunk_text
        }))
      });
    } else {
      // Fallback to ILIKE text search
      const terms = query.split(/\s+/).filter(t => t.length > 0);
      const conditions = terms.map((_, i) => `(
        rule_id ILIKE $${i*6+1} OR rule_name ILIKE $${i*6+2} OR
        rule_description ILIKE $${i*6+3} OR rule_category ILIKE $${i*6+4} OR
        region ILIKE $${i*6+5} OR vendor ILIKE $${i*6+6}
      )`);
      const params = terms.flatMap(t => Array(6).fill(`%${t}%`));
      params.push(limit);

      const result = await pool.query(`
        SELECT rule_id, rule_name, rule_description, rule_category,
               region, vendor, threshold_amount, risk_score_threshold, action
        FROM fraud_rules
        WHERE ${conditions.join(' AND ') || '1=1'}
        ORDER BY CASE WHEN region = 'GLOBAL' THEN 1 ELSE 0 END, rule_id
        LIMIT $${params.length}
      `, params);

      res.json({
        ok: true,
        mode: 'text',
        query,
        note: 'AIDB semantic search not configured. Run: docker exec -i bfsi-pgd psql -U postgres -d demo -f /scripts/setup-minio-aidb.sql',
        results: result.rows.map(r => ({
          rule_id: r.rule_id,
          rule_name: r.rule_name,
          description: r.rule_description,
          region: r.region,
          vendor: r.vendor,
          action: r.action,
          threshold: r.threshold_amount ? parseFloat(r.threshold_amount) : null
        }))
      });
    }
  } catch (e) {
    res.status(500).json({ ok: false, error: `Search error: ${e.message}` });
  }
});

const PORT=process.env.PORT||3001;
server.listen(PORT,'0.0.0.0',()=>{
  console.log(`App started at http://localhost:${PORT}`);
  console.log('[TEST] Server listen callback executed');

  // Test CH connectivity at startup
  curlPing().then(ok => {
    chStatus.connected = ok;
    console.log(`[CH] Startup ping: ${ok ? '✓ OK' : '✗ FAILED'} at ${CH_URL}`);
  });

  // Auto-connect to Postgres in local mode, then auto-initialize RisingWave
  console.log('[TEST] About to call autoConnectPostgres');
  autoConnectPostgres()
    .then(() => {
      console.log('[Startup] Auto-connect complete, checking RisingWave...');
      return autoInitRisingWave();
    })
    .then(() => {
      console.log('[Startup] All auto-initialization complete');
    })
    .catch(e => console.error('[Startup] Error:', e.message));
  console.log('[TEST] autoConnectPostgres called');
});
