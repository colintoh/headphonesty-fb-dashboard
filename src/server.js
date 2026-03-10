const express = require('express');
const Database = require('better-sqlite3');
const compression = require('compression');
const path = require('path');
const fs = require('fs');

const app = express();
app.use(compression());

const PORT = process.env.PORT || 3000;
const DB_PATH = process.env.DB_PATH || path.join(__dirname, '..', 'data', 'posts.db');

const CLICKY_SITE_ID = '101226983';
const CLICKY_SITE_KEY = 'f74e08aa55d8b412';
const CLICKY_BASE = 'https://api.clicky.com/api/stats/4';

const LABBY_URL = 'https://labby.headphonesty.com/v1/facebook/reach';
const LABBY_TOKEN = 'prod_cky_4f8e2b7c9a1d6e3f5b2c7a8e9d1f4b6c';

// Weekly targets (derived from monthly)
const MONTHLY_TARGETS = { visitors: 1500000, fb_reach: 16000000, fb_clicks: 200000 };
const WEEKLY_TARGETS = {
  visitors: Math.round(MONTHLY_TARGETS.visitors / 4.33),
  fb_reach: Math.round(MONTHLY_TARGETS.fb_reach / 4.33),
  fb_clicks: Math.round(MONTHLY_TARGETS.fb_clicks / 4.33),
  articles: 15
};

function getDb() {
  if (!fs.existsSync(DB_PATH)) return null;
  return new Database(DB_PATH, { readonly: true });
}

// All date logic in SGT (UTC+8)
function nowSGT() {
  const utc = new Date();
  return new Date(utc.getTime() + 8 * 60 * 60 * 1000);
}

function fmtDate(d) {
  return d.toISOString().slice(0, 10);
}

function getWeekBounds(weeksAgo = 0) {
  const now = nowSGT();
  const day = now.getUTCDay(); // 0=Sun (use UTC methods since we already offset)
  const diffToMon = day === 0 ? 6 : day - 1;
  const mon = new Date(now);
  mon.setUTCDate(mon.getUTCDate() - diffToMon - (weeksAgo * 7));
  mon.setUTCHours(0, 0, 0, 0);
  const sun = new Date(mon);
  sun.setUTCDate(sun.getUTCDate() + 6);
  const dayOfWeek = diffToMon + 1; // 1=Mon ... 7=Sun
  return {
    start: fmtDate(mon),
    end: fmtDate(sun),
    dayOfWeek
  };
}

function getMonthBounds() {
  const now = nowSGT();
  const y = now.getUTCFullYear();
  const m = now.getUTCMonth(); // 0-indexed
  const start = `${y}-${String(m + 1).padStart(2, '0')}-01`;
  const end = fmtDate(now);
  const dayOfMonth = now.getUTCDate();
  const daysInMonth = new Date(Date.UTC(y, m + 1, 0)).getUTCDate();
  return { start, end, dayOfMonth, daysInMonth };
}

async function clickyFetch(type, opts = {}) {
  const params = new URLSearchParams({
    site_id: CLICKY_SITE_ID, sitekey: CLICKY_SITE_KEY, type, output: 'json', ...opts
  });
  const r = await fetch(`${CLICKY_BASE}?${params}`);
  return r.json();
}

// ===== REAL-TIME =====
app.get('/api/realtime', async (req, res) => {
  try {
    const data = await clickyFetch('visitors-online', { _: Date.now() });
    const count = parseInt(data[0]?.dates?.[0]?.items?.[0]?.value || '0');
    res.json({ visitors: count, timestamp: new Date().toISOString() });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ===== SCORECARDS (weekly or monthly) =====
app.get('/api/scorecards', async (req, res) => {
  const period = req.query.period || 'weekly'; // weekly | monthly
  try {
    let visitorRange, fbRange, targetSet, daysElapsed, daysTotal;

    if (period === 'monthly') {
      const mb = getMonthBounds();
      visitorRange = `${mb.start},${mb.end}`;
      fbRange = { start: mb.start, end: mb.end };
      targetSet = MONTHLY_TARGETS;
      daysElapsed = mb.dayOfMonth;
      daysTotal = mb.daysInMonth;
    } else {
      const wb = getWeekBounds();
      visitorRange = `${wb.start},${wb.end}`;
      fbRange = { start: wb.start, end: wb.end };
      targetSet = WEEKLY_TARGETS;
      daysElapsed = wb.dayOfWeek;
      daysTotal = 7;
    }

    // Fetch visitors
    const vData = await clickyFetch('visitors', { date: visitorRange });
    const visitors = parseInt(vData[0]?.dates?.[0]?.items?.[0]?.value || '0');
    const visitorsProjected = Math.round(visitors / daysElapsed * daysTotal);

    // Fetch FB data
    const form = new FormData();
    form.append('startDate', fbRange.start);
    form.append('endDate', fbRange.end);
    const fbResp = await fetch(LABBY_URL, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${LABBY_TOKEN}` },
      body: form
    });
    const fbData = (await fbResp.json())?.data || {};
    const fbReach = fbData.totalReach || 0;
    const fbClicks = fbData.totalFacebookLinkClicks || 0;
    const fbReachProjected = Math.round(fbReach / daysElapsed * daysTotal);
    const fbClicksProjected = Math.round(fbClicks / daysElapsed * daysTotal);

    // Fetch prior period for comparison
    let priorVisitorRange, priorFbRange;
    if (period === 'monthly') {
      const pm = new Date();
      pm.setMonth(pm.getMonth() - 1);
      const pStart = `${pm.getFullYear()}-${String(pm.getMonth() + 1).padStart(2, '0')}-01`;
      const pEnd = new Date(pm.getFullYear(), pm.getMonth() + 1, 0).toISOString().slice(0, 10);
      priorVisitorRange = `${pStart},${pEnd}`;
      priorFbRange = { start: pStart, end: pEnd };
    } else {
      const pw = getWeekBounds(1);
      priorVisitorRange = `${pw.start},${pw.end}`;
      priorFbRange = { start: pw.start, end: pw.end };
    }

    const pvData = await clickyFetch('visitors', { date: priorVisitorRange });
    const priorVisitors = parseInt(pvData[0]?.dates?.[0]?.items?.[0]?.value || '0');

    const pfForm = new FormData();
    pfForm.append('startDate', priorFbRange.start);
    pfForm.append('endDate', priorFbRange.end);
    const pfResp = await fetch(LABBY_URL, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${LABBY_TOKEN}` },
      body: pfForm
    });
    const pfData = (await pfResp.json())?.data || {};
    const priorReach = pfData.totalReach || 0;
    const priorClicks = pfData.totalFacebookLinkClicks || 0;

    // WP articles this week
    const wb = getWeekBounds();
    const wpResp = await fetch(`https://www.headphonesty.com/wp-json/wp/v2/posts?per_page=100&after=${wb.start}T00:00:00&before=${wb.end}T23:59:59&_fields=id,date,title,link&status=publish&orderby=date&order=desc`);
    const wpPosts = await wpResp.json();
    const articlesCount = Array.isArray(wpPosts) ? wpPosts.length : 0;

    res.json({
      period,
      dateRange: period === 'monthly' ? getMonthBounds() : getWeekBounds(),
      visitors: { current: visitors, projected: visitorsProjected, prior: priorVisitors, target: targetSet.visitors },
      fbReach: { current: fbReach, projected: fbReachProjected, prior: priorReach, target: targetSet.fb_reach },
      fbClicks: { current: fbClicks, projected: fbClicksProjected, prior: priorClicks, target: targetSet.fb_clicks },
      articles: { current: articlesCount, target: WEEKLY_TARGETS.articles, weekRange: wb }
    });
  } catch (e) {
    console.error('Scorecards error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ===== TRAFFIC =====
app.get('/api/visitors-daily', async (req, res) => {
  const days = parseInt(req.query.days) || 30;
  try {
    const data = await clickyFetch('visitors', { date: `last-${days}-days`, daily: 1 });
    res.json(data[0]?.dates?.map(d => ({ date: d.date, visitors: parseInt(d.items?.[0]?.value || '0') })) || []);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/traffic-sources', async (req, res) => {
  const days = parseInt(req.query.days) || 7;
  try {
    // Current period
    const current = await clickyFetch('traffic-sources', { date: `last-${days}-days` });
    // Prior period (SGT-aware)
    const sgt = nowSGT();
    const priorEnd = new Date(sgt); priorEnd.setUTCDate(priorEnd.getUTCDate() - days);
    const priorStart = new Date(priorEnd); priorStart.setUTCDate(priorStart.getUTCDate() - days);
    const prior = await clickyFetch('traffic-sources', { date: `${fmtDate(priorStart)},${fmtDate(priorEnd)}` });
    res.json({ current: current[0]?.dates?.[0]?.items || [], prior: prior[0]?.dates?.[0]?.items || [] });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/top-pages', async (req, res) => {
  const days = parseInt(req.query.days) || 7;
  try {
    const data = await clickyFetch('pages', { date: `last-${days}-days`, limit: req.query.limit || 8 });
    res.json(data[0]?.dates?.[0]?.items?.map(i => ({ title: i.title, visitors: parseInt(i.value), url: i.url })) || []);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ===== WORDPRESS =====
app.get('/api/wp-posts', async (req, res) => {
  const days = parseInt(req.query.days) || 60;
  const after = new Date(Date.now() - days * 86400000).toISOString();
  try {
    const resp = await fetch(`https://www.headphonesty.com/wp-json/wp/v2/posts?per_page=100&after=${after}&_fields=id,date,title,status,link&status=publish&orderby=date&order=desc`);
    const posts = await resp.json();
    res.json({ posts: Array.isArray(posts) ? posts : [], total: Array.isArray(posts) ? posts.length : 0 });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ===== POSTS.DB =====
// All DB queries convert created_at (UTC+0000) to SGT date before grouping/filtering
// created_at format: "2026-03-09T21:35:24+0000" — hour is at position 11-12
// SGT = UTC+8, so if hour >= 16, the SGT date is next day
function sgtCutoff(days) {
  const sgt = nowSGT();
  sgt.setUTCDate(sgt.getUTCDate() - days);
  return fmtDate(sgt);
}

const SGT_DAY_EXPR = `CASE WHEN CAST(substr(created_at,12,2) AS INTEGER) >= 16 THEN date(substr(created_at,1,10), '+1 day') ELSE substr(created_at,1,10) END`;

app.get('/api/daily', (req, res) => {
  const days = parseInt(req.query.days) || 30;
  const db = getDb();
  if (!db) return res.status(500).json({ error: 'DB not available' });
  try {
    res.json(db.prepare(`SELECT ${SGT_DAY_EXPR} as day, COUNT(*) as posts, ROUND(AVG(reach)) as avg_reach, SUM(reach) as total_reach, SUM(shares) as total_shares, SUM(comments) as total_comments FROM posts WHERE ${SGT_DAY_EXPR} >= ? GROUP BY day ORDER BY day`).all(sgtCutoff(days)));
  } finally { db.close(); }
});

app.get('/api/formats', (req, res) => {
  const days = parseInt(req.query.days) || 7;
  const db = getDb();
  if (!db) return res.status(500).json({ error: 'DB not available' });
  try {
    res.json(db.prepare(`SELECT post_type, COUNT(*) as cnt, ROUND(AVG(reach)) as avg_reach FROM posts WHERE ${SGT_DAY_EXPR} >= ? GROUP BY post_type ORDER BY avg_reach DESC`).all(sgtCutoff(days)));
  } finally { db.close(); }
});

app.get('/api/hourly', (req, res) => {
  const days = parseInt(req.query.days) || 30;
  const db = getDb();
  if (!db) return res.status(500).json({ error: 'DB not available' });
  try {
    res.json(db.prepare(`SELECT (CAST(substr(created_at,12,2) AS INTEGER)+8)%24 as hour_sgt, COUNT(*) as posts, ROUND(AVG(reach)) as avg_reach FROM posts WHERE ${SGT_DAY_EXPR} >= ? GROUP BY hour_sgt ORDER BY hour_sgt`).all(sgtCutoff(days)));
  } finally { db.close(); }
});

app.get('/api/top-posts', (req, res) => {
  const days = parseInt(req.query.days) || 7;
  const db = getDb();
  if (!db) return res.status(500).json({ error: 'DB not available' });
  try {
    res.json(db.prepare(`SELECT id, ${SGT_DAY_EXPR} as day, post_type, reach, shares, comments, link_url, substr(message,1,100) as msg FROM posts WHERE ${SGT_DAY_EXPR} >= ? ORDER BY reach DESC LIMIT 10`).all(sgtCutoff(days)));
  } finally { db.close(); }
});

app.get('/api/mix', (req, res) => {
  const days = parseInt(req.query.days) || 7;
  const db = getDb();
  if (!db) return res.status(500).json({ error: 'DB not available' });
  try {
    res.json(db.prepare(`SELECT ${SGT_DAY_EXPR} as day, post_type, COUNT(*) as cnt FROM posts WHERE ${SGT_DAY_EXPR} >= ? GROUP BY day, post_type ORDER BY day`).all(sgtCutoff(days)));
  } finally { db.close(); }
});

app.get('/api/fb-daily-posts', (req, res) => {
  const days = parseInt(req.query.days) || 7;
  const db = getDb();
  if (!db) return res.status(500).json({ error: 'DB not available' });
  try {
    res.json(db.prepare(`SELECT ${SGT_DAY_EXPR} as day, COUNT(*) as posts FROM posts WHERE ${SGT_DAY_EXPR} >= ? GROUP BY day ORDER BY day`).all(sgtCutoff(days)));
  } finally { db.close(); }
});

// ===== SYNC =====
app.post('/api/sync', express.raw({ type: 'application/octet-stream', limit: '100mb' }), (req, res) => {
  if (req.headers['x-sync-token'] !== process.env.SYNC_TOKEN) return res.status(401).json({ error: 'Unauthorized' });
  const dir = path.dirname(DB_PATH);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(DB_PATH, req.body);
  res.json({ ok: true, size: req.body.length });
});

app.post('/api/sync-from-url', express.json(), async (req, res) => {
  if (req.headers['x-sync-token'] !== process.env.SYNC_TOKEN) return res.status(401).json({ error: 'Unauthorized' });
  const { url } = req.body;
  if (!url) return res.status(400).json({ error: 'url required' });
  try {
    const dir = path.dirname(DB_PATH);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    const { execSync } = require('child_process');
    const tmp = url.endsWith('.gz') ? DB_PATH + '.gz' : DB_PATH;
    execSync(`wget -q -O "${tmp}" "${url}"`, { timeout: 120000 });
    if (url.endsWith('.gz')) execSync(`gunzip -f "${tmp}"`, { timeout: 30000 });
    res.json({ ok: true, size: fs.statSync(DB_PATH).size });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/health', (req, res) => {
  const sgt = nowSGT();
  res.json({ status: 'ok', db: fs.existsSync(DB_PATH), timezone: 'SGT (UTC+8)', serverUTC: new Date().toISOString(), sgt: fmtDate(sgt) + 'T' + sgt.toISOString().slice(11) });
});

app.use(express.static(path.join(__dirname, '..', 'public')));
app.get('*', (req, res) => res.sendFile(path.join(__dirname, '..', 'public', 'index.html')));

app.listen(PORT, '0.0.0.0', () => console.log(`Dashboard v2 on port ${PORT}`));
