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

function getDb() {
  if (!fs.existsSync(DB_PATH)) return null;
  return new Database(DB_PATH, { readonly: true });
}

async function clickyFetch(type, opts = {}) {
  const params = new URLSearchParams({
    site_id: CLICKY_SITE_ID,
    sitekey: CLICKY_SITE_KEY,
    type,
    output: 'json',
    ...opts
  });
  const r = await fetch(`${CLICKY_BASE}?${params}`);
  return r.json();
}

// ===== CLICKY ENDPOINTS =====

// Real-time visitors
app.get('/api/realtime', async (req, res) => {
  try {
    const data = await clickyFetch('visitors-online');
    const count = parseInt(data[0]?.dates?.[0]?.items?.[0]?.value || '0');
    res.json({ visitors: count, timestamp: new Date().toISOString() });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Daily visitors trend
app.get('/api/visitors', async (req, res) => {
  const days = parseInt(req.query.days) || 30;
  try {
    const data = await clickyFetch('visitors', { date: `last-${days}-days`, daily: 1 });
    const rows = data[0]?.dates?.map(d => ({
      date: d.date,
      visitors: parseInt(d.items?.[0]?.value || '0')
    })) || [];
    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// MTD visitors (for target tracking)
app.get('/api/visitors-mtd', async (req, res) => {
  try {
    const now = new Date();
    const startOfMonth = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-01`;
    const today = now.toISOString().slice(0, 10);
    const data = await clickyFetch('visitors', { date: `${startOfMonth},${today}` });
    const total = parseInt(data[0]?.dates?.[0]?.items?.[0]?.value || '0');
    const dayOfMonth = now.getDate();
    const daysInMonth = new Date(now.getFullYear(), now.getMonth() + 1, 0).getDate();
    const projected = Math.round(total / dayOfMonth * daysInMonth);
    res.json({ mtd: total, projected, dayOfMonth, daysInMonth, target: 2000000 });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Traffic sources (daily or aggregate)
app.get('/api/traffic-sources', async (req, res) => {
  const days = parseInt(req.query.days) || 30;
  const daily = req.query.daily === '1';
  try {
    const opts = { date: `last-${days}-days` };
    if (daily) opts.daily = 1;
    const data = await clickyFetch('traffic-sources', opts);
    res.json(data[0]?.dates || []);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Top pages
app.get('/api/top-pages', async (req, res) => {
  const days = parseInt(req.query.days) || 30;
  const limit = parseInt(req.query.limit) || 10;
  try {
    const data = await clickyFetch('pages', { date: `last-${days}-days`, limit });
    const pages = data[0]?.dates?.[0]?.items?.map(i => ({
      title: i.title,
      visitors: parseInt(i.value),
      url: i.url
    })) || [];
    res.json(pages);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ===== LABBY / FACEBOOK ENDPOINTS =====

app.get('/api/fb-reach', async (req, res) => {
  try {
    const now = new Date();
    const startOfMonth = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-01`;
    const today = now.toISOString().slice(0, 10);

    const form = new FormData();
    form.append('startDate', req.query.start || startOfMonth);
    form.append('endDate', req.query.end || today);

    const r = await fetch(LABBY_URL, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${LABBY_TOKEN}` },
      body: form
    });
    const data = await r.json();
    const d = data?.data || {};
    const s = d.summary || {};

    const dayOfMonth = now.getDate();
    const daysInMonth = new Date(now.getFullYear(), now.getMonth() + 1, 0).getDate();

    res.json({
      reach: d.totalReach || 0,
      reachProjected: Math.round((d.totalReach || 0) / dayOfMonth * daysInMonth),
      reachTarget: 3000000,
      clicks: d.totalFacebookLinkClicks || 0,
      clicksProjected: Math.round((d.totalFacebookLinkClicks || 0) / dayOfMonth * daysInMonth),
      clicksTarget: 45000,
      clicksFromClicky: (s.totalClicksFreshPosts || 0) + (s.totalClicksRecycledPosts || 0),
      totalPosts: s.totalPosts || 0,
      freshPosts: s.uniqueFreshPosts || 0,
      recycledPosts: s.uniqueRecycledPosts || 0,
      engagement: s.totalSocialEngagement || 0,
      avgEngagement: s.averageEngagementPerPost || 0,
      dayOfMonth,
      daysInMonth
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ===== WORDPRESS ENDPOINTS =====

app.get('/api/wp-posts', async (req, res) => {
  const days = parseInt(req.query.days) || 60;
  const perPage = parseInt(req.query.per_page) || 100;
  const after = new Date(Date.now() - days * 86400000).toISOString();
  try {
    const url = `https://www.headphonesty.com/wp-json/wp/v2/posts?per_page=${perPage}&after=${after}&_fields=id,date,title,status,link&status=publish&orderby=date&order=desc`;
    const resp = await fetch(url);
    const total = resp.headers.get('x-wp-total');
    const posts = await resp.json();
    res.json({ posts, total: parseInt(total) || posts.length });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ===== POSTS.DB ENDPOINTS =====

app.get('/api/daily', (req, res) => {
  const days = parseInt(req.query.days) || 60;
  const db = getDb();
  if (!db) return res.status(500).json({ error: 'DB not available' });
  try {
    const rows = db.prepare(`
      SELECT substr(created_at, 1, 10) as day, COUNT(*) as posts,
        ROUND(AVG(reach)) as avg_reach, SUM(reach) as total_reach,
        SUM(shares) as total_shares, SUM(comments) as total_comments
      FROM posts WHERE created_at >= date('now', '-' || ? || ' days')
      GROUP BY substr(created_at, 1, 10) ORDER BY day
    `).all(days);
    res.json(rows);
  } finally { db.close(); }
});

app.get('/api/formats', (req, res) => {
  const days = parseInt(req.query.days) || 60;
  const db = getDb();
  if (!db) return res.status(500).json({ error: 'DB not available' });
  try {
    const rows = db.prepare(`
      SELECT post_type, COUNT(*) as cnt, ROUND(AVG(reach)) as avg_reach,
        ROUND(AVG(shares),1) as avg_shares, ROUND(AVG(comments),1) as avg_comments
      FROM posts WHERE created_at >= date('now', '-' || ? || ' days')
      GROUP BY post_type ORDER BY avg_reach DESC
    `).all(days);
    res.json(rows);
  } finally { db.close(); }
});

app.get('/api/hourly', (req, res) => {
  const days = parseInt(req.query.days) || 60;
  const db = getDb();
  if (!db) return res.status(500).json({ error: 'DB not available' });
  try {
    const rows = db.prepare(`
      SELECT (CAST(substr(created_at, 12, 2) AS INTEGER) + 8) % 24 as hour_sgt,
        COUNT(*) as posts, ROUND(AVG(reach)) as avg_reach
      FROM posts WHERE created_at >= date('now', '-' || ? || ' days')
      GROUP BY hour_sgt ORDER BY hour_sgt
    `).all(days);
    res.json(rows);
  } finally { db.close(); }
});

app.get('/api/top-posts', (req, res) => {
  const days = parseInt(req.query.days) || 30;
  const limit = parseInt(req.query.limit) || 10;
  const db = getDb();
  if (!db) return res.status(500).json({ error: 'DB not available' });
  try {
    const rows = db.prepare(`
      SELECT id, substr(created_at, 1, 10) as day, post_type, reach, shares,
        comments, reactions, link_url, substr(message, 1, 100) as msg
      FROM posts WHERE created_at >= date('now', '-' || ? || ' days')
      ORDER BY reach DESC LIMIT ?
    `).all(days, limit);
    res.json(rows);
  } finally { db.close(); }
});

app.get('/api/mix', (req, res) => {
  const days = parseInt(req.query.days) || 60;
  const db = getDb();
  if (!db) return res.status(500).json({ error: 'DB not available' });
  try {
    const rows = db.prepare(`
      SELECT substr(created_at, 1, 10) as day, post_type, COUNT(*) as cnt
      FROM posts WHERE created_at >= date('now', '-' || ? || ' days')
      GROUP BY substr(created_at, 1, 10), post_type ORDER BY day
    `).all(days);
    res.json(rows);
  } finally { db.close(); }
});

// FB posts per day (for slot tracking)
app.get('/api/fb-daily-posts', (req, res) => {
  const days = parseInt(req.query.days) || 14;
  const db = getDb();
  if (!db) return res.status(500).json({ error: 'DB not available' });
  try {
    const rows = db.prepare(`
      SELECT substr(created_at, 1, 10) as day, COUNT(*) as posts
      FROM posts WHERE created_at >= date('now', '-' || ? || ' days')
      GROUP BY substr(created_at, 1, 10) ORDER BY day
    `).all(days);
    res.json(rows);
  } finally { db.close(); }
});

// ===== SYNC ENDPOINTS =====

app.post('/api/sync', express.raw({ type: 'application/octet-stream', limit: '100mb' }), (req, res) => {
  if (req.headers['x-sync-token'] !== process.env.SYNC_TOKEN) return res.status(401).json({ error: 'Unauthorized' });
  const dataDir = path.dirname(DB_PATH);
  if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
  fs.writeFileSync(DB_PATH, req.body);
  res.json({ ok: true, size: req.body.length });
});

app.post('/api/sync-from-url', express.json(), async (req, res) => {
  if (req.headers['x-sync-token'] !== process.env.SYNC_TOKEN) return res.status(401).json({ error: 'Unauthorized' });
  const { url } = req.body;
  if (!url) return res.status(400).json({ error: 'url required' });
  try {
    const dataDir = path.dirname(DB_PATH);
    if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
    const { execSync } = require('child_process');
    const isGzip = url.endsWith('.gz');
    const tmpFile = isGzip ? DB_PATH + '.gz' : DB_PATH;
    execSync(`wget -q -O "${tmpFile}" "${url}"`, { timeout: 120000 });
    if (isGzip) execSync(`gunzip -f "${tmpFile}"`, { timeout: 30000 });
    const stats = fs.statSync(DB_PATH);
    res.json({ ok: true, size: stats.size });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', db: fs.existsSync(DB_PATH), timestamp: new Date().toISOString() });
});

// Serve static files
app.use(express.static(path.join(__dirname, '..', 'public')));
app.get('*', (req, res) => res.sendFile(path.join(__dirname, '..', 'public', 'index.html')));

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Dashboard v2 running on port ${PORT}`);
});
