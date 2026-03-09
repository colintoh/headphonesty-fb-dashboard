const express = require('express');
const Database = require('better-sqlite3');
const compression = require('compression');
const path = require('path');
const fs = require('fs');

const app = express();
app.use(compression());

const PORT = process.env.PORT || 3000;
const DB_PATH = process.env.DB_PATH || path.join(__dirname, '..', 'data', 'posts.db');

function getDb() {
  if (!fs.existsSync(DB_PATH)) {
    console.error('Database not found at', DB_PATH);
    return null;
  }
  return new Database(DB_PATH, { readonly: true });
}

// API endpoints
app.get('/api/daily', (req, res) => {
  const days = parseInt(req.query.days) || 60;
  const db = getDb();
  if (!db) return res.status(500).json({ error: 'Database not available' });
  
  try {
    const rows = db.prepare(`
      SELECT 
        substr(created_at, 1, 10) as day,
        COUNT(*) as posts,
        ROUND(AVG(reach)) as avg_reach,
        SUM(reach) as total_reach,
        SUM(shares) as total_shares,
        SUM(comments) as total_comments,
        SUM(reactions) as total_reactions
      FROM posts 
      WHERE created_at >= date('now', '-' || ? || ' days')
      GROUP BY substr(created_at, 1, 10)
      ORDER BY day
    `).all(days);
    res.json(rows);
  } finally {
    db.close();
  }
});

app.get('/api/formats', (req, res) => {
  const days = parseInt(req.query.days) || 60;
  const db = getDb();
  if (!db) return res.status(500).json({ error: 'Database not available' });
  
  try {
    const rows = db.prepare(`
      SELECT 
        post_type,
        COUNT(*) as cnt,
        ROUND(AVG(reach)) as avg_reach,
        SUM(reach) as total_reach,
        ROUND(AVG(shares), 1) as avg_shares,
        ROUND(AVG(comments), 1) as avg_comments
      FROM posts 
      WHERE created_at >= date('now', '-' || ? || ' days')
      GROUP BY post_type
      ORDER BY avg_reach DESC
    `).all(days);
    res.json(rows);
  } finally {
    db.close();
  }
});

app.get('/api/hourly', (req, res) => {
  const days = parseInt(req.query.days) || 60;
  const db = getDb();
  if (!db) return res.status(500).json({ error: 'Database not available' });
  
  try {
    const rows = db.prepare(`
      SELECT 
        (CAST(substr(created_at, 12, 2) AS INTEGER) + 8) % 24 as hour_sgt,
        COUNT(*) as posts,
        ROUND(AVG(reach)) as avg_reach,
        ROUND(AVG(shares), 1) as avg_shares
      FROM posts 
      WHERE created_at >= date('now', '-' || ? || ' days')
      GROUP BY hour_sgt
      ORDER BY hour_sgt
    `).all(days);
    res.json(rows);
  } finally {
    db.close();
  }
});

app.get('/api/top-posts', (req, res) => {
  const days = parseInt(req.query.days) || 30;
  const limit = parseInt(req.query.limit) || 10;
  const db = getDb();
  if (!db) return res.status(500).json({ error: 'Database not available' });
  
  try {
    const rows = db.prepare(`
      SELECT 
        id,
        substr(created_at, 1, 10) as day,
        post_type,
        reach,
        shares,
        comments,
        reactions,
        link_url,
        substr(message, 1, 100) as msg
      FROM posts 
      WHERE created_at >= date('now', '-' || ? || ' days')
      ORDER BY reach DESC
      LIMIT ?
    `).all(days, limit);
    res.json(rows);
  } finally {
    db.close();
  }
});

app.get('/api/mix', (req, res) => {
  const days = parseInt(req.query.days) || 60;
  const db = getDb();
  if (!db) return res.status(500).json({ error: 'Database not available' });
  
  try {
    const rows = db.prepare(`
      SELECT 
        substr(created_at, 1, 10) as day,
        post_type,
        COUNT(*) as cnt
      FROM posts 
      WHERE created_at >= date('now', '-' || ? || ' days')
      GROUP BY substr(created_at, 1, 10), post_type
      ORDER BY day
    `).all(days);
    res.json(rows);
  } finally {
    db.close();
  }
});

app.get('/api/stats', (req, res) => {
  const db = getDb();
  if (!db) return res.status(500).json({ error: 'Database not available' });
  
  try {
    const row = db.prepare(`
      SELECT 
        COUNT(*) as total_posts,
        MIN(created_at) as first_post,
        MAX(created_at) as last_post
      FROM posts
    `).get();
    res.json(row);
  } finally {
    db.close();
  }
});

// Upload endpoint for DB sync
app.post('/api/sync', express.raw({ type: 'application/octet-stream', limit: '100mb' }), (req, res) => {
  const token = req.headers['x-sync-token'];
  if (token !== process.env.SYNC_TOKEN) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  
  const dataDir = path.dirname(DB_PATH);
  if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
  
  fs.writeFileSync(DB_PATH, req.body);
  console.log(`DB synced: ${req.body.length} bytes at ${new Date().toISOString()}`);
  res.json({ ok: true, size: req.body.length, timestamp: new Date().toISOString() });
});

// Download DB from URL (for initial setup / large files)
app.post('/api/sync-from-url', express.json(), async (req, res) => {
  const token = req.headers['x-sync-token'];
  if (token !== process.env.SYNC_TOKEN) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  
  const { url } = req.body;
  if (!url) return res.status(400).json({ error: 'url required' });
  
  try {
    const dataDir = path.dirname(DB_PATH);
    if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
    
    const { execSync } = require('child_process');
    const isGzip = url.endsWith('.gz');
    const tmpFile = isGzip ? DB_PATH + '.gz' : DB_PATH;
    
    console.log(`Downloading DB from ${url}...`);
    execSync(`wget -q -O "${tmpFile}" "${url}"`, { timeout: 120000 });
    
    if (isGzip) {
      execSync(`gunzip -f "${tmpFile}"`, { timeout: 30000 });
    }
    
    const stats = fs.statSync(DB_PATH);
    console.log(`DB downloaded: ${stats.size} bytes at ${new Date().toISOString()}`);
    res.json({ ok: true, size: stats.size, timestamp: new Date().toISOString() });
  } catch (e) {
    console.error('Sync from URL failed:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// WordPress posts proxy
app.get('/api/wp-posts', async (req, res) => {
  const days = parseInt(req.query.days) || 30;
  const perPage = parseInt(req.query.per_page) || 100;
  const after = new Date(Date.now() - days * 86400000).toISOString();
  
  try {
    const url = `https://www.headphonesty.com/wp-json/wp/v2/posts?per_page=${perPage}&after=${after}&_fields=id,date,title,status,link&status=publish&orderby=date&order=desc`;
    const resp = await fetch(url);
    const total = resp.headers.get('x-wp-total');
    const posts = await resp.json();
    res.json({ posts, total: parseInt(total) || posts.length });
  } catch (e) {
    console.error('WP API error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// Health check
app.get('/api/health', (req, res) => {
  const dbExists = fs.existsSync(DB_PATH);
  res.json({ status: 'ok', db: dbExists, timestamp: new Date().toISOString() });
});

// Serve static files
app.use(express.static(path.join(__dirname, '..', 'public')));

// SPA fallback
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, '..', 'public', 'index.html'));
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Dashboard running on port ${PORT}`);
  console.log(`DB path: ${DB_PATH}`);
});
