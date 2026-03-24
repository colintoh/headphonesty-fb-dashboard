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

// ===== SCORECARDS (week-scoped with period-over-period) =====
// Cache: past weeks never change, current week cached for 2 min
const scorecardCache = new Map();
const CACHE_TTL_CURRENT = 2 * 60 * 1000; // 2 min for current week
const CACHE_TTL_PAST = 24 * 60 * 60 * 1000; // 24h for past weeks

async function fetchLabby(startDate, endDate) {
  const form = new FormData();
  form.append('startDate', startDate);
  form.append('endDate', endDate);
  const resp = await fetch(LABBY_URL, { method: 'POST', headers: { 'Authorization': `Bearer ${LABBY_TOKEN}` }, body: form });
  return (await resp.json())?.data || {};
}

app.get('/api/scorecards', async (req, res) => {
  const weeksAgo = parseInt(req.query.weeksAgo) || 0;
  const cacheKey = `sc_${weeksAgo}`;
  const cached = scorecardCache.get(cacheKey);
  const ttl = weeksAgo === 0 ? CACHE_TTL_CURRENT : CACHE_TTL_PAST;
  if (cached && Date.now() - cached.ts < ttl) {
    return res.json({ ...cached.data, cached: true });
  }

  try {
    const wb = getWeekBounds(weeksAgo);
    const isCurrentWeek = weeksAgo === 0;
    const daysElapsed = isCurrentWeek ? wb.dayOfWeek : 7;
    const priorWb = getWeekBounds(weeksAgo + 1);
    const priorEnd = new Date(priorWb.start + 'T00:00:00Z');
    priorEnd.setUTCDate(priorEnd.getUTCDate() + daysElapsed - 1);
    const priorEndStr = fmtDate(priorEnd);
    const currentEnd = isCurrentWeek ? fmtDate(nowSGT()) : wb.end;

    // ALL 5 API calls in PARALLEL — graceful partial failures
    const results = await Promise.allSettled([
      clickyFetch('visitors', { date: `${wb.start},${currentEnd}` }),
      clickyFetch('visitors', { date: `${priorWb.start},${priorEndStr}` }),
      fetchLabby(wb.start, currentEnd),
      fetchLabby(priorWb.start, priorEndStr),
      fetch(`https://www.headphonesty.com/wp-json/wp/v2/posts?per_page=100&after=${wb.start}T00:00:00&before=${wb.end}T23:59:59&_fields=id,date,title,link&status=publish&orderby=date&order=desc`).then(r => r.json()).catch(() => []),
    ]);
    const val = (i) => results[i].status === 'fulfilled' ? results[i].value : null;
    const vData = val(0), pvData = val(1), fbData = val(2) || {}, pfData = val(3) || {}, wpPosts = val(4) || [];
    results.forEach((r, i) => { if (r.status === 'rejected') console.error(`Scorecards API ${i} failed:`, r.reason?.message || r.reason); });

    const visitors = parseInt(vData?.[0]?.dates?.[0]?.items?.[0]?.value || '0');
    const priorVisitors = parseInt(pvData?.[0]?.dates?.[0]?.items?.[0]?.value || '0');
    const fbReach = fbData.totalReach || 0;
    const fbClicks = fbData.totalFacebookLinkClicks || 0;
    const priorReach = pfData.totalReach || 0;
    const priorClicks = pfData.totalFacebookLinkClicks || 0;
    const articlesCount = Array.isArray(wpPosts) ? wpPosts.length : 0;

    const visitorsProjected = isCurrentWeek ? Math.round(visitors / daysElapsed * 7) : visitors;
    const fbReachProjected = isCurrentWeek ? Math.round(fbReach / daysElapsed * 7) : fbReach;
    const fbClicksProjected = isCurrentWeek ? Math.round(fbClicks / daysElapsed * 7) : fbClicks;

    const result = {
      weeksAgo, isCurrentWeek, daysElapsed,
      weekRange: wb,
      priorWindow: { start: priorWb.start, end: priorEndStr },
      visitors: { current: visitors, projected: visitorsProjected, prior: priorVisitors, target: WEEKLY_TARGETS.visitors },
      fbReach: { current: fbReach, projected: fbReachProjected, prior: priorReach, target: WEEKLY_TARGETS.fb_reach },
      fbClicks: { current: fbClicks, projected: fbClicksProjected, prior: priorClicks, target: WEEKLY_TARGETS.fb_clicks },
      articles: { current: articlesCount, target: WEEKLY_TARGETS.articles }
    };

    scorecardCache.set(cacheKey, { ts: Date.now(), data: result });
    res.json(result);
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
    const result = (data[0]?.dates?.map(d => ({ date: d.date, visitors: parseInt(d.items?.[0]?.value || '0') })) || []);
    result.sort((a, b) => a.date.localeCompare(b.date));
    res.json(result);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Traffic sources: supports ?weeksAgo= for week nav + period-over-period (same elapsed days)
const trafficSourcesCache = new Map();

app.get('/api/traffic-sources', async (req, res) => {
  const weeksAgo = parseInt(req.query.weeksAgo) || 0;
  const cacheKey = `ts_${weeksAgo}`;
  const cached = trafficSourcesCache.get(cacheKey);
  const ttl = weeksAgo === 0 ? CACHE_TTL_CURRENT : CACHE_TTL_PAST;
  if (cached && Date.now() - cached.ts < ttl) {
    return res.json(cached.data);
  }

  try {
    const wb = getWeekBounds(weeksAgo);
    const isCurrentWeek = weeksAgo === 0;
    const daysElapsed = isCurrentWeek ? wb.dayOfWeek : 7;
    const currentEnd = isCurrentWeek ? fmtDate(nowSGT()) : wb.end;

    // Prior week same window (Mon through Mon+daysElapsed-1)
    const priorWb = getWeekBounds(weeksAgo + 1);
    const priorEnd = new Date(priorWb.start + 'T00:00:00Z');
    priorEnd.setUTCDate(priorEnd.getUTCDate() + daysElapsed - 1);
    const priorEndStr = fmtDate(priorEnd);

    // Parallel fetch
    const [current, prior] = await Promise.all([
      clickyFetch('traffic-sources', { date: `${wb.start},${currentEnd}` }),
      clickyFetch('traffic-sources', { date: `${priorWb.start},${priorEndStr}` }),
    ]);

    const result = {
      current: current[0]?.dates?.[0]?.items || [],
      prior: prior[0]?.dates?.[0]?.items || [],
      daysElapsed, isCurrentWeek
    };
    trafficSourcesCache.set(cacheKey, { ts: Date.now(), data: result });
    res.json(result);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/top-pages', async (req, res) => {
  const weeksAgo = parseInt(req.query.weeksAgo) || 0;
  try {
    const wb = getWeekBounds(weeksAgo);
    const currentEnd = weeksAgo === 0 ? fmtDate(nowSGT()) : wb.end;
    const data = await clickyFetch('pages', { date: `${wb.start},${currentEnd}`, limit: req.query.limit || 8 });
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

// ===== NOTION / ICEBOX PIPELINE =====
const NOTION_API_KEY = process.env.NOTION_API_KEY || '';
const NOTION_DB_ID = '9186c98c714c4125a3b477f63b4a86a0';
const NOTION_BASE = 'https://api.notion.com/v1';

async function notionQuery(dbId, filter = {}, startCursor = undefined) {
  const body = { page_size: 100, ...filter };
  if (startCursor) body.start_cursor = startCursor;
  const r = await fetch(`${NOTION_BASE}/databases/${dbId}/query`, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${NOTION_API_KEY}`,
      'Notion-Version': '2022-06-28',
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(body)
  });
  return r.json();
}

async function notionQueryAll(dbId, filter = {}) {
  let all = [];
  let cursor = undefined;
  do {
    const resp = await notionQuery(dbId, filter, cursor);
    all = all.concat(resp.results || []);
    cursor = resp.has_more ? resp.next_cursor : undefined;
  } while (cursor);
  return all;
}

// Cache icebox data for 5 min
let iceboxCache = { ts: 0, data: null };
const ICEBOX_CACHE_TTL = 5 * 60 * 1000;

app.get('/api/icebox', async (req, res) => {
  const now = Date.now();
  if (iceboxCache.data && now - iceboxCache.ts < ICEBOX_CACHE_TTL && !req.query.refresh) {
    return res.json(iceboxCache.data);
  }

  try {
    // Two parallel queries: Icebox items (bank) + recent items (inflow, last 13 weeks only)
    const inflowStart = new Date(Date.now() - 13 * 7 * 86400000).toISOString();
    const [iceboxItems, recentItems] = await Promise.all([
      notionQueryAll(NOTION_DB_ID, { filter: { property: 'Stage', select: { equals: 'Icebox' } } }),
      notionQueryAll(NOTION_DB_ID, { filter: { timestamp: 'created_time', created_time: { on_or_after: inflowStart } } })
    ]);

    // Bank: current Icebox breakdown
    let listicles = 0, nonListicles = 0;
    const bankItems = iceboxItems.map(p => {
      const title = (p.properties['Angle']?.title || []).map(t => t.plain_text).join('');
      const types = (p.properties['Article Type']?.multi_select || []).map(t => t.name);
      const isList = types.includes('List');
      if (isList) listicles++; else nonListicles++;
      return { id: p.id, title, types, created: p.created_time };
    });

    // Weekly inflow: pre-populate last 12 weeks (Mon-Sun), then count
    const weekMap = {};
    const sgtNow = nowSGT();
    const nowDay = sgtNow.getUTCDay() || 7; // 1=Mon..7=Sun
    const thisMon = new Date(sgtNow);
    thisMon.setUTCDate(thisMon.getUTCDate() - (nowDay - 1));
    thisMon.setUTCHours(0, 0, 0, 0);
    for (let i = 0; i < 12; i++) {
      const mon = new Date(thisMon);
      mon.setUTCDate(mon.getUTCDate() - i * 7);
      weekMap[fmtDate(mon)] = 0;
    }

    recentItems.forEach(p => {
      const created = new Date(p.created_time);
      const sgt = new Date(created.getTime() + 8 * 60 * 60 * 1000);
      const day = sgt.getUTCDay() || 7;
      const mon = new Date(sgt);
      mon.setUTCDate(mon.getUTCDate() - (day - 1));
      const weekKey = fmtDate(mon);
      if (weekKey in weekMap) weekMap[weekKey]++;
    });

    const weeklyInflow = Object.entries(weekMap)
      .sort((a, b) => b[0].localeCompare(a[0]))
      .map(([week, count]) => ({ week, count }));

    const thisWeek = weeklyInflow[0]?.count || 0;
    const lastWeek = weeklyInflow[1]?.count || 0;

    const result = {
      bank: { total: iceboxItems.length, listicles, nonListicles, items: bankItems },
      weeklyInflow,
      thisWeek,
      lastWeek,
      totalTopics: recentItems.length
    };

    iceboxCache = { ts: now, data: result };
    res.json(result);
  } catch (e) {
    console.error('Icebox error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ===== POSTS.DB =====
// All DB queries convert created_at (UTC+0000) to SGT date before grouping/filtering
const SGT_DAY_EXPR = `CASE WHEN CAST(substr(created_at,12,2) AS INTEGER) >= 16 THEN date(substr(created_at,1,10), '+1 day') ELSE substr(created_at,1,10) END`;

// Helper: get date range from query params. Supports ?start=&end= OR ?days=
function getDateRange(query, defaultDays = 30) {
  if (query.start && query.end) return { start: query.start, end: query.end };
  const days = parseInt(query.days) || defaultDays;
  const sgt = nowSGT();
  const end = fmtDate(sgt);
  sgt.setUTCDate(sgt.getUTCDate() - days);
  return { start: fmtDate(sgt), end };
}

app.get('/api/daily', (req, res) => {
  const { start, end } = getDateRange(req.query, 30);
  const db = getDb();
  if (!db) return res.status(500).json({ error: 'DB not available' });
  try {
    res.json(db.prepare(`SELECT ${SGT_DAY_EXPR} as day, COUNT(*) as posts, ROUND(AVG(reach)) as avg_reach, SUM(reach) as total_reach, SUM(shares) as total_shares, SUM(comments) as total_comments FROM posts WHERE ${SGT_DAY_EXPR} >= ? AND ${SGT_DAY_EXPR} <= ? GROUP BY day ORDER BY day`).all(start, end));
  } finally { db.close(); }
});

app.get('/api/formats', (req, res) => {
  const { start, end } = getDateRange(req.query, 7);
  const db = getDb();
  if (!db) return res.status(500).json({ error: 'DB not available' });
  try {
    res.json(db.prepare(`SELECT post_type, COUNT(*) as cnt, ROUND(AVG(reach)) as avg_reach FROM posts WHERE ${SGT_DAY_EXPR} >= ? AND ${SGT_DAY_EXPR} <= ? GROUP BY post_type ORDER BY avg_reach DESC`).all(start, end));
  } finally { db.close(); }
});

app.get('/api/hourly', (req, res) => {
  const { start, end } = getDateRange(req.query, 30);
  const db = getDb();
  if (!db) return res.status(500).json({ error: 'DB not available' });
  try {
    res.json(db.prepare(`SELECT (CAST(substr(created_at,12,2) AS INTEGER)+8)%24 as hour_sgt, COUNT(*) as posts, ROUND(AVG(reach)) as avg_reach FROM posts WHERE ${SGT_DAY_EXPR} >= ? AND ${SGT_DAY_EXPR} <= ? GROUP BY hour_sgt ORDER BY hour_sgt`).all(start, end));
  } finally { db.close(); }
});

app.get('/api/top-posts', (req, res) => {
  const { start, end } = getDateRange(req.query, 7);
  const db = getDb();
  if (!db) return res.status(500).json({ error: 'DB not available' });
  try {
    res.json(db.prepare(`SELECT id, ${SGT_DAY_EXPR} as day, post_type, COALESCE(post_purpose,'') as purpose, COALESCE(post_freshness,'') as freshness, reach, shares, comments, link_url, substr(message,1,100) as msg FROM posts WHERE ${SGT_DAY_EXPR} >= ? AND ${SGT_DAY_EXPR} <= ? ORDER BY reach DESC LIMIT 10`).all(start, end));
  } finally { db.close(); }
});

app.get('/api/mix', (req, res) => {
  const { start, end } = getDateRange(req.query, 7);
  const db = getDb();
  if (!db) return res.status(500).json({ error: 'DB not available' });
  try {
    res.json(db.prepare(`SELECT ${SGT_DAY_EXPR} as day, post_type, COUNT(*) as cnt FROM posts WHERE ${SGT_DAY_EXPR} >= ? AND ${SGT_DAY_EXPR} <= ? GROUP BY day, post_type ORDER BY day`).all(start, end));
  } finally { db.close(); }
});

// Content Impact — aggregated by type with median reach
app.get('/api/content-impact', (req, res) => {
  const weeksAgo = parseInt(req.query.weeksAgo) || 0;
  const wb = getWeekBounds(weeksAgo);
  const currentEnd = weeksAgo === 0 ? fmtDate(nowSGT()) : wb.end;
  const db = getDb();
  if (!db) return res.status(500).json({ error: 'DB not available' });
  try {
    const posts = db.prepare(`SELECT post_type, COALESCE(post_purpose,'engagement') as purpose, post_freshness, reach, shares, comments FROM posts WHERE ${SGT_DAY_EXPR} >= ? AND ${SGT_DAY_EXPR} <= ?`).all(wb.start, currentEnd);

    function computeStats(filtered, allCount, allReach) {
      const byFormat = {};
      const allFormats = ['reel', 'story', 'meme', 'status', 'other'];
      allFormats.forEach(f => { byFormat[f] = { reaches: [], totalReach: 0, totalShares: 0, totalComments: 0 }; });
      let totalReach = 0, totalPosts = 0;
      filtered.forEach(p => {
        const f = allFormats.includes(p.post_type) ? p.post_type : 'other';
        byFormat[f].reaches.push(p.reach || 0);
        byFormat[f].totalReach += (p.reach || 0);
        byFormat[f].totalShares += (p.shares || 0);
        byFormat[f].totalComments += (p.comments || 0);
        totalReach += (p.reach || 0);
        totalPosts++;
      });
      const formats = Object.entries(byFormat).map(([fmt, d]) => {
        const sorted = d.reaches.sort((a, b) => a - b);
        const median = sorted.length === 0 ? 0 : sorted.length % 2 ? sorted[Math.floor(sorted.length / 2)] : Math.round((sorted[Math.floor(sorted.length / 2) - 1] + sorted[Math.floor(sorted.length / 2)]) / 2);
        const engRate = d.totalReach > 0 ? ((d.totalShares + d.totalComments) / d.totalReach * 100) : 0;
        return {
          format: fmt, count: sorted.length,
          countPct: totalPosts > 0 ? Math.round(sorted.length / totalPosts * 100) : 0,
          totalReach: d.totalReach,
          reachPct: totalReach > 0 ? Math.round(d.totalReach / totalReach * 100) : 0,
          medianReach: median, totalShares: d.totalShares, totalComments: d.totalComments,
          engRate: Math.round(engRate * 100) / 100
        };
      }).sort((a, b) => b.medianReach - a.medianReach);
      return { totalPosts, totalReach, countPctOfAll: allCount > 0 ? Math.round(totalPosts / allCount * 100) : 0, reachPctOfAll: allReach > 0 ? Math.round(totalReach / allReach * 100) : 0, formats };
    }

    const allCount = posts.length;
    const allReach = posts.reduce((s, p) => s + (p.reach || 0), 0);
    const promo = posts.filter(p => p.purpose === 'promotional');
    const engage = posts.filter(p => p.purpose !== 'promotional');

    // Freshness split for promotional
    const promoFresh = promo.filter(p => p.post_freshness === 'fresh');
    const promoRecycled = promo.filter(p => p.post_freshness === 'recycled');

    const freshReach = promoFresh.reduce((s,p)=>s+(p.reach||0),0);
    const recycledReach = promoRecycled.reduce((s,p)=>s+(p.reach||0),0);

    // Combined freshness×format breakdown (all sum to 100% of promo)
    const promoTotal = promo.length;
    const promoReach = promo.reduce((s,p)=>s+(p.reach||0),0);
    const comboMap = {};
    promo.forEach(p => {
      const f = ['reel','story','meme','status'].includes(p.post_type) ? p.post_type : 'other';
      const key = `${p.post_freshness||'fresh'}_${f}`;
      if (!comboMap[key]) comboMap[key] = { reaches: [], totalReach: 0, totalShares: 0, totalComments: 0, freshness: p.post_freshness||'fresh', format: f };
      comboMap[key].reaches.push(p.reach||0);
      comboMap[key].totalReach += (p.reach||0);
      comboMap[key].totalShares += (p.shares||0);
      comboMap[key].totalComments += (p.comments||0);
    });
    const combos = Object.values(comboMap).map(d => {
      const sorted = d.reaches.sort((a,b)=>a-b);
      const median = sorted.length===0?0:sorted.length%2?sorted[Math.floor(sorted.length/2)]:Math.round((sorted[Math.floor(sorted.length/2)-1]+sorted[Math.floor(sorted.length/2)])/2);
      const engRate = d.totalReach>0?((d.totalShares+d.totalComments)/d.totalReach*100):0;
      return {
        freshness: d.freshness, format: d.format,
        count: sorted.length, countPct: promoTotal>0?Math.round(sorted.length/promoTotal*100):0,
        totalReach: d.totalReach, reachPct: promoReach>0?Math.round(d.totalReach/promoReach*100):0,
        medianReach: median, totalShares: d.totalShares, totalComments: d.totalComments,
        engRate: Math.round(engRate*100)/100
      };
    }).sort((a,b)=>b.totalReach-a.totalReach);

    res.json({
      weekRange: wb, totalPosts: allCount, totalReach: allReach,
      promotional: {
        ...computeStats(promo, allCount, allReach),
        fresh: { count: promoFresh.length, reach: freshReach },
        recycled: { count: promoRecycled.length, reach: recycledReach },
        combos
      },
      engagement: computeStats(engage, allCount, allReach)
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
  finally { db.close(); }
});

// Posts detail by day (for distribution calendar popup)
app.get('/api/fb-posts-detail', (req, res) => {
  const { start, end } = getDateRange(req.query, 60);
  const db = getDb();
  if (!db) return res.status(500).json({ error: 'DB not available' });
  try {
    const rows = db.prepare(`
      SELECT ${SGT_DAY_EXPR} as day, id, post_type, reach, shares, comments, link_url, substr(message,1,120) as msg,
        substr(created_at,12,5) as time_utc,
        printf('%02d:%02d', (CAST(substr(created_at,12,2) AS INTEGER)+8)%24, CAST(substr(created_at,15,2) AS INTEGER)) as time_sgt
      FROM posts
      WHERE ${SGT_DAY_EXPR} >= ? AND ${SGT_DAY_EXPR} <= ?
      ORDER BY created_at
    `).all(start, end);
    // Group by day
    const byDay = {};
    rows.forEach(r => {
      if (!byDay[r.day]) byDay[r.day] = [];
      byDay[r.day].push(r);
    });
    res.json(byDay);
  } finally { db.close(); }
});

app.get('/api/fb-daily-posts', (req, res) => {
  const { start, end } = getDateRange(req.query, 7);
  const db = getDb();
  if (!db) return res.status(500).json({ error: 'DB not available' });
  try {
    res.json(db.prepare(`SELECT ${SGT_DAY_EXPR} as day, COUNT(*) as posts FROM posts WHERE ${SGT_DAY_EXPR} >= ? AND ${SGT_DAY_EXPR} <= ? GROUP BY day ORDER BY day`).all(start, end));
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

// ===== INSIGHTS (AI Advisor Panel) =====
const Anthropic = require('@anthropic-ai/sdk');
let insightsCache = { ts: 0, data: null, generating: false };
const INSIGHTS_TTL = 60 * 60 * 1000; // 1 hour

async function gatherDashboardContext() {
  const db = getDb();
  if (!db) return 'Database not available.';

  try {
    const wb = getWeekBounds(0);
    const priorWb = getWeekBounds(1);
    const currentEnd = fmtDate(nowSGT());
    const daysElapsed = wb.dayOfWeek;

    // Prior week same window
    const priorEnd = new Date(priorWb.start + 'T00:00:00Z');
    priorEnd.setUTCDate(priorEnd.getUTCDate() + daysElapsed - 1);
    const priorEndStr = fmtDate(priorEnd);

    // Parallel: Clicky + Labby + WP + DB queries
    const [vData, pvData, fbData, pfData, wpPosts] = await Promise.all([
      clickyFetch('visitors', { date: `${wb.start},${currentEnd}` }),
      clickyFetch('visitors', { date: `${priorWb.start},${priorEndStr}` }),
      fetchLabby(wb.start, currentEnd),
      fetchLabby(priorWb.start, priorEndStr),
      fetch(`https://www.headphonesty.com/wp-json/wp/v2/posts?per_page=100&after=${wb.start}T00:00:00&before=${wb.end}T23:59:59&_fields=id,date,title,link&status=publish&orderby=date&order=desc`).then(r => r.json()).catch(() => []),
    ]);

    const visitors = parseInt(vData[0]?.dates?.[0]?.items?.[0]?.value || '0');
    const priorVisitors = parseInt(pvData[0]?.dates?.[0]?.items?.[0]?.value || '0');
    const fbReach = fbData.totalReach || 0;
    const fbClicks = fbData.totalFacebookLinkClicks || 0;
    const priorReach = pfData.totalReach || 0;
    const priorClicks = pfData.totalFacebookLinkClicks || 0;
    const articlesCount = Array.isArray(wpPosts) ? wpPosts.length : 0;

    // DB queries
    const topPosts = db.prepare(`SELECT id, ${SGT_DAY_EXPR} as day, post_type, reach, shares, comments, link_url, substr(message,1,120) as msg FROM posts WHERE ${SGT_DAY_EXPR} >= ? AND ${SGT_DAY_EXPR} <= ? ORDER BY reach DESC LIMIT 10`).all(wb.start, wb.end);
    const mix = db.prepare(`SELECT post_type, COUNT(*) as cnt, ROUND(AVG(reach)) as avg_reach FROM posts WHERE ${SGT_DAY_EXPR} >= ? AND ${SGT_DAY_EXPR} <= ? GROUP BY post_type ORDER BY avg_reach DESC`).all(wb.start, currentEnd);
    const dailyPosts = db.prepare(`SELECT ${SGT_DAY_EXPR} as day, COUNT(*) as posts FROM posts WHERE ${SGT_DAY_EXPR} >= ? AND ${SGT_DAY_EXPR} <= ? GROUP BY day ORDER BY day`).all(wb.start, currentEnd);

    // 30-day trends
    const sgt30 = new Date(nowSGT()); sgt30.setUTCDate(sgt30.getUTCDate() - 30);
    const daily30 = db.prepare(`SELECT ${SGT_DAY_EXPR} as day, COUNT(*) as posts, SUM(reach) as total_reach, SUM(shares) as total_shares, SUM(comments) as total_comments FROM posts WHERE ${SGT_DAY_EXPR} >= ? AND ${SGT_DAY_EXPR} <= ? GROUP BY day ORDER BY day`).all(fmtDate(sgt30), currentEnd);

    db.close();

    // Format context
    const visitorsProj = Math.round(visitors / daysElapsed * 7);
    const reachProj = Math.round(fbReach / daysElapsed * 7);
    const clicksProj = Math.round(fbClicks / daysElapsed * 7);

    const dayNames = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];

    let ctx = `## Current Week (${wb.start} to ${wb.end}) — Day ${daysElapsed} (${dayNames[daysElapsed - 1]})\n\n`;
    ctx += `### Scorecards (vs same days prior week)\n`;
    ctx += `- Visitors: ${visitors.toLocaleString()} (proj: ${visitorsProj.toLocaleString()}/wk) | Target: ${WEEKLY_TARGETS.visitors.toLocaleString()} | Prior: ${priorVisitors.toLocaleString()} (${priorVisitors ? ((visitors - priorVisitors) / priorVisitors * 100).toFixed(1) : '—'}%)\n`;
    ctx += `- FB Reach: ${fbReach.toLocaleString()} (proj: ${reachProj.toLocaleString()}/wk) | Target: ${WEEKLY_TARGETS.fb_reach.toLocaleString()} | Prior: ${priorReach.toLocaleString()} (${priorReach ? ((fbReach - priorReach) / priorReach * 100).toFixed(1) : '—'}%)\n`;
    ctx += `- FB Clicks: ${fbClicks.toLocaleString()} (proj: ${clicksProj.toLocaleString()}/wk) | Target: ${WEEKLY_TARGETS.fb_clicks.toLocaleString()} | Prior: ${priorClicks.toLocaleString()} (${priorClicks ? ((fbClicks - priorClicks) / priorClicks * 100).toFixed(1) : '—'}%)\n`;
    ctx += `- Articles Published: ${articlesCount} / 15 target\n\n`;

    ctx += `### Content Mix This Week\n`;
    const totalMix = mix.reduce((s, m) => s + m.cnt, 0);
    mix.forEach(m => { ctx += `- ${m.post_type}: ${m.cnt} posts (${(m.cnt / totalMix * 100).toFixed(0)}%) — avg reach ${m.avg_reach}\n`; });
    ctx += `\n`;

    ctx += `### Daily FB Posting (this week)\n`;
    dailyPosts.forEach(d => { ctx += `- ${d.day}: ${d.posts} posts ${d.posts >= 16 ? '✅' : d.posts >= 11 ? '⚠️' : '🔴'}\n`; });
    ctx += `Target: 16 posts/day\n\n`;

    ctx += `### Top 10 Posts This Week (by reach)\n`;
    topPosts.forEach((p, i) => { ctx += `${i + 1}. [${p.post_type}] reach=${p.reach} shares=${p.shares} comments=${p.comments} — "${(p.msg || '').slice(0, 80)}"\n`; });
    ctx += `\n`;

    ctx += `### 30-Day Trends (last 7 days vs prior 7)\n`;
    if (daily30.length >= 14) {
      const last7 = daily30.slice(-7);
      const prior7 = daily30.slice(-14, -7);
      const l7reach = last7.reduce((s, d) => s + (d.total_reach || 0), 0);
      const p7reach = prior7.reduce((s, d) => s + (d.total_reach || 0), 0);
      const l7shares = last7.reduce((s, d) => s + (d.total_shares || 0), 0);
      const p7shares = prior7.reduce((s, d) => s + (d.total_shares || 0), 0);
      ctx += `- Reach: last 7d ${l7reach.toLocaleString()} vs prior 7d ${p7reach.toLocaleString()} (${p7reach ? ((l7reach - p7reach) / p7reach * 100).toFixed(1) : '—'}%)\n`;
      ctx += `- Shares: last 7d ${l7shares.toLocaleString()} vs prior 7d ${p7shares.toLocaleString()} (${p7shares ? ((l7shares - p7shares) / p7shares * 100).toFixed(1) : '—'}%)\n`;
    }

    return ctx;
  } catch (e) {
    if (db) db.close();
    return `Error gathering data: ${e.message}`;
  }
}

app.get('/api/insights', async (req, res) => {
  const forceRefresh = req.query.refresh === 'true';

  // Return cached if fresh
  if (!forceRefresh && insightsCache.data && Date.now() - insightsCache.ts < INSIGHTS_TTL) {
    return res.json({ ...insightsCache.data, cached: true, generatedAt: new Date(insightsCache.ts).toISOString() });
  }

  // If already generating, return status
  if (insightsCache.generating) {
    return res.json({ status: 'generating', message: 'Analysis in progress...' });
  }

  // Check API key
  if (!process.env.ANTHROPIC_API_KEY) {
    return res.status(500).json({ error: 'ANTHROPIC_API_KEY not configured' });
  }

  insightsCache.generating = true;

  try {
    const context = await gatherDashboardContext();
    const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

    const message = await client.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 1024,
      messages: [{
        role: 'user',
        content: `You are lil zucc, Headphonesty's Facebook performance analyst. You're direct, data-driven, slightly snarky, and obsessed with what the algo rewards. You speak in short punchy sentences with numbers up front.

Analyze the following dashboard data and give a brief status report (4-6 bullet points max). Focus on:
1. Are we on track for weekly targets? What's ahead/behind?
2. Any concerning trends or patterns?
3. What's working well right now?
4. One actionable recommendation

Keep it SHORT and punchy — this appears in a small widget. No fluff. Numbers first.

Use basic HTML for formatting (<b>, <br>, bullet points as • lines). No markdown.

${context}`
      }]
    });

    const analysis = message.content[0]?.text || 'No analysis generated.';

    insightsCache = {
      ts: Date.now(),
      data: { analysis, status: 'ok' },
      generating: false
    };

    res.json({ analysis, status: 'ok', cached: false, generatedAt: new Date().toISOString() });
  } catch (e) {
    insightsCache.generating = false;
    console.error('Insights error:', e);
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/health', (req, res) => {
  const sgt = nowSGT();
  res.json({ status: 'ok', db: fs.existsSync(DB_PATH), timezone: 'SGT (UTC+8)', serverUTC: new Date().toISOString(), sgt: fmtDate(sgt) + 'T' + sgt.toISOString().slice(11) });
});

app.use(express.static(path.join(__dirname, '..', 'public')));
app.get('*', (req, res) => res.sendFile(path.join(__dirname, '..', 'public', 'index.html')));

app.listen(PORT, '0.0.0.0', () => console.log(`Dashboard v2 on port ${PORT}`));
