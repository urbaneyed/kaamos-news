require('dotenv').config();
const express = require('express');
const mysql = require('mysql2/promise');
const RSSParser = require('rss-parser');
const NodeCache = require('node-cache');
const cheerio = require('cheerio');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3001;
const SECRET = process.env.PROXY_SECRET || 'kaamos-news-2026-secret';
const FETCH_INTERVAL = parseInt(process.env.FETCH_INTERVAL_MS || '7200000');

const cache = new NodeCache({ stdTTL: 300 });
const rssParser = new RSSParser({
  timeout: 15000,
  headers: { 'User-Agent': 'KaamOS/1.0 (News Service; +https://sassy.work)' },
  customFields: {
    item: [
      ['media:content', 'mediaContent', { keepArray: false }],
      ['media:thumbnail', 'mediaThumbnail', { keepArray: false }],
    ],
  },
});

// ──────────────────────────────────────────────
//  Database
// ──────────────────────────────────────────────

let pool;

function getPool() {
  if (!pool) {
    pool = mysql.createPool({
      host: process.env.DB_HOST || 'localhost',
      user: process.env.DB_USER,
      password: process.env.DB_PASS,
      database: process.env.DB_NAME,
      waitForConnections: true,
      connectionLimit: 5,
      charset: 'utf8mb4',
    });
  }
  return pool;
}

// ──────────────────────────────────────────────
//  OpenAI helper
// ──────────────────────────────────────────────

let openai = null;
function getOpenAI() {
  if (!openai && process.env.OPENAI_API_KEY) {
    const OpenAI = require('openai');
    openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
  }
  return openai;
}

async function generateAISummary(title, summary, sourceName) {
  const ai = getOpenAI();
  if (!ai) return null;

  const text = summary ? `${title}\n\n${summary.substring(0, 800)}` : title;

  try {
    const resp = await ai.chat.completions.create({
      model: 'gpt-4o-mini',
      max_tokens: 100,
      temperature: 0.3,
      messages: [
        {
          role: 'system',
          content:
            'You are a concise news editor. Write a 1-2 sentence summary (max 200 chars) of this article. Be factual and direct. No opinions. Start with the key fact.',
        },
        { role: 'user', content: text },
      ],
    });
    return resp.choices[0]?.message?.content?.trim() || null;
  } catch (err) {
    console.error('[AI] Summary error:', err.message);
    return null;
  }
}

// ──────────────────────────────────────────────
//  og:image extraction
// ──────────────────────────────────────────────

const UA =
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36';

async function extractOgImage(articleUrl) {
  const timeout = parseInt(process.env.OG_IMAGE_TIMEOUT_MS || '8000');
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);

  try {
    const resp = await fetch(articleUrl, {
      headers: { 'User-Agent': UA },
      signal: controller.signal,
      redirect: 'follow',
    });
    clearTimeout(timer);

    if (!resp.ok) return null;

    const ct = resp.headers.get('content-type') || '';
    if (!ct.includes('text/html')) return null;

    const html = await resp.text();
    const $ = cheerio.load(html.substring(0, 50000)); // Only parse first 50KB

    // Try og:image first, then twitter:image
    let img =
      $('meta[property="og:image"]').attr('content') ||
      $('meta[name="twitter:image"]').attr('content') ||
      $('meta[name="twitter:image:src"]').attr('content') ||
      null;

    // Ensure absolute URL
    if (img && !img.startsWith('http')) {
      try {
        img = new URL(img, articleUrl).href;
      } catch {
        img = null;
      }
    }

    return img;
  } catch {
    clearTimeout(timer);
    return null;
  }
}

// ──────────────────────────────────────────────
//  Article scoring
// ──────────────────────────────────────────────

const SOURCE_TIERS = {
  // Tier 1 — premium global outlets
  'BBC Top Stories': 10,
  'BBC World': 10,
  'BBC Business': 9,
  'BBC Technology': 9,
  'Financial Times': 10,
  Reuters: 10,
  CNBC: 9,
  // Tier 2 — quality tech/business
  TechCrunch: 8,
  'The Verge': 8,
  Wired: 8,
  'Ars Technica': 8,
  'Hacker News': 7,
  'MIT Technology Review': 8,
  VentureBeat: 7,
  'Rest of World': 7,
  // Tier 3 — regional / niche
  'The Guardian World': 8,
  'The Guardian Tech': 7,
  'India Today': 7,
  'NDTV Top Stories': 7,
  'Economic Times - Jobs': 6,
  'LiveMint - Companies': 7,
  Moneycontrol: 7,
  YourStory: 6,
};

function scoreArticle(article, sourceName) {
  let score = 0;

  // Source quality (0-10)
  score += SOURCE_TIERS[sourceName] || 5;

  // Recency — published within last N hours
  if (article.publishedAt) {
    const ageHours = (Date.now() - new Date(article.publishedAt).getTime()) / 3600000;
    if (ageHours < 1) score += 5;
    else if (ageHours < 3) score += 4;
    else if (ageHours < 6) score += 3;
    else if (ageHours < 12) score += 2;
    else if (ageHours < 24) score += 1;
  }

  // Has image
  if (article.imageUrl) score += 1;

  // Title quality — not too short, not all caps
  const title = article.title || '';
  if (title.length >= 20 && title.length <= 150) score += 1;
  if (title === title.toUpperCase() && title.length > 10) score -= 1; // ALL CAPS penalty

  // Has summary
  if (article.summary && article.summary.length > 50) score += 1;

  return Math.max(0, Math.min(20, score)); // clamp 0-20
}

// ──────────────────────────────────────────────
//  RSS Feed Fetching
// ──────────────────────────────────────────────

async function fetchFeed(feedUrl) {
  try {
    const feed = await rssParser.parseURL(feedUrl);
    return feed.items || [];
  } catch (err) {
    console.error(`[RSS] Failed to fetch ${feedUrl}:`, err.message);
    return null;
  }
}

function extractImageFromItem(item) {
  // rss-parser custom fields
  if (item.mediaContent?.$?.url) return item.mediaContent.$.url;
  if (item.mediaThumbnail?.$?.url) return item.mediaThumbnail.$.url;

  // enclosure
  if (item.enclosure?.url && item.enclosure?.type?.startsWith('image/')) {
    return item.enclosure.url;
  }

  // Try to find <img> in content/description
  const html = item['content:encoded'] || item.content || item.contentSnippet || '';
  const match = html.match(/<img[^>]+src=["']([^"']+)["']/);
  if (match) return match[1];

  return null;
}

async function fetchAndProcessSource(db, tenantId, source, options = {}) {
  const { enrichAI = true, extractImages = true } = options;
  const results = { inserted: 0, skipped: 0, enriched: 0, error: '' };

  const items = await fetchFeed(source.feed_url);
  if (items === null) {
    results.error = 'Failed to fetch feed';
    return results;
  }
  if (items.length === 0) {
    results.error = 'No items in feed';
    return results;
  }

  for (const item of items) {
    const url = (item.link || '').trim();
    const title = (item.title || '').trim();
    if (!url || !title) {
      results.skipped++;
      continue;
    }

    // Dedup
    const [existing] = await db.query(
      'SELECT id FROM news_items WHERE url = ? AND tenant_id = ?',
      [url, tenantId]
    );
    if (existing.length > 0) {
      results.skipped++;
      continue;
    }

    // Parse published date
    let publishedAt = null;
    if (item.pubDate || item.isoDate) {
      const d = new Date(item.isoDate || item.pubDate);
      if (!isNaN(d.getTime())) {
        publishedAt = d.toISOString().slice(0, 19).replace('T', ' ');
      }
    }
    if (!publishedAt) {
      publishedAt = new Date().toISOString().slice(0, 19).replace('T', ' ');
    }

    // Skip articles older than 7 days
    if (new Date(publishedAt).getTime() < Date.now() - 7 * 86400000) {
      results.skipped++;
      continue;
    }

    // Clean summary
    let summary = item.contentSnippet || item.content || '';
    summary = summary.replace(/<[^>]*>/g, '').trim();
    if (summary.length > 2000) summary = summary.substring(0, 2000);

    // Image extraction
    let imageUrl = extractImageFromItem(item);

    // og:image fallback (if enabled and no RSS image)
    if (!imageUrl && extractImages) {
      imageUrl = await extractOgImage(url);
    }

    // AI summary
    let aiLead = null;
    if (enrichAI) {
      aiLead = await generateAISummary(title, summary, source.name);
    }

    // Score
    const articleData = {
      title,
      summary,
      imageUrl,
      publishedAt,
    };
    const artScore = scoreArticle(articleData, source.name);

    try {
      await db.query(
        `INSERT INTO news_items
          (tenant_id, source_id, title, url, summary, image_url, category, source_name,
           ai_lead, score, published_at, fetched_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())`,
        [
          tenantId,
          source.id,
          title.substring(0, 500),
          url.substring(0, 500),
          summary || null,
          imageUrl || null,
          source.category || 'General',
          source.name,
          aiLead,
          artScore,
          publishedAt,
        ]
      );
      results.inserted++;
      if (aiLead) results.enriched++;
    } catch (err) {
      results.skipped++;
    }
  }

  return results;
}

// ──────────────────────────────────────────────
//  Batch enrich — add AI summaries + images
//  to existing articles that are missing them
// ──────────────────────────────────────────────

async function enrichExistingArticles(db, tenantId, limit = 20) {
  const batchSize = parseInt(process.env.ENRICH_BATCH_SIZE || '10');
  const delay = parseInt(process.env.ENRICH_DELAY_MS || '500');

  // Articles from last 3 days without ai_lead
  const [rows] = await db.query(
    `SELECT id, title, summary, url, image_url
     FROM news_items
     WHERE tenant_id = ? AND ai_lead IS NULL
       AND published_at > DATE_SUB(NOW(), INTERVAL 3 DAY)
     ORDER BY published_at DESC
     LIMIT ?`,
    [tenantId, limit]
  );

  let enriched = 0;
  let imagesAdded = 0;

  for (const row of rows) {
    const updates = {};

    // AI summary
    const aiLead = await generateAISummary(row.title, row.summary, '');
    if (aiLead) {
      updates.ai_lead = aiLead;
      enriched++;
    }

    // og:image if missing
    if (!row.image_url) {
      const img = await extractOgImage(row.url);
      if (img) {
        updates.image_url = img;
        imagesAdded++;
      }
    }

    if (Object.keys(updates).length > 0) {
      const setClauses = Object.keys(updates)
        .map((k) => `${k} = ?`)
        .join(', ');
      await db.query(`UPDATE news_items SET ${setClauses} WHERE id = ?`, [
        ...Object.values(updates),
        row.id,
      ]);
    }

    // Rate limit
    await new Promise((r) => setTimeout(r, delay));
  }

  return { total: rows.length, enriched, imagesAdded };
}

// ──────────────────────────────────────────────
//  Auth middleware
// ──────────────────────────────────────────────

function authMiddleware(req, res, next) {
  const key = req.headers['x-proxy-key'] || req.query.secret || '';
  if (key !== SECRET) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  next();
}

// ──────────────────────────────────────────────
//  API Routes
// ──────────────────────────────────────────────

// Health check
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    service: 'kaamos-news',
    uptime: process.uptime(),
    sources: cache.get('source_count') || 'unknown',
  });
});

// ── Fetch all sources for a tenant ──
app.post('/api/fetch', authMiddleware, async (req, res) => {
  const tenantId = parseInt(req.body.tenant_id || req.query.tenant_id || '1');
  const enrichAI = req.body.enrich !== false;
  const extractImages = req.body.images !== false;

  try {
    const db = getPool();
    const [sources] = await db.query(
      'SELECT * FROM news_sources WHERE tenant_id = ? AND is_active = 1',
      [tenantId]
    );

    if (sources.length === 0) {
      return res.json({ message: 'No active sources', inserted: 0 });
    }

    cache.set('source_count', sources.length);

    // Process sources in small batches (2 at a time to be gentle on shared hosting)
    const batchSize = 2;
    let totalInserted = 0;
    let totalSkipped = 0;
    let totalEnriched = 0;
    const errors = [];

    for (let i = 0; i < sources.length; i += batchSize) {
      const batch = sources.slice(i, i + batchSize);
      const results = await Promise.all(
        batch.map((src) =>
          fetchAndProcessSource(db, tenantId, src, { enrichAI, extractImages })
        )
      );

      // Small delay between batches to avoid overwhelming shared hosting
      if (i + batchSize < sources.length) {
        await new Promise((r) => setTimeout(r, 1000));
      }

      for (let j = 0; j < results.length; j++) {
        const r = results[j];
        totalInserted += r.inserted;
        totalSkipped += r.skipped;
        totalEnriched += r.enriched;
        if (r.error) errors.push(`${batch[j].name}: ${r.error}`);

        // Update last_fetched_at
        await db.query('UPDATE news_sources SET last_fetched_at = NOW() WHERE id = ?', [
          batch[j].id,
        ]);
      }
    }

    res.json({
      sources: sources.length,
      inserted: totalInserted,
      skipped: totalSkipped,
      enriched: totalEnriched,
      errors,
    });
  } catch (err) {
    console.error('[Fetch] Error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── Fetch single source ──
app.post('/api/fetch/:sourceId', authMiddleware, async (req, res) => {
  const sourceId = parseInt(req.params.sourceId);
  const tenantId = parseInt(req.body.tenant_id || '1');

  try {
    const db = getPool();
    const [sources] = await db.query('SELECT * FROM news_sources WHERE id = ? AND tenant_id = ?', [
      sourceId,
      tenantId,
    ]);

    if (sources.length === 0) {
      return res.status(404).json({ error: 'Source not found' });
    }

    const result = await fetchAndProcessSource(db, tenantId, sources[0], {
      enrichAI: req.body.enrich !== false,
      extractImages: req.body.images !== false,
    });

    await db.query('UPDATE news_sources SET last_fetched_at = NOW() WHERE id = ?', [sourceId]);

    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Enrich existing articles (AI summaries + og:images) ──
app.post('/api/enrich', authMiddleware, async (req, res) => {
  const tenantId = parseInt(req.body.tenant_id || '1');
  const limit = parseInt(req.body.limit || '20');

  try {
    const db = getPool();
    const result = await enrichExistingArticles(db, tenantId, limit);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── List sources ──
app.get('/api/sources', authMiddleware, async (req, res) => {
  const tenantId = parseInt(req.query.tenant_id || '1');

  try {
    const db = getPool();
    const [sources] = await db.query(
      `SELECT id, name, feed_url, category, is_active, is_default,
              last_fetched_at, created_at
       FROM news_sources
       WHERE tenant_id = ?
       ORDER BY is_default DESC, category, name`,
      [tenantId]
    );
    res.json(sources);
  } catch (err) {
    // is_default column may not exist yet
    try {
      const db = getPool();
      const [sources] = await db.query(
        `SELECT id, name, feed_url, category, is_active,
                last_fetched_at, created_at
         FROM news_sources
         WHERE tenant_id = ?
         ORDER BY category, name`,
        [tenantId]
      );
      res.json(sources);
    } catch (err2) {
      res.status(500).json({ error: err2.message });
    }
  }
});

// ── Add custom source ──
app.post('/api/sources', authMiddleware, async (req, res) => {
  const { tenant_id = 1, name, feed_url, category = 'Custom' } = req.body;

  if (!name || !feed_url) {
    return res.status(400).json({ error: 'name and feed_url required' });
  }

  // Validate feed URL — try to parse it
  try {
    const items = await fetchFeed(feed_url);
    if (items === null) {
      return res.status(400).json({ error: 'Could not parse RSS feed at that URL' });
    }

    const db = getPool();
    const [result] = await db.query(
      `INSERT INTO news_sources (tenant_id, name, feed_url, category, is_active, is_default)
       VALUES (?, ?, ?, ?, 1, 0)`,
      [tenant_id, name, feed_url, category]
    );

    res.json({
      id: result.insertId,
      name,
      feed_url,
      category,
      items_found: items.length,
      message: `Source added. Found ${items.length} items in feed.`,
    });
  } catch (err) {
    // Retry without is_default column
    try {
      const db = getPool();
      const [result] = await db.query(
        `INSERT INTO news_sources (tenant_id, name, feed_url, category, is_active)
         VALUES (?, ?, ?, ?, 1)`,
        [tenant_id, name, feed_url, category]
      );
      res.json({ id: result.insertId, name, feed_url, category });
    } catch (err2) {
      res.status(500).json({ error: err2.message });
    }
  }
});

// ── Update source ──
app.put('/api/sources/:id', authMiddleware, async (req, res) => {
  const id = parseInt(req.params.id);
  const { name, feed_url, category, is_active } = req.body;
  const tenantId = parseInt(req.body.tenant_id || '1');

  try {
    const db = getPool();
    const updates = [];
    const params = [];

    if (name !== undefined) { updates.push('name = ?'); params.push(name); }
    if (feed_url !== undefined) { updates.push('feed_url = ?'); params.push(feed_url); }
    if (category !== undefined) { updates.push('category = ?'); params.push(category); }
    if (is_active !== undefined) { updates.push('is_active = ?'); params.push(is_active ? 1 : 0); }

    if (updates.length === 0) {
      return res.status(400).json({ error: 'No fields to update' });
    }

    params.push(id, tenantId);
    await db.query(
      `UPDATE news_sources SET ${updates.join(', ')} WHERE id = ? AND tenant_id = ?`,
      params
    );

    res.json({ message: 'Source updated' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Delete source ──
app.delete('/api/sources/:id', authMiddleware, async (req, res) => {
  const id = parseInt(req.params.id);
  const tenantId = parseInt(req.query.tenant_id || '1');

  try {
    const db = getPool();

    // Don't delete default sources
    const [source] = await db.query(
      'SELECT is_default FROM news_sources WHERE id = ? AND tenant_id = ?',
      [id, tenantId]
    );

    if (source.length === 0) {
      return res.status(404).json({ error: 'Source not found' });
    }

    // Allow deletion regardless (is_default column may not exist)
    await db.query('DELETE FROM news_sources WHERE id = ? AND tenant_id = ?', [id, tenantId]);

    res.json({ message: 'Source deleted' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Trending / top articles ──
app.get('/api/trending', authMiddleware, async (req, res) => {
  const tenantId = parseInt(req.query.tenant_id || '1');
  const category = req.query.category || null;
  const limit = parseInt(req.query.limit || '20');
  const hours = parseInt(req.query.hours || '24');

  const cacheKey = `trending_${tenantId}_${category}_${limit}_${hours}`;
  const cached = cache.get(cacheKey);
  if (cached) return res.json(cached);

  try {
    const db = getPool();
    let query = `
      SELECT id, title, url, summary, image_url, category, source_name,
             ai_lead, score, published_at
      FROM news_items
      WHERE tenant_id = ?
        AND published_at > DATE_SUB(NOW(), INTERVAL ? HOUR)
    `;
    const params = [tenantId, hours];

    if (category) {
      query += ' AND category = ?';
      params.push(category);
    }

    query += ' ORDER BY score DESC, published_at DESC LIMIT ?';
    params.push(limit);

    const [rows] = await db.query(query, params);
    cache.set(cacheKey, rows, 120); // 2 min cache
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Categories available ──
app.get('/api/categories', authMiddleware, async (req, res) => {
  const tenantId = parseInt(req.query.tenant_id || '1');

  try {
    const db = getPool();
    const [rows] = await db.query(
      `SELECT DISTINCT category, COUNT(*) as count
       FROM news_sources WHERE tenant_id = ? AND is_active = 1
       GROUP BY category ORDER BY count DESC`,
      [tenantId]
    );
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Stats ──
app.get('/api/stats', authMiddleware, async (req, res) => {
  const tenantId = parseInt(req.query.tenant_id || '1');

  try {
    const db = getPool();
    const [[{ total }]] = await db.query(
      'SELECT COUNT(*) as total FROM news_items WHERE tenant_id = ?',
      [tenantId]
    );
    const [[{ today }]] = await db.query(
      'SELECT COUNT(*) as today FROM news_items WHERE tenant_id = ? AND fetched_at > CURDATE()',
      [tenantId]
    );
    const [[{ with_ai }]] = await db.query(
      'SELECT COUNT(*) as with_ai FROM news_items WHERE tenant_id = ? AND ai_lead IS NOT NULL',
      [tenantId]
    );
    const [[{ with_image }]] = await db.query(
      'SELECT COUNT(*) as with_image FROM news_items WHERE tenant_id = ? AND image_url IS NOT NULL',
      [tenantId]
    );
    const [[{ sources }]] = await db.query(
      'SELECT COUNT(*) as sources FROM news_sources WHERE tenant_id = ? AND is_active = 1',
      [tenantId]
    );

    res.json({ total, today, with_ai, with_image, sources });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Validate a feed URL (for admin UI) ──
app.post('/api/validate-feed', authMiddleware, async (req, res) => {
  const { feed_url } = req.body;
  if (!feed_url) return res.status(400).json({ error: 'feed_url required' });

  try {
    const items = await fetchFeed(feed_url);
    if (items === null) {
      return res.json({ valid: false, error: 'Could not parse feed' });
    }

    // Show first 3 items as preview
    const preview = items.slice(0, 3).map((i) => ({
      title: i.title,
      link: i.link,
      date: i.pubDate || i.isoDate || null,
    }));

    res.json({
      valid: true,
      item_count: items.length,
      preview,
    });
  } catch (err) {
    res.json({ valid: false, error: err.message });
  }
});

// ──────────────────────────────────────────────
//  Scheduled fetching
// ──────────────────────────────────────────────

let fetchTimer = null;

async function scheduledFetch() {
  console.log(`[Scheduler] Starting fetch cycle at ${new Date().toISOString()}`);
  try {
    const db = getPool();

    // Get all tenants that have active sources
    const [tenants] = await db.query(
      'SELECT DISTINCT tenant_id FROM news_sources WHERE is_active = 1'
    );

    for (const { tenant_id } of tenants) {
      const [sources] = await db.query(
        'SELECT * FROM news_sources WHERE tenant_id = ? AND is_active = 1',
        [tenant_id]
      );

      let totalInserted = 0;

      // Sequential processing — one source at a time to be gentle on shared hosting
      for (const src of sources) {
        try {
          const result = await fetchAndProcessSource(db, tenant_id, src, {
            enrichAI: !!process.env.OPENAI_API_KEY,
            extractImages: false, // Skip og:image in scheduled runs to reduce outbound requests
          });
          totalInserted += result.inserted;
          await db.query('UPDATE news_sources SET last_fetched_at = NOW() WHERE id = ?', [src.id]);
        } catch (err) {
          console.error(`[Scheduler] Source ${src.name} error:`, err.message);
        }

        // 2 second delay between sources
        await new Promise((r) => setTimeout(r, 2000));
      }

      console.log(
        `[Scheduler] Tenant ${tenant_id}: ${sources.length} sources, ${totalInserted} new articles`
      );
    }
  } catch (err) {
    console.error('[Scheduler] Error:', err.message);
  }
}

// ──────────────────────────────────────────────
//  Start server
// ──────────────────────────────────────────────

app.listen(PORT, () => {
  console.log(`KaamOS News Service running on port ${PORT}`);
  console.log(`Fetch interval: ${FETCH_INTERVAL / 60000} minutes`);

  // Scheduled fetching — no auto-fetch on startup (use POST /api/fetch to trigger manually)
  // Scheduled interval starts after 5 minutes to let server stabilize
  if (FETCH_INTERVAL > 0) {
    setTimeout(() => {
      fetchTimer = setInterval(scheduledFetch, FETCH_INTERVAL);
      console.log(`[Scheduler] Auto-fetch enabled every ${FETCH_INTERVAL / 60000} minutes`);
    }, 300000);
  }
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  if (fetchTimer) clearInterval(fetchTimer);
  if (pool) await pool.end();
  process.exit(0);
});
