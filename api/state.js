const { createClient } = require('redis');

const EMPTY_STATE = {
  current: null,
  lastSeen: null,
  events: [],
  siteCounts: {},
  spotter: {}
};

const MAX_EVENTS = 80;
const MAX_NAME_LENGTH = 24;
const SITES = new Set(['ATL77', 'ATL68', 'ATL73', 'ATL74', 'ATL76', 'Unknown']);
let redisClientPromise;

function redisConfig() {
  return {
    connectionString: process.env.REDIS_URL,
    url: process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL,
    token: process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN
  };
}

async function getRedisClient() {
  const { connectionString } = redisConfig();
  if (!connectionString) {
    throw new Error('Missing REDIS_URL');
  }

  if (!redisClientPromise) {
    const client = createClient({ url: connectionString });
    redisClientPromise = client.connect().then(() => client);
  }

  return redisClientPromise;
}

function todayKey() {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).formatToParts(new Date());
  const get = type => parts.find(part => part.type === type).value;
  return `jt-state:${get('year')}-${get('month')}-${get('day')}`;
}

async function redis(command) {
  const { connectionString, url, token } = redisConfig();

  if (connectionString) {
    const client = await getRedisClient();
    const [name, key, value, option, ttl] = command;
    if (name === 'GET') {
      return client.get(key);
    }
    if (name === 'SET' && option === 'EX') {
      return client.set(key, value, { EX: ttl });
    }
    throw new Error(`Unsupported Redis command: ${name}`);
  }

  if (!url || !token) {
    throw new Error('Missing Redis environment variables');
  }

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(command)
  });

  if (!response.ok) {
    throw new Error(`Redis request failed: ${response.status}`);
  }

  const data = await response.json();
  if (data.error) {
    throw new Error(data.error);
  }
  return data.result;
}

function cleanName(name) {
  const value = String(name || 'Anonymous').trim().slice(0, MAX_NAME_LENGTH);
  return value || 'Anonymous';
}

function cleanText(value, maxLength = 120) {
  return String(value || '').trim().slice(0, maxLength);
}

function sanitizeState(input) {
  const state = { ...EMPTY_STATE, ...(input || {}) };
  const siteCounts = {};
  const spotter = {};

  for (const [site, count] of Object.entries(state.siteCounts || {})) {
    if (SITES.has(site)) {
      siteCounts[site] = Math.max(0, Math.min(Number(count) || 0, 9999));
    }
  }

  for (const [name, count] of Object.entries(state.spotter || {})) {
    spotter[cleanName(name)] = Math.max(0, Math.min(Number(count) || 0, 9999));
  }

  const events = Array.isArray(state.events) ? state.events.slice(0, MAX_EVENTS).map(event => {
    const type = ['spot', 'move', 'gone'].includes(event.type) ? event.type : 'spot';
    return {
      type,
      site: SITES.has(event.site) ? event.site : undefined,
      from: SITES.has(event.from) ? event.from : undefined,
      to: SITES.has(event.to) ? event.to : undefined,
      name: cleanName(event.name),
      ts: Number(event.ts) || Date.now(),
      quip: cleanText(event.quip)
    };
  }) : [];

  const current = SITES.has(state.current) && state.current !== 'Unknown' ? state.current : null;

  return {
    current,
    lastSeen: Number(state.lastSeen) || null,
    events,
    siteCounts,
    spotter
  };
}

async function parseBody(req) {
  if (!req.body || typeof req.body === 'object') {
    return req.body || {};
  }

  try {
    return JSON.parse(req.body);
  } catch (error) {
    return {};
  }
}

module.exports = async function handler(req, res) {
  const key = todayKey();

  try {
    if (req.method === 'GET') {
      const value = await redis(['GET', key]);
      res.setHeader('Cache-Control', 'no-store');
      return res.status(200).json(value ? JSON.parse(value) : EMPTY_STATE);
    }

    if (req.method === 'POST') {
      const nextState = sanitizeState(await parseBody(req));
      await redis(['SET', key, JSON.stringify(nextState), 'EX', 172800]);
      res.setHeader('Cache-Control', 'no-store');
      return res.status(200).json(nextState);
    }

    res.setHeader('Allow', 'GET, POST');
    return res.status(405).json({ error: 'Method not allowed' });
  } catch (error) {
    console.error(error);
    return res.status(500).json({
      error: 'Shared state unavailable',
      detail: process.env.NODE_ENV === 'production' ? undefined : error.message
    });
  }
};
