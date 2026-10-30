import crypto from 'node:crypto';
import { client } from './redis.js';

/*
 * Rate limiting = "at most N requests per time window, per user".
 * The counter lives in Redis so every server instance shares it.
 *
 * Two algorithms:
 *   fixed window   - INCR a counter that expires. Simple and cheap, but allows a burst at the
 *                    window edge (limit at 0:59 + limit at 1:01 = 2x limit in two seconds).
 *   sliding window - remember each request's timestamp in a sorted set. Exact, uses more memory.
 *
 * Both run as Lua scripts. Redis runs a script as ONE step, so two requests can never
 * slip between "read the count" and "write the count" (the race you get with separate commands).
 */

// KEYS[1]=counter  ARGV[1]=window seconds
const FIXED_WINDOW = `
local count = redis.call('INCR', KEYS[1])
if count == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end
local ttl = redis.call('TTL', KEYS[1])
if ttl < 0 then redis.call('EXPIRE', KEYS[1], ARGV[1]); ttl = tonumber(ARGV[1]) end
return {count, ttl}
`;

// KEYS[1]=sorted set  ARGV[1]=window ms  ARGV[2]=limit  ARGV[3]=unique member
// Uses Redis's own clock (TIME) so app servers with different clocks agree.
const SLIDING_WINDOW = `
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local window = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])

redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now - window)
local count = redis.call('ZCARD', KEYS[1])

if count < limit then
  redis.call('ZADD', KEYS[1], now, ARGV[3])
  redis.call('PEXPIRE', KEYS[1], window)
  return {1, limit - count - 1, 0}
end

local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
return {0, 0, window - (now - tonumber(oldest[2]))}
`;

/** Fixed window check. Returns { allowed, limit, remaining, retryAfterSec } */
export async function fixedWindow(key, limit, windowSec) {
  const [count, ttl] = await client.eval(FIXED_WINDOW, { keys: [key], arguments: [String(windowSec)] });
  return {
    allowed: count <= limit,
    limit,
    remaining: Math.max(limit - count, 0),
    retryAfterSec: count <= limit ? 0 : ttl,
  };
}

/** Sliding window check. Returns { allowed, limit, remaining, retryAfterSec } */
export async function slidingWindow(key, limit, windowSec) {
  const member = `${Date.now()}-${crypto.randomUUID()}`; // must be unique or two requests collapse into one
  const [ok, remaining, retryMs] = await client.eval(SLIDING_WINDOW, {
    keys: [key],
    arguments: [String(windowSec * 1000), String(limit), member],
  });
  return { allowed: ok === 1, limit, remaining, retryAfterSec: Math.ceil(retryMs / 1000) };
}

// Logged-in users are limited per user, everyone else per IP.
// Behind a proxy or load balancer, set app.set('trust proxy', 1) or every user shares the proxy's IP.
const defaultIdentity = (req) => (req.session?.userId ? `user:${req.session.userId}` : `ip:${req.ip}`);

/**
 * Express middleware.
 *
 *   app.use(rateLimit({ name: 'api', limit: 60, windowSec: 60 }));
 *   app.post('/login', rateLimit({ name: 'login', limit: 5, windowSec: 60, identify: (req) => `ip:${req.ip}` }), ...)
 *
 * Adds X-RateLimit-* headers and answers 429 with Retry-After when the limit is hit.
 */
export function rateLimit({ name, limit, windowSec, algorithm = 'fixed', identify = defaultIdentity, failOpen = true }) {
  const check = algorithm === 'sliding' ? slidingWindow : fixedWindow;

  return async (req, res, next) => {
    try {
      const result = await check(`ratelimit:${name}:${identify(req)}`, limit, windowSec);

      res.set('X-RateLimit-Limit', String(result.limit));
      res.set('X-RateLimit-Remaining', String(result.remaining));

      if (!result.allowed) {
        res.set('Retry-After', String(result.retryAfterSec));
        return res.status(429).json({ error: 'Too many requests', retryAfterSec: result.retryAfterSec });
      }
      next();
    } catch (err) {
      // Redis down. failOpen: let traffic through so a cache outage doesn't become a site outage.
      // For something abuse-sensitive like login, pass failOpen: false to reject instead.
      console.error(`[ratelimit:${name}] Redis error:`, err.message);
      if (failOpen) return next();
      res.status(503).json({ error: 'Service temporarily unavailable' });
    }
  };
}
