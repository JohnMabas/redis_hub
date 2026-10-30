import { client } from './redis.js';

const NOT_FOUND = '__null__'; // stored when the database says "no such record"
const NOT_FOUND_TTL = 30; // keep "not found" briefly so bad IDs don't hammer the database
const LOCK_TTL = 10; // seconds

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Add up to 10% random extra time so keys created together don't all expire together.
const withJitter = (ttl) => ttl + Math.floor(Math.random() * ttl * 0.1);

const decode = (raw) => (raw === NOT_FOUND ? null : JSON.parse(raw));

/**
 * Cache-aside helper.
 *
 *   1. Look in Redis.            -> hit:  return it
 *   2. Miss: take a short lock so only ONE request rebuilds the value.
 *   3. Call fetchFn() (the slow database/API call), store the result with a TTL, return it.
 *
 * @param {string}   key      e.g. 'cache:product:42'
 * @param {number}   ttl      seconds the value stays cached
 * @param {Function} fetchFn  async () => value. Return null/undefined if the record doesn't exist.
 */
export async function getOrSetCache(key, ttl, fetchFn) {
  // 1. Try the cache. If Redis itself is down, fall back to the database instead of failing.
  let cached;
  try {
    cached = await client.get(key);
  } catch (err) {
    console.error(`[cache] Redis unavailable, going straight to the source: ${err.message}`);
    return fetchFn();
  }

  if (cached !== null) {
    console.log(`[cache] HIT  ${key}`);
    return decode(cached);
  }

  console.log(`[cache] MISS ${key}`);

  // 2. Stampede protection: NX means "only set if it doesn't exist", so exactly one caller wins.
  const lockKey = `lock:${key}`;
  const gotLock = await client.set(lockKey, '1', { NX: true, EX: LOCK_TTL });

  if (!gotLock) {
    // Someone else is rebuilding this value. Wait for it instead of hitting the database too.
    for (let i = 0; i < 20; i++) {
      await sleep(50);
      const retry = await client.get(key);
      if (retry !== null) return decode(retry);
    }
    // Still empty after ~1s: stop waiting and fetch it ourselves.
  }

  // 3. Fetch, store, return.
  try {
    const value = await fetchFn();

    if (value === null || value === undefined) {
      await client.set(key, NOT_FOUND, { EX: NOT_FOUND_TTL }); // negative caching
      return null;
    }

    await client.set(key, JSON.stringify(value), { EX: withJitter(ttl) });
    return value;
  } finally {
    // Only release a lock we actually took.
    // (Production code also stores a unique token in the lock so it never deletes someone else's.)
    if (gotLock) await client.del(lockKey);
  }
}

/** Remove one cached entry. Call this whenever the underlying data changes. */
export async function invalidateCache(key) {
  return client.del(key);
}

/**
 * Remove every key matching a pattern, e.g. 'cache:product:*'.
 * Uses SCAN (non-blocking), never KEYS. Returns how many keys were deleted.
 */
export async function invalidateByPattern(pattern) {
  let deleted = 0;

  for await (const batch of client.scanIterator({ MATCH: pattern, COUNT: 100 })) {
    // node-redis v5 yields an array of keys per batch; v4 yields one key at a time.
    const keys = Array.isArray(batch) ? batch : [batch];
    if (keys.length > 0) deleted += await client.del(keys);
  }
  return deleted;
}

/** Hit/miss numbers for the whole Redis server, from INFO stats. */
export async function cacheStats() {
  const info = await client.info('stats');
  const read = (name) => Number(info.match(new RegExp(`${name}:(\\d+)`))?.[1] ?? 0);

  const hits = read('keyspace_hits');
  const misses = read('keyspace_misses');
  const total = hits + misses;

  return { hits, misses, hitRate: total ? Number((hits / total).toFixed(3)) : 0 };
}
