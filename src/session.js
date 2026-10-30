import crypto from 'node:crypto';
import { client } from './redis.js';

export const SESSION_TTL = 1800; // 30 minutes, in seconds

const sessionKey = (id) => `session:${id}`;
const userSessionsKey = (userId) => `user:${userId}:sessions`;

/**
 * Create a session for a user. Returns the new session ID.
 * Redis: HSET + EXPIRE (the session) and SADD (the per-user index), sent together.
 */
export async function createSession(userId, data = {}) {
  // 32 random bytes = 64 hex chars. Never use a guessable ID (counter, user id, timestamp).
  const id = crypto.randomBytes(32).toString('hex');
  const key = sessionKey(id);

  // Hash fields are strings, so convert everything.
  const fields = { userId: String(userId), createdAt: String(Date.now()) };
  for (const [k, v] of Object.entries(data)) fields[k] = String(v);

  await client
    .multi() // MULTI/EXEC: all three commands run as one unit
    .hSet(key, fields)
    .expire(key, SESSION_TTL)
    .sAdd(userSessionsKey(userId), id)
    .exec();

  return id;
}

/**
 * Look up a session and slide its expiry forward.
 * Returns the session object, or null if it doesn't exist or has expired.
 */
export async function getSession(id) {
  if (!id) return null;
  const key = sessionKey(id);

  const [data] = await client
    .multi()
    .hGetAll(key)
    .expire(key, SESSION_TTL) // sliding expiration: every request resets the clock
    .exec();

  if (!data || Object.keys(data).length === 0) return null; // missing/expired key = empty hash
  return { id, ...data };
}

/** Change fields on an existing session. Returns false if the session is gone. */
export async function updateSession(id, fields) {
  const key = sessionKey(id);
  if (!(await client.exists(key))) return false;

  const strings = {};
  for (const [k, v] of Object.entries(fields)) strings[k] = String(v);
  await client.hSet(key, strings);
  return true;
}

/** Atomically add to a numeric field, e.g. the cart count. Returns the new value. */
export async function incrementSessionField(id, field, by = 1) {
  const key = sessionKey(id);
  if (!(await client.exists(key))) return null;
  return client.hIncrBy(key, field, by);
}

/** Log out one session (one device). */
export async function destroySession(id) {
  const key = sessionKey(id);
  const userId = await client.hGet(key, 'userId');

  const multi = client.multi().del(key);
  if (userId) multi.sRem(userSessionsKey(userId), id);
  await multi.exec();
}

/**
 * List a user's live sessions (their logged-in devices).
 * Set members never expire on their own, so we clean out dead IDs as we go.
 */
export async function listUserSessions(userId) {
  const ids = await client.sMembers(userSessionsKey(userId));
  const live = [];

  for (const id of ids) {
    const data = await client.hGetAll(sessionKey(id));
    if (Object.keys(data).length === 0) {
      await client.sRem(userSessionsKey(userId), id); // session expired: remove stale ID
    } else {
      live.push({ id, ...data });
    }
  }
  return live;
}

/** Log out of every device. */
export async function destroyAllUserSessions(userId) {
  const indexKey = userSessionsKey(userId);
  const ids = await client.sMembers(indexKey);

  const multi = client.multi();
  if (ids.length > 0) multi.del(ids.map(sessionKey));
  multi.del(indexKey);
  await multi.exec();

  return ids.length;
}
