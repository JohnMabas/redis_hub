// Runs the whole lesson in the terminal, no server needed.
//   npm run demo
import { connectRedis, closeRedis, client } from './redis.js';
import {
  createSession,
  getSession,
  incrementSessionField,
  listUserSessions,
  destroySession,
  destroyAllUserSessions,
} from './session.js';
import { getOrSetCache, invalidateCache, invalidateByPattern } from './cache.js';
import { findProduct } from './fakeDb.js';

const heading = (text) => console.log(`\n=== ${text} ===`);

await connectRedis();
console.log('PING ->', await client.ping());

// ---------- Sessions ----------
heading('SESSIONS');

const sid = await createSession('ada', { username: 'Ada', role: 'student', cart: 0 });
console.log('Created session:', sid.slice(0, 12) + '...');
console.log('TTL (seconds):', await client.ttl(`session:${sid}`));

console.log('Read back:', await getSession(sid));
console.log('Cart after add:', await incrementSessionField(sid, 'cart', 1));

const phoneSid = await createSession('ada', { username: 'Ada', device: 'phone' });
console.log('Ada is now logged in on', (await listUserSessions('ada')).length, 'devices');

await destroySession(sid);
console.log('After logout, getSession ->', await getSession(sid));

const count = await destroyAllUserSessions('ada');
console.log(`Logged out of all devices (${count} session left to remove)`);
console.log('Phone session now ->', await getSession(phoneSid));

// ---------- Caching ----------
heading('CACHING');
await invalidateByPattern('cache:product:*'); // start clean

for (const attempt of ['1st (expect MISS, slow)', '2nd (expect HIT, fast)']) {
  const started = Date.now();
  const product = await getOrSetCache('cache:product:1', 300, () => findProduct(1));
  console.log(`${attempt}: ${product.name} in ${Date.now() - started}ms`);
}
console.log('TTL on cache entry:', await client.ttl('cache:product:1'));

console.log('\nInvalidating (as if the product was edited)...');
await invalidateCache('cache:product:1');
const started = Date.now();
await getOrSetCache('cache:product:1', 300, () => findProduct(1));
console.log(`After invalidation: MISS again, ${Date.now() - started}ms`);

console.log('\nAsking for a product that does not exist (negative caching):');
console.log('1st ->', await getOrSetCache('cache:product:999', 300, () => findProduct(999)));
console.log('2nd ->', await getOrSetCache('cache:product:999', 300, () => findProduct(999)));
console.log('"not found" cached for', await client.ttl('cache:product:999'), 'seconds');

await invalidateByPattern('cache:product:*');
await closeRedis();
console.log('\nDone.');
