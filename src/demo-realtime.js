// Pub/Sub, Streams and rate limiting in the terminal, no server needed.
//   npm run demo:realtime
import { connectRedis, closeRedis, client } from './redis.js';
import { publish, subscribe, psubscribe, closePubSub } from './pubsub.js';
import { addEvent, ensureGroup, startConsumer } from './streams.js';
import { fixedWindow, slidingWindow } from './rateLimit.js';

const heading = (text) => console.log(`\n=== ${text} ===`);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

await connectRedis();

// ---------- 1. PUB/SUB ----------
heading('PUB/SUB: live broadcast, nothing stored');

const stopA = await subscribe('news', (msg) => console.log('  [listener A] got:', msg));
await subscribe('news', (msg) => console.log('  [listener B] got:', msg));
await psubscribe('news*', (msg, channel) => console.log(`  [pattern news*] got on ${channel}:`, msg));

// PUBLISH returns how many Redis subscriptions were notified. Redis counts connections, not handler
// functions: here one connection holds a "news" subscription and a "news*" pattern, so the answer is 2.
console.log('publish to "news" ->', await publish('news', { headline: 'Maize prices up' }), 'subscriptions notified');
await sleep(100);

await stopA();
console.log('\nlistener A unsubscribed. B and the pattern still listen:');
await publish('news', { headline: 'Second story' });
await sleep(100);

console.log('\npublish to "sports" (nobody listening) ->', await publish('sports', 'goal!'), 'notified. The message is gone for good.');
await closePubSub();

// ---------- 2. STREAMS ----------
heading('STREAMS: a log that keeps everything');

const stream = 'demo:orders';
await client.del(stream);

const products = ['maize', 'feed', 'cassava', 'yam', 'beans', 'rice'];
for (const [i, product] of products.entries()) await addEvent(stream, { product, qty: i + 1 });
console.log('6 orders added BEFORE any consumer exists. XLEN =', await client.xLen(stream));

console.log('A late reader can still see history:');
for (const { id, message } of await client.xRange(stream, '-', '+')) console.log('  ', id, message);

await ensureGroup(stream, 'demo-group');

console.log('\nTwo workers share the work (each order goes to ONE of them):');
const handler = (name) => async (order) => {
  console.log(`  [${name}] processed ${order.product} x${order.qty}`);
  await sleep(100);
};
// count: 1 = take one entry at a time, so the two workers visibly alternate.
const w1 = await startConsumer({ stream, group: 'demo-group', consumer: 'w1', handler: handler('w1'), blockMs: 300, count: 1 });
const w2 = await startConsumer({ stream, group: 'demo-group', consumer: 'w2', handler: handler('w2'), blockMs: 300, count: 1 });
await sleep(1500);
console.log('pending after processing:', (await client.xPending(stream, 'demo-group')).pending, '(0 = everything acknowledged)');
await Promise.all([w1.stop(), w2.stop()]);

console.log('\nA worker that fails before ACK loses nothing:');
await addEvent(stream, { product: 'tractor', qty: 1 });
const flaky = await startConsumer({
  stream,
  group: 'demo-group',
  consumer: 'flaky',
  blockMs: 300,
  handler: async () => {
    throw new Error('boom'); // simulates a crash mid-job
  },
});
await sleep(800);
console.log('pending now:', (await client.xPending(stream, 'demo-group')).pending, '(the failed order is waiting, not lost)');

await flaky.stop();
console.log('worker restarts...');
const rescuer = await startConsumer({ stream, group: 'demo-group', consumer: 'flaky', handler: handler('flaky, restarted'), blockMs: 300 });
await sleep(800);
console.log('after restart, pending:', (await client.xPending(stream, 'demo-group')).pending);
await rescuer.stop();
await client.del(stream);

// ---------- 3. RATE LIMITING ----------
heading('RATE LIMITING: 3 requests per 2 seconds');

const show = (label, r) =>
  console.log(`  ${label}: ${r.allowed ? 'allowed' : 'BLOCKED'}  remaining=${r.remaining}` + (r.allowed ? '' : `  retry in ${r.retryAfterSec}s`));

await client.del(['demo:rl:fixed', 'demo:rl:sliding']);

console.log('Fixed window:');
for (let i = 1; i <= 5; i++) show(`request ${i}`, await fixedWindow('demo:rl:fixed', 3, 2));
await sleep(2100);
show('after the window resets', await fixedWindow('demo:rl:fixed', 3, 2));
console.log('  TTL on the counter key:', await client.ttl('demo:rl:fixed'), 'seconds (it deletes itself)');

console.log('\nSliding window:');
for (let i = 1; i <= 4; i++) show(`request ${i}`, await slidingWindow('demo:rl:sliding', 3, 2));
await sleep(2100);
show('2s later', await slidingWindow('demo:rl:sliding', 3, 2));

await client.del(['demo:rl:fixed', 'demo:rl:sliding']);

await closeRedis();
console.log('\nDone.');
