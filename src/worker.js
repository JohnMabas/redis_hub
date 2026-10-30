// A stream worker. Run one or several, each with its own name:
//   node src/worker.js worker-1
//   node src/worker.js worker-2
// Orders added by POST /orders are shared between them: each order goes to exactly ONE worker.
import { connectRedis, closeRedis } from './redis.js';
import { startConsumer, ORDERS } from './streams.js';
import { publish, closePubSub } from './pubsub.js';

const name = process.argv[2] ?? `worker-${process.pid}`;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

await connectRedis();

const consumer = await startConsumer({
  stream: ORDERS,
  consumer: name,
  handler: async (order) => {
    console.log(`[${name}] processing order ${order.id}: ${order.qty} x ${order.product} for ${order.userId}`);
    await sleep(300); // pretend work: charge card, update stock, send email...
    // Tell any live listeners (the /events page) it's done. Pub/Sub is perfect for this: it's only a notification.
    await publish('notifications', { type: 'order.processed', orderId: order.id, product: order.product, by: name });
    console.log(`[${name}] done ${order.id}`);
  },
});

console.log(`[${name}] waiting for orders on ${ORDERS} ...`);

process.on('SIGINT', async () => {
  console.log(`\n[${name}] shutting down...`);
  await consumer.stop();
  await closePubSub();
  await closeRedis();
  process.exit(0);
});
