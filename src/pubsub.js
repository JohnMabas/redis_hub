import { client } from './redis.js';

/*
 * Pub/Sub = live broadcast. Publishers send to a CHANNEL; everyone subscribed RIGHT NOW gets it.
 * Nothing is stored. If nobody is listening, the message is gone (use Streams when you can't lose it).
 *
 * A connection in subscriber mode can ONLY (un)subscribe, it can't run GET/SET.
 * So subscribing always needs its own connection: client.duplicate().
 */

let subscriber = null;

async function getSubscriber() {
  if (!subscriber) {
    subscriber = client.duplicate(); // same settings, separate connection
    subscriber.on('error', (err) => console.error('Redis subscriber error:', err.message));
    await subscriber.connect();
  }
  return subscriber;
}

/** Send a message. Returns how many subscribers received it (0 = nobody was listening). */
export async function publish(channel, payload) {
  return client.publish(channel, JSON.stringify(payload));
}

const parse = (raw) => {
  try {
    return JSON.parse(raw);
  } catch {
    return raw; // not JSON (for example sent from redis-cli): pass the text through
  }
};

/** Listen on one channel. Returns an async function that stops listening. */
export async function subscribe(channel, handler) {
  const sub = await getSubscriber();
  const listener = (raw, ch) => handler(parse(raw), ch);
  await sub.subscribe(channel, listener);
  return () => sub.unsubscribe(channel, listener);
}

/** Listen on every channel matching a pattern, e.g. 'orders:*'. Handler gets (message, channel). */
export async function psubscribe(pattern, handler) {
  const sub = await getSubscriber();
  const listener = (raw, ch) => handler(parse(raw), ch);
  await sub.pSubscribe(pattern, listener);
  return () => sub.pUnsubscribe(pattern, listener);
}

export async function closePubSub() {
  if (subscriber?.isOpen) await subscriber.quit();
  subscriber = null;
}
