import { createClient } from 'redis';

// One client for the whole app. Create it once, connect once, reuse it everywhere.
export const client = createClient({
  url: process.env.REDIS_URL ?? 'redis://localhost:6379',
});

// Without an 'error' listener, a connection problem crashes the process.
client.on('error', (err) => console.error('Redis error:', err.message));

export async function connectRedis() {
  if (!client.isOpen) await client.connect();
  return client;
}

export async function closeRedis() {
  if (client.isOpen) await client.quit();
}
