import { client } from './redis.js';

/*
 * A Stream is an append-only log. Unlike Pub/Sub, entries are KEPT, so:
 *   - a consumer that starts late can read history,
 *   - a consumer GROUP shares the work (each entry goes to ONE worker),
 *   - entries stay "pending" until a worker ACKs them, so a crash loses nothing.
 */

export const ORDERS = 'stream:orders';
export const GROUP = 'order-workers';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** Stream values must be strings, so stringify everything. Returns the entry ID, e.g. 1792638997782-0 */
export async function addEvent(stream, fields) {
  const strings = Object.fromEntries(Object.entries(fields).map(([k, v]) => [k, String(v)]));
  return client.xAdd(stream, '*', strings, {
    // Keep roughly the latest 1000 entries so the stream can't grow forever. '~' = trim in cheap chunks.
    TRIM: { strategy: 'MAXLEN', strategyModifier: '~', threshold: 1000 },
  });
}

/** Create the consumer group if it doesn't exist. MKSTREAM creates the stream too. Safe to call repeatedly. */
export async function ensureGroup(stream = ORDERS, group = GROUP) {
  try {
    await client.xGroupCreate(stream, group, '0', { MKSTREAM: true }); // '0' = group starts from the beginning
  } catch (err) {
    if (!String(err.message).includes('BUSYGROUP')) throw err; // BUSYGROUP just means "already exists"
  }
}

export async function recentEvents(stream = ORDERS, count = 5) {
  return client.xRevRange(stream, '+', '-', { COUNT: count }); // newest first
}

export async function streamStats(stream = ORDERS, group = GROUP) {
  await ensureGroup(stream, group);
  const [length, pending] = await Promise.all([client.xLen(stream), client.xPending(stream, group)]);
  return { length, pending: pending.pending, consumers: pending.consumers ?? [] };
}

/**
 * Run a worker in a consumer group.
 *
 * Blocking reads (BLOCK) freeze the connection while they wait, so the worker gets its OWN
 * connection and the shared client stays free for normal commands.
 *
 * Returns { stop } so callers can shut the worker down cleanly.
 */
export async function startConsumer({ stream = ORDERS, group = GROUP, consumer, handler, blockMs = 2000, count = 10 }) {
  const conn = client.duplicate();
  conn.on('error', (err) => console.error(`[${consumer}] Redis error:`, err.message));
  await conn.connect();
  await ensureGroup(stream, group);

  let running = true;

  // Read a batch with the given ID: '>' = new entries only, '0' = my own unacknowledged entries.
  const readBatch = (id, block) =>
    conn.xReadGroup(group, consumer, { key: stream, id }, { COUNT: count, ...(block ? { BLOCK: block } : {}) });

  async function handle(entries) {
    for (const { id, message } of entries) {
      if (message === null) {
        await conn.xAck(stream, group, id); // entry was trimmed away while pending: nothing left to process
        continue;
      }
      try {
        await handler({ id, ...message });
        await conn.xAck(stream, group, id); // ACK only AFTER success. No ack = stays pending = retried.
      } catch (err) {
        console.error(`[${consumer}] failed on ${id}, leaving it pending:`, err.message);
      }
    }
  }

  const loop = (async () => {
    // 1. Crash recovery: retry anything this consumer read before but never acknowledged.
    //    Walk forward with a cursor so an entry that keeps failing is tried once per start-up,
    //    not in an endless loop.
    let cursor = '0';
    while (running) {
      const res = await readBatch(cursor);
      const entries = res?.[0]?.messages ?? [];
      if (entries.length === 0) break;
      await handle(entries);
      cursor = entries[entries.length - 1].id;
    }

    // 2. Normal operation: wait for new entries.
    while (running) {
      try {
        const res = await readBatch('>', blockMs);
        const entries = res?.[0]?.messages ?? [];
        if (entries.length > 0) await handle(entries);
      } catch (err) {
        if (!running) break;
        console.error(`[${consumer}] read failed, retrying:`, err.message);
        await sleep(1000);
      }
    }
  })();

  return {
    async stop() {
      running = false;
      await loop; // finishes the current block (up to blockMs), then exits
      await conn.quit();
    },
  };
}
