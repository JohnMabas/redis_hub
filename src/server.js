import express from 'express';
import { connectRedis, closeRedis } from './redis.js';
import {
  SESSION_TTL,
  createSession,
  getSession,
  incrementSessionField,
  destroySession,
  destroyAllUserSessions,
  listUserSessions,
} from './session.js';
import { getOrSetCache, invalidateCache, invalidateByPattern, cacheStats } from './cache.js';
import { findProduct, updateProduct } from './fakeDb.js';
import { rateLimit } from './rateLimit.js';
import { publish, subscribe, closePubSub } from './pubsub.js';
import { addEvent, recentEvents, streamStats, ensureGroup, ORDERS } from './streams.js';

const app = express();
app.use(express.json());
// Behind a proxy / load balancer, uncomment so req.ip is the real client and not the proxy:
// app.set('trust proxy', 1);

const COOKIE_NAME = 'sid';
const isProduction = process.env.NODE_ENV === 'production';

// Tiny cookie reader so we don't need another dependency.
function readCookie(req, name) {
  const header = req.headers.cookie ?? '';
  for (const part of header.split(';')) {
    const [k, ...rest] = part.trim().split('=');
    if (k === name) return decodeURIComponent(rest.join('='));
  }
  return null;
}

// ---------- Session middleware ----------
// Runs on every request: reads the cookie, loads the session from Redis, attaches it to req.
app.use(async (req, res, next) => {
  try {
    const id = readCookie(req, COOKIE_NAME);
    req.session = await getSession(id); // also slides the expiry forward
    req.sessionId = req.session ? id : null;
    next();
  } catch (err) {
    next(err);
  }
});

// ---------- Global rate limit ----------
// After the session middleware, so logged-in users are counted per user and everyone else per IP.
// Streaming (/events) is excluded: it is one long request, not many.
const apiLimiter = rateLimit({ name: 'api', limit: 60, windowSec: 60 });
app.use((req, res, next) => (req.path === '/events' ? next() : apiLimiter(req, res, next)));

function requireAuth(req, res, next) {
  if (!req.session) return res.status(401).json({ error: 'Not logged in' });
  next();
}

// ---------- Auth routes ----------
// Strict limit by IP: slows down password guessing. failOpen:false = if Redis is down, refuse logins.
const loginLimiter = rateLimit({
  name: 'login',
  limit: 5,
  windowSec: 60,
  identify: (req) => `ip:${req.ip}`,
  failOpen: false,
});

app.post('/login', loginLimiter, async (req, res, next) => {
  try {
    const { username } = req.body ?? {};
    if (!username) return res.status(400).json({ error: 'username is required' });

    // A real app checks a password against the database here. We just accept any username.
    const userId = username.toLowerCase();
    const sessionId = await createSession(userId, { username, role: 'student', cart: 0 });

    res.cookie(COOKIE_NAME, sessionId, {
      httpOnly: true, // JavaScript in the browser can't read it (blocks XSS theft)
      sameSite: 'lax', // basic CSRF protection
      secure: isProduction, // HTTPS only in production
      maxAge: SESSION_TTL * 1000,
    });
    res.json({ message: `Logged in as ${username}` });
  } catch (err) {
    next(err);
  }
});

app.get('/me', requireAuth, (req, res) => {
  res.json({ ...req.session, cart: Number(req.session.cart) }); // hash values are strings
});

app.post('/cart/add', requireAuth, async (req, res, next) => {
  try {
    const cart = await incrementSessionField(req.sessionId, 'cart', 1);
    res.json({ cart });
  } catch (err) {
    next(err);
  }
});

app.get('/sessions', requireAuth, async (req, res, next) => {
  try {
    res.json(await listUserSessions(req.session.userId));
  } catch (err) {
    next(err);
  }
});

app.post('/logout', requireAuth, async (req, res, next) => {
  try {
    await destroySession(req.sessionId);
    res.clearCookie(COOKIE_NAME);
    res.json({ message: 'Logged out' });
  } catch (err) {
    next(err);
  }
});

app.post('/logout-all', requireAuth, async (req, res, next) => {
  try {
    const count = await destroyAllUserSessions(req.session.userId);
    res.clearCookie(COOKIE_NAME);
    res.json({ message: `Logged out of ${count} session(s)` });
  } catch (err) {
    next(err);
  }
});

// ---------- Cached routes ----------
const productKey = (id) => `cache:product:${id}`;

// Sliding window: no edge-of-window bursts. 10 reads per 30 seconds per user or IP.
const productReadLimiter = rateLimit({ name: 'product-read', limit: 10, windowSec: 30, algorithm: 'sliding' });

app.get('/products/:id', productReadLimiter, async (req, res, next) => {
  try {
    const started = Date.now();
    const product = await getOrSetCache(productKey(req.params.id), 300, () =>
      findProduct(req.params.id)
    );

    if (!product) return res.status(404).json({ error: 'Product not found' });
    res.json({ product, tookMs: Date.now() - started }); // ~500ms on a miss, a few ms on a hit
  } catch (err) {
    next(err);
  }
});

app.put('/products/:id', requireAuth, async (req, res, next) => {
  try {
    const updated = await updateProduct(req.params.id, req.body);
    if (!updated) return res.status(404).json({ error: 'Product not found' });

    await invalidateCache(productKey(req.params.id)); // data changed, so drop the stale copy
    // Tell anyone watching live (see /events). Fire-and-forget: if nobody listens, nothing is lost that matters.
    await publish('notifications', { type: 'product.updated', id: updated.id, price: updated.price });
    res.json({ product: updated, cacheCleared: true });
  } catch (err) {
    next(err);
  }
});

app.delete('/cache/products', requireAuth, async (req, res, next) => {
  try {
    const deleted = await invalidateByPattern('cache:product:*');
    res.json({ deleted });
  } catch (err) {
    next(err);
  }
});

app.get('/cache/stats', async (req, res, next) => {
  try {
    res.json(await cacheStats());
  } catch (err) {
    next(err);
  }
});

// ---------- Pub/Sub: live notifications ----------
// POST /notify publishes. GET /events streams every message to the browser (Server-Sent Events).
//   curl -N localhost:3000/events        <- leave this running, then call POST /notify in another terminal
app.post('/notify', requireAuth, async (req, res, next) => {
  try {
    const { message } = req.body ?? {};
    if (!message) return res.status(400).json({ error: 'message is required' });

    const receivers = await publish('notifications', {
      type: 'message',
      from: req.session.userId,
      message,
      at: new Date().toISOString(),
    });
    // Redis counts subscribed CONNECTIONS (this server's shared subscriber, other servers' subscribers),
    // not browser tabs. 0 means no server had anyone listening.
    res.json({ receivers });
  } catch (err) {
    next(err);
  }
});

app.get('/events', async (req, res, next) => {
  try {
    res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
    res.flushHeaders();
    res.write(': connected\n\n'); // a line starting with ':' is an SSE comment

    // One Redis subscription per browser tab, all on the app's single shared subscriber connection.
    const unsubscribe = await subscribe('notifications', (data) => {
      res.write(`data: ${JSON.stringify(data)}\n\n`);
    });
    const heartbeat = setInterval(() => res.write(': ping\n\n'), 25000); // keeps proxies from closing the connection

    req.on('close', () => {
      clearInterval(heartbeat);
      unsubscribe();
    });
  } catch (err) {
    next(err);
  }
});

// ---------- Streams: a durable job queue ----------
// POST /orders adds an entry. One or more workers (npm run worker) take entries off the stream.
app.post('/orders', requireAuth, async (req, res, next) => {
  try {
    const { product, qty = 1 } = req.body ?? {};
    if (!product) return res.status(400).json({ error: 'product is required' });

    const id = await addEvent(ORDERS, { userId: req.session.userId, product, qty });
    res.status(202).json({ queued: true, id }); // 202 = accepted, will be processed later
  } catch (err) {
    next(err);
  }
});

app.get('/orders/recent', async (req, res, next) => {
  try {
    res.json(await recentEvents(ORDERS, 5));
  } catch (err) {
    next(err);
  }
});

// pending = delivered to a worker but not yet acknowledged
app.get('/orders/stats', async (req, res, next) => {
  try {
    res.json(await streamStats());
  } catch (err) {
    next(err);
  }
});

// ---------- Error handler ----------
app.use((err, req, res, next) => {
  console.error(err);
  res.status(500).json({ error: 'Something went wrong' });
});

// ---------- Start ----------
await connectRedis();
await ensureGroup(); // so /orders/stats works before the first worker starts
const port = process.env.PORT ?? 3000;
const server = app.listen(port, () => console.log(`Server running on http://localhost:${port}`));

// Close things cleanly on Ctrl+C.
process.on('SIGINT', async () => {
  server.closeAllConnections?.(); // end open /events streams so close() can finish
  server.close();
  await closePubSub();
  await closeRedis();
  process.exit(0);
});
