# redis-lab: sessions, caching, pub/sub, streams and rate limiting with node-redis

Teaching order: `docs/week6-redis-cli.md` (redis-cli first), then the code here.

## 1. Start Redis

```bash
docker run -d --name redis -p 6379:6379 redis:latest
```

Or use a local install / WSL. Check it works: `redis-cli ping` should answer `PONG`.
Needs **Redis 7+** (the lesson sheet uses `EXPIRE ... NX`) and Node 18+.

## 2. Install and run

```bash
npm install
npm run demo            # sessions + caching, in the terminal
npm run demo:realtime   # pub/sub + streams + rate limiting, in the terminal
npm start               # Express server on http://localhost:3000
npm run worker worker-1 # a stream worker (start several, in separate terminals)
```

Use another Redis with `REDIS_URL=redis://host:6379 npm start`.

## 3. Try the server

`-c cookies.txt` saves the session cookie, `-b cookies.txt` sends it back.
Rate limits apply, so on a shared machine give each student a few seconds between runs of the spam tests.

```bash
# Sessions
curl -c cookies.txt -X POST localhost:3000/login -H "Content-Type: application/json" -d '{"username":"Ada"}'
curl -b cookies.txt localhost:3000/me
curl -b cookies.txt -X POST localhost:3000/cart/add
curl -b cookies.txt localhost:3000/sessions
curl -b cookies.txt -X POST localhost:3000/logout
curl -b cookies.txt localhost:3000/me          # 401 now

# Caching: run the first one twice and compare tookMs
curl localhost:3000/products/1
curl localhost:3000/products/1
curl localhost:3000/products/999              # 404, then cached as "not found" for 30s
curl localhost:3000/cache/stats

# Invalidation (log in first)
curl -b cookies.txt -X PUT localhost:3000/products/1 -H "Content-Type: application/json" -d '{"price":35000}'
curl -b cookies.txt -X DELETE localhost:3000/cache/products
```

### Pub/Sub (live events)

```bash
# Terminal 1: leave this running. Messages appear here as they are published.
curl -N localhost:3000/events

# Terminal 2 (logged in):
curl -b cookies.txt -X POST localhost:3000/notify -H "Content-Type: application/json" -d '{"message":"class starts at 2pm"}'
curl -b cookies.txt -X PUT localhost:3000/products/1 -H "Content-Type: application/json" -d '{"price":36000}'   # also publishes
```

### Streams (a job queue)

```bash
# Start one or two workers first:  npm run worker worker-1   /   npm run worker worker-2
curl -b cookies.txt -X POST localhost:3000/orders -H "Content-Type: application/json" -d '{"product":"maize","qty":2}'
curl localhost:3000/orders/recent
curl localhost:3000/orders/stats            # pending = delivered but not yet acknowledged
```

With `/events` open in another terminal you'll also see an `order.processed` message each time a worker finishes.
Stop all workers, queue a few orders, and watch `pending` / `length` grow; start a worker and it catches up.

### Rate limiting

```bash
# /login allows 5 per minute per IP. The 6th gets 429 and a Retry-After header.
for i in 1 2 3 4 5 6; do curl -s -o /dev/null -D - -X POST localhost:3000/login -H "Content-Type: application/json" -d '{"username":"x"}' | grep -iE '^(HTTP|retry-after|x-ratelimit-remaining)'; done

# /products/:id allows 10 reads per 30 seconds (sliding window). The 11th gets 429.
for i in $(seq 1 12); do curl -s -o /dev/null -w '%{http_code} ' localhost:3000/products/2; done
```

Everything else shares one 60-per-minute limit per user (or per IP when logged out).

## 4. Watch Redis while it runs

Keep `redis-cli` open in a second terminal:

```
KEYS session:*            (fine on a lab machine, use SCAN in production)
HGETALL session:<id>
TTL session:<id>
SMEMBERS user:ada:sessions
GET cache:product:1
TTL cache:product:1
KEYS ratelimit:*
GET ratelimit:login:ip:127.0.0.1
ZRANGE ratelimit:product-read:ip:127.0.0.1 0 -1 WITHSCORES
XRANGE stream:orders - +
XINFO GROUPS stream:orders
SUBSCRIBE notifications   (a second listener, next to the browser/curl one)
MONITOR                   (streams every command as it happens, Ctrl+C to stop)
INFO stats
```

## What's where

| File | Purpose |
|---|---|
| `src/redis.js` | One shared client: connect, error listener, close |
| `src/session.js` | create / get (sliding TTL) / update / destroy / destroy-all / list |
| `src/cache.js` | `getOrSetCache` (cache-aside + lock + negative caching), invalidation, stats |
| `src/pubsub.js` | `publish`, `subscribe`, `psubscribe` on a dedicated subscriber connection |
| `src/streams.js` | `addEvent`, consumer groups, `startConsumer` (ack, crash recovery) |
| `src/worker.js` | A runnable stream worker that processes orders |
| `src/rateLimit.js` | Fixed-window and sliding-window limiters (Lua) + Express middleware |
| `src/fakeDb.js` | Slow pretend database so cache hits are visible |
| `src/server.js` | Express app using all of the above |
| `src/demo.js` | Sessions + caching lesson as one script |
| `src/demo-realtime.js` | Pub/sub + streams + rate limiting lesson as one script |
| `docs/week6-redis-cli.md` | CLI-first lesson sheet with a quick check and answers |

## Key patterns

- **Session** = hash at `session:<random id>` + TTL, cookie holds only the ID.
- **Sliding expiry** = `EXPIRE` on every request.
- **Per-user index** = set at `user:<id>:sessions` (enables "log out everywhere").
- **Cache-aside** = GET, on miss fetch + `SET ... EX`, invalidate with `DEL` on change.
- **Stampede lock** = `SET lock:<key> 1 NX EX 10`.
- **Negative caching** = remember "not found" for 30s.
- **Pub/Sub** = fire-and-forget broadcast on a channel; subscribers need their own connection.
- **Stream + consumer group** = durable queue: `XADD`, `XREADGROUP`, `XACK`; unacknowledged entries are retried.
- **Rate limit** = a counter (or sorted set) per user, checked in one Lua script; 429 + `Retry-After` when exceeded.

## Not covered (good next steps)

- `XAUTOCLAIM` in the worker, so a *different* worker rescues entries from a worker that never comes back.
- Pub/Sub or Streams across several server instances: the code already supports it, since the state lives in Redis.
- Rate-limit headers on blocked requests for `X-RateLimit-Reset`.
# redis_hub
