# Week 6 lesson sheet: sessions, pub/sub, streams, rate limiting

Order of teaching: **redis-cli first**, then the node-redis twin of every command.
Every command here was run against Redis 7 before this sheet was written.

---

## 0. Sessions (recap)

A session is a hash with a TTL. The cookie holds only the random ID.

```
HSET session:abc123 userId 42 role admin cart 0
EXPIRE session:abc123 1800
HGETALL session:abc123
HINCRBY session:abc123 cart 1
EXPIRE session:abc123 1800        <- sliding expiry: run on every request
SADD user:42:sessions abc123      <- index, for "log out everywhere"
DEL session:abc123
```

Code: `src/session.js`.

---

## 1. Pub/Sub: live broadcast, nothing stored

**Idea:** radio. Whoever is tuned in *right now* hears it. Nobody tuned in = the message is gone.

Open **two terminals** with `redis-cli`.

| Terminal A (listener) | Terminal B (sender) |
|---|---|
| `SUBSCRIBE news` | |
| | `PUBLISH news "maize prices up"` |
| A prints the message | B prints `(integer) 1` (subscribers who got it) |
| | `PUBLISH news "anyone there?"` after closing A prints `0`: lost forever |

More commands:
```
PSUBSCRIBE news:*              listen to every channel matching a pattern
PUBLISH news:sports "goal"     the pattern above receives this
PUBSUB CHANNELS                channels that currently have listeners
PUBSUB NUMSUB news             how many are listening to one channel
UNSUBSCRIBE news
```

Things to point out:
- A client in subscribe mode can't run `GET`/`SET`. That's why node-redis needs a **second connection** (`client.duplicate()`).
- `PUBLISH` counts Redis subscriptions (connections), not handler functions in your code.
- No history, no acknowledgements, no retry. Use it for notifications, live dashboards, chat, cache-busting signals. Don't use it for anything you can't afford to lose.

| CLI | node-redis |
|---|---|
| `PUBLISH ch msg` | `client.publish('ch', msg)` |
| `SUBSCRIBE ch` | `sub.subscribe('ch', (message, channel) => {...})` |
| `PSUBSCRIBE p*` | `sub.pSubscribe('p*', (message, channel) => {...})` |

Code: `src/pubsub.js`, and `GET /events` in `src/server.js` (Server-Sent Events).

---

## 2. Streams: a log that keeps everything

**Idea:** a ledger. Entries are appended with an ID, kept, and can be read now or later, by one reader or a team.

### Basic log
```
XADD orders * product maize qty 2        * = auto-generate the ID (timestamp-sequence)
XADD orders * product feed qty 1
XLEN orders
XRANGE orders - +                        everything, oldest first (- = start, + = end)
XREVRANGE orders + - COUNT 1             the newest entry
XREAD COUNT 5 STREAMS orders 0           read from the beginning
XREAD BLOCK 5000 STREAMS orders $        wait up to 5s for NEW entries ($ = only new)
```

### Consumer groups: a team sharing the work
```
XGROUP CREATE orders workers 0 MKSTREAM  group starts at the beginning; MKSTREAM creates the stream if missing
XREADGROUP GROUP workers w1 COUNT 1 STREAMS orders >     w1 takes the next undelivered entry
XREADGROUP GROUP workers w2 COUNT 1 STREAMS orders >     w2 gets a DIFFERENT entry
XPENDING orders workers                  delivered but not yet acknowledged
XPENDING orders workers - + 10           which entries, and who holds them
XACK orders workers <id>                 "done". Removes it from pending
```

### When a worker dies
```
XREADGROUP GROUP workers w1 COUNT 10 STREAMS orders 0    ID 0 = "my own unacknowledged entries"
XAUTOCLAIM orders workers w2 30000 0-0 COUNT 10          w2 takes over entries idle for 30s+
```

### Housekeeping
```
XINFO GROUPS orders
XTRIM orders MAXLEN ~ 1000               keep roughly the newest 1000 (~ = cheap approximate trim)
```

### Stream vs Pub/Sub vs List
| | Pub/Sub | List (`LPUSH`/`BRPOP`) | Stream |
|---|---|---|---|
| Stored after delivery | No | No (popped) | **Yes** |
| Late reader sees history | No | No | **Yes** |
| Several workers share jobs | No (all get a copy) | Yes | **Yes** |
| Acknowledge / retry after crash | No | No | **Yes** |

| CLI | node-redis |
|---|---|
| `XADD s * f v` | `client.xAdd('s', '*', { f: 'v' })` (values must be strings) |
| `XRANGE s - +` | `client.xRange('s', '-', '+')` |
| `XGROUP CREATE s g 0 MKSTREAM` | `client.xGroupCreate('s', 'g', '0', { MKSTREAM: true })` |
| `XREADGROUP GROUP g c COUNT 1 BLOCK 2000 STREAMS s >` | `conn.xReadGroup('g', 'c', { key: 's', id: '>' }, { COUNT: 1, BLOCK: 2000 })` |
| `XACK s g id` | `client.xAck('s', 'g', id)` |

Gotchas:
- Creating a group twice throws `BUSYGROUP`. That just means "already exists", so catch and ignore it.
- `BLOCK` freezes its connection while waiting, so workers use their own (`duplicate()`).
- Entries print as `[Object: null prototype]`. That's normal; read them like any object.

Code: `src/streams.js`, `src/worker.js`, `POST /orders` in `src/server.js`.

---

## 3. Rate limiting

**Idea:** at most N requests per window per user. The counter is in Redis so every server shares it.

### Fixed window: simple and cheap
```
INCR rl:ada                 1, 2, 3 ...  (creates the key at 0 first)
EXPIRE rl:ada 60 NX         start the 60s clock only if none is running (Redis 7+)
TTL rl:ada                  seconds until the counter resets
```
If the count is above the limit, answer **429 Too Many Requests** with a `Retry-After` header.

Weakness: the burst. With 5 per minute, a user can send 5 at 0:59 and 5 at 1:01, so 10 requests in two seconds.

### Sliding window: exact, a bit more memory
One sorted set per user, with each request's timestamp as the score:
```
ZADD rl:sliding 1000 req-a
ZADD rl:sliding 1500 req-b
ZADD rl:sliding 2500 req-c
ZREMRANGEBYSCORE rl:sliding 0 600        drop requests older than the window
ZCARD rl:sliding                         how many are left in the window
```

### Why Lua?
Between "read the count" and "write the count", another request can sneak in. `EVAL` runs a script as **one indivisible step**:
```
EVAL "return redis.call('INCR', KEYS[1])" 1 rl:ada
```
`src/rateLimit.js` uses this for both algorithms.

| CLI | node-redis |
|---|---|
| `INCR k` / `EXPIRE k 60 NX` | `client.incr(k)` / `client.expire(k, 60, 'NX')` |
| `ZADD k score member` | `client.zAdd(k, { score, value })` |
| `EVAL script 1 key arg` | `client.eval(script, { keys: [key], arguments: [arg] })` |

Choosing the key: logged-in users by user ID, everyone else by IP. Different limits for different routes: strict on `/login` (5 per minute), generous elsewhere.

If Redis is down: **fail open** (let traffic through) for general APIs, **fail closed** (refuse) for login.

Code: `src/rateLimit.js`, used in `src/server.js`.

---

## Quick check (for the start of the next class)

1. Why can't a client in `SUBSCRIBE` mode run `GET`?
2. You `PUBLISH` to a channel nobody is on. What does Redis return, and where does the message go?
3. Give two reasons to pick a stream over a list for a job queue.
4. A worker reads an entry and crashes before `XACK`. Where is the entry now, and how does a new worker get it?
5. What does `XADD s * ...` do with the `*`?
6. A fixed window allows 5 requests per minute. What's the most requests a user can send in any 2-second span?
7. Why is `INCR` then `EXPIRE` as two separate commands risky? Name two fixes.
8. Which HTTP status and header does a blocked request get?

**Answers**
1. Subscribe mode dedicates the connection to receiving messages; it only accepts (un)subscribe commands. Use a second connection.
2. `0`. The message is gone; nothing stores it.
3. Entries are kept after delivery, workers share jobs through groups, unacknowledged entries are retried after a crash, and late readers can see history.
4. It stays in the group's pending list. The same consumer re-reads it with ID `0` on restart, or another worker takes it with `XAUTOCLAIM` after it's been idle long enough.
5. Auto-generates the ID (millisecond timestamp plus a sequence number).
6. 10: 5 at the end of one window and 5 at the start of the next.
7. If the app crashes between them, the key never expires and the user is blocked forever. Two fixes: run both in one atomic step (a Lua script, or `MULTI`/`EXEC`), or create the key with its TTL first (`SET key 0 EX 60 NX`) and `INCR` afterwards.
8. `429 Too Many Requests` with `Retry-After: <seconds>`.
