// ANSWER KEY: Naira Rate Checker (Levels 1-5)
// Run:  node exercises/rates-solution.js
// Needs a running Redis on localhost:6379.
import { createClient } from "redis";

// Real API by default. The env override lets you test against a local stand-in.
const API_BASE = process.env.RATES_API ?? "https://open.er-api.com/v6/latest";

const MIN_TTL = 60; // never cache for less than a minute
const MAX_TTL = 6 * 60 * 60; // never trust data for more than 6 hours
const LOCK_TTL = 10; // seconds

const client = createClient();
client.on("error", (err) => console.log("Redis error", err));
await client.connect();

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
let apiCalls = 0; // only used to prove the cache works in the demo

async function fetchRates(base) {
  console.log("🌍 calling the API...");
  apiCalls++;
  const res = await fetch(`${API_BASE}/${base}`);
  if (!res.ok) throw new Error(`API error ${res.status}`);

  const data = await res.json();
  if (data.result !== "success") throw new Error(`API says: ${data.result}`);
  return data;
}

// LEVEL 2: cache for as long as the API says the data is fresh, within limits.
// time_next_update_unix is in SECONDS; Date.now() is in MILLISECONDS.
function ttlFor(data) {
  const secondsLeft = Math.floor(data.time_next_update_unix - Date.now() / 1000);
  return Math.min(Math.max(secondsLeft, MIN_TTL), MAX_TTL);
}

// LEVELS 1, 3 and 5: cache-aside, with counters and a stampede lock.
async function getRates(base) {
  const key = `cache:rates:${base}`;

  const cached = await client.get(key);
  if (cached !== null) {
    await client.incr("stats:rates:hits");
    console.log("HIT");
    return JSON.parse(cached);
  }

  // Miss. Only one request may rebuild the value: SET ... NX succeeds for one caller only.
  const lockKey = `lock:rates:${base}`;
  const gotLock = await client.set(lockKey, "1", { NX: true, EX: LOCK_TTL });

  if (!gotLock) {
    // Someone else is calling the API. Wait for their result (up to ~1s).
    for (let i = 0; i < 20; i++) {
      await sleep(50);
      const retry = await client.get(key);
      if (retry !== null) {
        await client.incr("stats:rates:hits");
        console.log("HIT (waited for another request)");
        return JSON.parse(retry);
      }
    }
    // Still nothing after a second: give up waiting and call the API ourselves.
  }

  try {
    // Double-check: another request may have finished between our first GET and our lock.
    if (gotLock) {
      const again = await client.get(key);
      if (again !== null) {
        await client.incr("stats:rates:hits");
        console.log("HIT (filled while we got the lock)");
        return JSON.parse(again);
      }
    }

    await client.incr("stats:rates:misses");
    console.log("MISS");

    const data = await fetchRates(base);
    await client.set(key, JSON.stringify(data), { EX: ttlFor(data) });
    return data;
  } finally {
    if (gotLock) await client.del(lockKey); // only release a lock we took
  }
}

// LEVEL 4: one cached rate table per BASE currency serves every conversion from it.
async function convert(amount, from, to) {
  const data = await getRates(from);
  const rate = data.rates[to];
  if (rate === undefined) throw new Error(`Unknown currency: ${to}`);
  return amount * rate;
}

// LEVEL 3: hit/miss numbers from our own counters (not INFO stats, which counts every key).
async function printStats() {
  const hits = Number((await client.get("stats:rates:hits")) ?? 0);
  const misses = Number((await client.get("stats:rates:misses")) ?? 0);
  const total = hits + misses;
  const rate = total ? ((hits / total) * 100).toFixed(0) : 0;
  console.log(`Stats: ${hits} hits, ${misses} misses, ${rate}% hit rate`);
}

// ---------------- Demo: runs every level in order ----------------
const heading = (t) => console.log(`\n=== ${t} ===`);

await client.del([
  "cache:rates:USD",
  "cache:rates:EUR",
  "stats:rates:hits",
  "stats:rates:misses",
]);

heading("Level 1: three calls, expect MISS then HIT HIT");
for (let i = 0; i < 3; i++) await getRates("USD");

heading("Level 2: TTL comes from the API, not a guess");
console.log("TTL cache:rates:USD =", await client.ttl("cache:rates:USD"), "seconds");

heading("Level 3: ten calls, then stats");
for (let i = 0; i < 10; i++) await getRates("USD");
await printStats();

heading("Level 4: conversions share one cached table");
const before = apiCalls;
console.log("100 USD =", await convert(100, "USD", "NGN"), "NGN");
console.log("5000 USD =", await convert(5000, "USD", "NGN"), "NGN");
console.log("API calls for the two USD conversions:", apiCalls - before);
console.log("100 EUR =", await convert(100, "EUR", "NGN"), "NGN (new base, so one new call)");
console.log("Total API calls so far:", apiCalls);

heading("Level 5: 10 simultaneous requests on an empty cache");
await client.del("cache:rates:USD");
const callsBefore = apiCalls;
await Promise.all(Array.from({ length: 10 }, () => getRates("USD")));
console.log("API calls made by 10 simultaneous requests:", apiCalls - callsBefore);

heading("Bad currency code is rejected");
try {
  await convert(100, "USD", "ZZZ");
} catch (err) {
  console.log("Error:", err.message);
}

await client.quit(); // without this the script never exits
