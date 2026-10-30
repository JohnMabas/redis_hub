// Seeing the fixed-window BURST on purpose.
//   npm run demo:burst        (takes about 20 seconds)
//
// Limit: 3 requests per 5 seconds. A perfect limiter would never allow more than 3 in ANY 5-second span.
// We aim bursts at the moment a window ends and the next begins, and count what gets through.
import { connectRedis, closeRedis, client } from "./src/redis.js";
import { fixedWindow, slidingWindow } from "./src/rateLimit.js";

const LIMIT = 3;
const WINDOW = 5; // seconds

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const heading = (text) => console.log(`\n=== ${text} ===`);

let t0;
let events;
const startRun = () => {
  t0 = Date.now();
  events = [];
};

async function hit(label, check) {
  const r = await check();
  const t = (Date.now() - t0) / 1000;
  events.push({ t, allowed: r.allowed });
  console.log(
    `  t=${t.toFixed(2).padStart(5)}s  ${label.padEnd(8)} ${r.allowed ? "allowed" : "BLOCKED"}`,
  );
}

// The most allowed requests that landed inside any 1-second stretch.
function busiestSecond() {
  const ok = events.filter((e) => e.allowed).map((e) => e.t);
  return Math.max(
    ...ok.map((start) => ok.filter((t) => t >= start && t < start + 1).length),
  );
}

function summary() {
  console.log(
    `  -> most requests allowed inside ONE second: ${busiestSecond()}   (limit is ${LIMIT} per ${WINDOW}s)`,
  );
}

await connectRedis();

// ---------- A. The project's fixed window: the window starts at the FIRST request ----------
heading(
  `A. Fixed window as written in rateLimit.js (window starts at the first request)`,
);
await client.del("burst:a");
const check = () => fixedWindow("burst:a", LIMIT, WINDOW);
startRun();

await hit("start", check); // request 1 starts the 5-second clock. It also uses up one of the 3 slots.
while ((await client.pTTL("burst:a")) > 400) await sleep(25); // wait until the window is nearly over
await hit("late 1", check);
await hit("late 2", check); // slots 2 and 3 used, right at the end
console.log("  ...window ends...");
while (await client.exists("burst:a")) await sleep(10); // key deleted by its TTL: new window
await hit("new 1", check);
await hit("new 2", check);
await hit("new 3", check);
summary();

// ---------- B. Fixed window aligned to the clock: the textbook version ----------
heading(
  `B. Fixed window aligned to the clock (windows start at :00, :05, :10 ...)`,
);
const bucket = () => Math.floor(Date.now() / (WINDOW * 1000));
const aligned = () => fixedWindow(`burst:b:${bucket()}`, LIMIT, WINDOW * 2); // one key per clock window

// Wait until about 0.4s before a clock boundary.
while (WINDOW * 1000 - (Date.now() % (WINDOW * 1000)) > 400) await sleep(10);
startRun();
await hit("end 1", aligned);
await hit("end 2", aligned);
await hit("end 3", aligned); // all 3 slots used just before the boundary
console.log("  ...clock boundary...");
while (Date.now() % (WINDOW * 1000) > 300) await sleep(5); // wait until the next window has begun
await hit("start 1", aligned);
await hit("start 2", aligned);
await hit("start 3", aligned);
summary();

// ---------- C. Sliding window: same trick, no burst ----------
heading(`C. Sliding window (same timing as A)`);
await client.del("burst:c");
const sliding = () => slidingWindow("burst:c", LIMIT, WINDOW);
startRun();

await hit("start", sliding);
await sleep(WINDOW * 1000 - 400);
await hit("late 1", sliding);
await hit("late 2", sliding);
console.log("  ...5 seconds after the first request...");
await sleep(500);
await hit("new 1", sliding);
await hit("new 2", sliding);
await hit("new 3", sliding);
summary();

await client.del(["burst:a", "burst:c"]);
await closeRedis();
console.log("\nDone.");