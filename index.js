import { ethers } from "ethers";

/* ═══════════════════════════════════════════════════════════════════════════
   leveraged.lol — trade indexer

   Reads Swap events from every launched pool and writes them to Supabase so
   the frontend does not have to. Before this, opening a token page meant
   querying logs across thousands of blocks, which took seconds and produced
   a sparse chart because sparse data is what raw log ranges give you.

   Runs alongside the price keeper. Same shape: one file, no build step.

   ── WHY RAW SWAPS RATHER THAN CANDLES ────────────────────────────────────

   Storing OHLC would mean choosing an interval at write time. A token minutes
   old needs second-level candles or the chart is one flat bar; an established
   one wants hours. Raw rows are small, and letting the frontend bucket them
   keeps that choice where the context is.
   ═══════════════════════════════════════════════════════════════════════════ */

const RPC     = process.env.RPC_URL;
const FACTORY = process.env.FACTORY_ADDRESS;
const SB_URL  = (process.env.SUPABASE_URL || "").trim().replace(/\/+$/, "");

/* Pasting a long key into a dashboard field often introduces a line break, and
   a header containing one is rejected outright with an error that names the
   key rather than the cause. Strip all whitespace rather than letting that
   happen every write. */
const SB_KEY = (process.env.SUPABASE_SERVICE_KEY || "").replace(/\s+/g, "");

if (!RPC || !FACTORY || !SB_URL || !SB_KEY) {
  console.error("Missing RPC_URL, FACTORY_ADDRESS, SUPABASE_URL or SUPABASE_SERVICE_KEY");
  process.exit(1);
}
if (!/^sb_secret_|^eyJ/.test(SB_KEY)) {
  console.error("SUPABASE_SERVICE_KEY does not look like a secret key. It should");
  console.error("start with sb_secret_ . The publishable key cannot write.");
  process.exit(1);
}

const CFG = {
  everySec:   Number(process.env.EVERY_SEC   || 20),
  logStep:    Number(process.env.LOG_STEP    || 2000),
  maxPerPass: Number(process.env.MAX_PER_PASS || 4000),
  quoteDec:   18,
};

const FAC_ABI = [
  "function launchCount() view returns (uint256)",
  "function launches(uint256) view returns (address token,address quote,address pool,address locker,address creator,uint64 createdAt)",
];
const POOL_ABI = [
  "event Swap(address indexed sender,address indexed recipient,int256 amount0,int256 amount1,uint160 sqrtPriceX96,uint128 liquidity,int24 tick)",
];

const provider = new ethers.JsonRpcProvider(RPC);
const factory  = new ethers.Contract(FACTORY, FAC_ABI, provider);

/* Plain fetch against PostgREST rather than the supabase-js client. The client
   pulls in a realtime websocket layer that refuses to start on Node 20, and
   this worker only ever does two things: upsert rows and read one column. Not
   worth a dependency, and one less thing to break on a runtime upgrade. */
const sbHeaders = {
  apikey: SB_KEY,
  Authorization: `Bearer ${SB_KEY}`,
  "Content-Type": "application/json",
};

async function sbSelect(path) {
  const r = await fetch(`${SB_URL}/rest/v1/${path}`, { headers: sbHeaders });
  if (!r.ok) throw new Error(`select ${r.status}: ${await r.text()}`);
  return r.json();
}

/** Upsert. `onConflict` names the primary key columns. */
async function sbUpsert(table, rows, onConflict, ignoreDuplicates = false) {
  const r = await fetch(`${SB_URL}/rest/v1/${table}?on_conflict=${onConflict}`, {
    method: "POST",
    headers: {
      ...sbHeaders,
      Prefer: `resolution=${ignoreDuplicates ? "ignore" : "merge"}-duplicates,return=minimal`,
    },
    body: JSON.stringify(rows),
  });
  if (!r.ok) throw new Error(`upsert ${r.status}: ${await r.text()}`);
}

const log = (...a) => console.log(new Date().toISOString(), ...a);

/* Block timestamps are the expensive part: one RPC call each, and a busy pool
   can touch hundreds. Cache aggressively, since a block's timestamp never
   changes once it exists. */
const tsCache = new Map();
async function timestampOf(block) {
  if (tsCache.has(block)) return tsCache.get(block);
  const b = await provider.getBlock(block);
  const t = b ? b.timestamp : Math.floor(Date.now() / 1000);
  tsCache.set(block, t);
  if (tsCache.size > 20_000) tsCache.clear();
  return t;
}

/* Where to start a pool that has never been indexed. Binary search on
   timestamps rather than a fixed lookback, which would either miss history or
   scan a hundred thousand empty blocks. */
async function blockAtTime(ts) {
  let lo = 0, hi = await provider.getBlockNumber();
  const head = await provider.getBlock(hi);
  if (head && head.timestamp <= ts) return hi;
  for (let i = 0; i < 40 && lo < hi; i++) {
    const mid = Math.floor((lo + hi) / 2);
    const b = await provider.getBlock(mid);
    if (!b) { lo = mid + 1; continue; }
    if (b.timestamp < ts) lo = mid + 1; else hi = mid;
  }
  return Math.max(0, lo - 10);
}

async function listPools() {
  const n = Number(await factory.launchCount());
  const out = [];
  for (let i = 0; i < n; i++) {
    const r = await factory.launches(i);
    out.push({ pool: r.pool.toLowerCase(), createdAt: Number(r.createdAt) });
  }
  return out;
}

async function indexPool({ pool, createdAt }, head) {
  const st = await sbSelect(
    `index_state?pool=eq.${pool}&select=last_block&limit=1`);
  const last = st[0]?.last_block;

  let from = last ? Number(last) + 1 : await blockAtTime(createdAt);
  if (from > head) return 0;

  const c = new ethers.Contract(pool, POOL_ABI, provider);
  let step = CFG.logStep, rows = [], scanned = 0, reached = from - 1;

  while (from <= head && scanned < CFG.maxPerPass) {
    const to = Math.min(from + step - 1, head);
    let logs;
    try {
      logs = await c.queryFilter(c.filters.Swap(), from, to);
    } catch {
      // Providers cap log ranges and the cap is not always documented. Back
      // off rather than dropping the window, which is how the old frontend
      // chart silently lost every trade.
      if (step <= 10) { from = to + 1; continue; }
      step = Math.max(10, Math.floor(step / 4));
      continue;
    }

    for (const l of logs) {
      const ts = await timestampOf(l.blockNumber);
      const sqrt = Number(l.args.sqrtPriceX96);
      rows.push({
        pool,
        block: l.blockNumber,
        log_index: l.index,
        ts: new Date(ts * 1000).toISOString(),
        price: (sqrt ** 2) / 2 ** 192,
        amount_token: Number(l.args.amount0) / 1e18,
        amount_quote: Number(l.args.amount1) / 10 ** CFG.quoteDec,
        tx: l.transactionHash,
      });
    }

    scanned += to - from + 1;
    reached = to;
    from = to + 1;
    if (step < CFG.logStep) step = Math.min(CFG.logStep, step * 2);
  }

  if (rows.length) {
    // Upsert rather than insert: a pass can overlap a previous one after a
    // crash, and the primary key makes that a no-op instead of a duplicate.
    // Chunked because a long backfill can produce thousands of rows at once.
    for (let i = 0; i < rows.length; i += 500) {
      await sbUpsert("swaps", rows.slice(i, i + 500), "pool,block,log_index", true);
    }
  }

  await sbUpsert("index_state",
    [{ pool, last_block: reached, updated_at: new Date().toISOString() }], "pool");

  return rows.length;
}

async function pass() {
  const head = await provider.getBlockNumber();
  const pools = await listPools();
  let total = 0, touched = 0;

  for (const p of pools) {
    try {
      const n = await indexPool(p, head);
      if (n) { total += n; touched++; log(`  ${p.pool}  +${n} swaps`); }
    } catch (e) {
      log(`  ${p.pool}: ${e.shortMessage || e.message}`);
    }
  }
  if (total) log(`indexed ${total} swaps across ${touched} pools (head ${head})`);
}

async function main() {
  const net = await provider.getNetwork();
  log(`indexer on chain ${net.chainId}`);
  log(`factory ${FACTORY}`);
  log(`pools: ${(await listPools()).length}, polling every ${CFG.everySec}s`);

  await pass();
  setInterval(() => pass().catch(e => log("pass failed:", e.message)),
              CFG.everySec * 1000);
}

main().catch(e => { console.error(e); process.exit(1); });
