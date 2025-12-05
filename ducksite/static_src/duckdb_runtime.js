// ducksite/static_src/duckdb_runtime.js
// Initialize DuckDB-Wasm from the jsDelivr CDN and expose:
//   - initDuckDB() -> { db, conn }
//   - executeQuery(conn, sqlText) -> normalized row objects
//
// We *only* rely on httpfs-style access to Parquet via HTTP URLs.
// No OPFS, no registerOPFSFileName.

const DUCKDB_WASM_VERSION = "1.28.0";
const DEFAULT_DUCKDB_WASM_URL =
  `https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@${DUCKDB_WASM_VERSION}/+esm`;

let duckdbModulePromise = null;
let duckdbInstance = null;
let duckdbConnection = null;
let duckdbHttpfsConfigured = false;

export async function ensureHttpfs(conn) {
  if (duckdbHttpfsConfigured) {
    return;
  }
  console.debug("[ducksite] initDuckDB: enabling httpfs with metadata cache");
  await conn.query("LOAD httpfs;");
  await conn.query("SET enable_http_metadata_cache=true;");
  duckdbHttpfsConfigured = true;
}

async function loadDuckDBModule() {
  if (duckdbModulePromise) {
    return duckdbModulePromise;
  }

  const override =
    typeof window !== "undefined" && window.ducksiteDuckDBWasmUrl
      ? window.ducksiteDuckDBWasmUrl
      : null;

  const url = override || DEFAULT_DUCKDB_WASM_URL;
  console.debug("[ducksite] initDuckDB: importing DuckDB-Wasm from", url);

  duckdbModulePromise = import(url);
  return duckdbModulePromise;
}

export async function initDuckDB() {
  if (duckdbInstance && duckdbConnection) {
    console.debug("[ducksite] initDuckDB: reusing existing instance");
    return { db: duckdbInstance, conn: duckdbConnection };
  }

  const duckdb = await loadDuckDBModule();
  const bundles = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(bundles);

  const workerUrl = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], {
      type: "text/javascript",
    }),
  );

  try {
    const worker = new Worker(workerUrl);
    const logger = new duckdb.ConsoleLogger();
    const db = new duckdb.AsyncDuckDB(logger, worker);

    console.debug("[ducksite] initDuckDB: instantiating DuckDB-Wasm");
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

    const conn = await db.connect();
    await ensureHttpfs(conn);
    duckdbInstance = db;
    duckdbConnection = conn;

    console.debug("[ducksite] initDuckDB: DuckDB-Wasm ready");
    return { db, conn };
  } finally {
    URL.revokeObjectURL(workerUrl);
  }
}

// ---------- Result normalisation ----------
//
// We normalise DuckDB result values so downstream code never sees BigInt,
// NaN, Infinity, or exotic objects that ECharts / the DOM might choke on.

function normalizeScalar(value) {
  if (value === null || value === undefined) {
    return null;
  }

  const t = typeof value;

  if (t === "bigint") {
    const num = Number(value);
    if (Number.isSafeInteger(num)) {
      return num;
    }
    return value.toString();
  }

  if (t === "number") {
    if (!Number.isFinite(value)) {
      return null;
    }
    return value;
  }

  if (t === "string" || t === "boolean") {
    return value;
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (Array.isArray(value)) {
    return value.map((v) => normalizeScalar(v));
  }

  if (typeof value === "object" && value.toString !== Object.prototype.toString) {
    return value.toString();
  }

  return String(value);
}

function normalizeRows(rows) {
  return rows.map((row) => {
    const out = {};
    for (const [key, val] of Object.entries(row)) {
      out[key] = normalizeScalar(val);
    }
    return out;
  });
}

export async function executeQuery(conn, sqlText) {
  console.debug(
    "[ducksite] executeQuery: SQL (first 200 chars)",
    sqlText.slice(0, 200),
  );
  try {
    const result = await conn.query(sqlText);
    const rawRows = result.toArray();
    const rows = normalizeRows(rawRows);
    console.debug("[ducksite] executeQuery: returned rows", rows.length);
    return rows;
  } catch (e) {
    console.error("[ducksite] executeQuery: query error", e);
    throw e;
  }
}
