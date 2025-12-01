// ducksite/static_src/duckdb-bundle.js
// Small adapter that loads DuckDB-WASM from a CDN and exposes a single
// async function `createDuckDB()`.
//
// This keeps all heavy DuckDB/Arrow/WASM assets on the CDN while the
// rest of ducksite's runtime JS stays local.
//
// Contract (used by duckdb_runtime.js):
//   - export async function createDuckDB()
//   - returns { db, conn } where:
//       * db   is an AsyncDuckDB instance
//       * conn is a connection with async query(sqlText) -> DuckDBResult

const DUCKDB_WASM_CDN =
  "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.30.0/+esm";

let cachedPromise = null;

export async function createDuckDB() {
  if (cachedPromise) {
    return cachedPromise;
  }

  cachedPromise = (async () => {
    console.debug(
      "[ducksite] duckdb-bundle: importing DuckDB-WASM from",
      DUCKDB_WASM_CDN,
    );
    const duckdb = await import(DUCKDB_WASM_CDN);

    const bundles = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(bundles);
    console.debug("[ducksite] duckdb-bundle: selected bundle", bundle);

    const workerUrl = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`], {
        type: "text/javascript",
      }),
    );

    const worker = new Worker(workerUrl);
    const logger = new duckdb.ConsoleLogger();
    const db = new duckdb.AsyncDuckDB(logger, worker);

    try {
      await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    } finally {
      // We only need the Blob URL during instantiation.
      URL.revokeObjectURL(workerUrl);
    }

    const conn = await db.connect();
    console.debug("[ducksite] duckdb-bundle: database instantiated");

    // duckdb_runtime.js only cares about { db, conn }, but we also
    // return the module in case callers want metadata later.
    return { duckdb, db, conn };
  })();

  return cachedPromise;
}
