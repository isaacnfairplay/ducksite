# Ducksearch deep demos

The `ducksearch/demos/deep_demos` root ships ready-to-run exercises that stress fast paths (materializations, bindings, imports, hybrid parameters) and show how quickly ducksearch produces shareable Parquet artifacts. Each demo sticks to the published layout (`config.toml`, `reports/`, `composites/`, `cache/`) so you can copy the root to temp storage or run it in place.

## Quick start

1. Point ducksearch at the bundled root and lint it to confirm the metadata and SQL are valid:
   ```bash
   DEMO_ROOT=ducksearch/demos/deep_demos
   python -m ducksearch.cli lint --root "$DEMO_ROOT"
   ```
2. Serve the demos on a local port:
   ```bash
   python -m ducksearch.cli serve --root "$DEMO_ROOT" --port 8081 --host 127.0.0.1
   ```
3. Hit the `/report` endpoint with the relative `report` path shown in each section. Every request returns JSON pointing at Parquet artifacts under `cache/`; reusing the same parameters reuses the same cached files.

Tip: the repo ignores generated Parquet files under `ducksearch/demos/deep_demos/cache/`, but you can also copy the entire root to `/tmp` before serving if you want to keep the working tree pristine.

### HTML preview API

Add `format=html` to any `/report` URL to see a small, self-contained preview page. The server still builds the Parquet artifacts first, then returns HTML that links to the base Parquet plus any materializations, literal sources, and bindings. The page bootstraps DuckDB-Wasm from jsDelivr and renders up to 200 rows from the `base_parquet` artifact directly in the browser.

Example:

```
http://127.0.0.1:8081/report?report=deep_demos/speed/rolling_latency.sql&Region=north&DayWindow=2&format=html
```

Use this when you want to share a repro URL with a built-in table preview instead of just JSON.
Type in the preview search box to quickly filter the returned rows client-side without another request.

## Demo 1: rolling latency lane (materializations + config defaults)

* **Report:** `deep_demos/speed/rolling_latency.sql`
* **Highlights:** `MATERIALIZE`/`MATERIALIZE_CLOSED` CTEs, optional data parameters with defaults, `CONFIG` placeholders, literal sources for UI pickers.

The report builds four small regional time series, pre-materializes them, then filters and ranks rows with a rolling latency window that falls back to the configured default when no client window is provided. Try a targeted request:

```bash
curl "http://127.0.0.1:8081/report?report=deep_demos/speed/rolling_latency.sql&Region=north&DayWindow=2" | jq .
```

Key observations:

* The JSON payload returns a `base_parquet` path plus a `materialize` map with the rolling window intermediate—both live under `cache/`, so reloads hit cached Parquet instead of recomputing the window.
* Adjusting `DayWindow` changes the cache key (`__` hash suffix) and writes a new Parquet file; repeating the same URL keeps the path stable for five minutes (the default cache TTL).
* The `LITERAL_SOURCES` block exposes region values generated entirely in SQL, so the UI can render picker options without an extra request.

## Demo 2: binding-driven focus with hybrid parameters

* **Report:** `deep_demos/bindings/segment_focus.sql`
* **Highlights:** `BINDINGS` that map server-filtered IDs to friendly labels, hybrid parameters that can stay client-only, and ranking over cross-joined shards.

The binding ties the `Segment` parameter to a lookup table and injects the friendly label back into the result via `{{bind segment_label}}`. Hybrid `Shard` filtering illustrates client vs server application:

```bash
# Server-side shard filter
curl "http://127.0.0.1:8081/report?report=deep_demos/bindings/segment_focus.sql&Segment=alpha&Shard=2" | jq .

# Client-only shard hint (server keeps the wide slice)
curl "http://127.0.0.1:8081/report?report=deep_demos/bindings/segment_focus.sql&Segment=alpha&__client__Shard=2" | jq .
```

Compare the two responses: the server-filtered request emits a single shard row and a smaller `base_parquet`, while the client-only hint returns every shard for the `alpha` segment. Both reuse the same binding Parquet file, demonstrating server-side key resolution regardless of client filters.

## Demo 3: import fan-out with pass-through parameters

* **Reports:** `deep_demos/imports/shared_base.sql` (source) and `deep_demos/imports/topic_drilldown.sql` (consumer)
* **Highlights:** `IMPORTS` with `pass_params`, multi-level materializations, and config-driven constants shared across imports.

The shared base builds a small workload table and materializes it. The drilldown report imports that base, applies an optional variant filter, and echoes the configured default region into every row to prove config values flow through imports.

The import placeholder sits inside a string literal (`parquet_scan('{{import stories}}')`) to satisfy the Parquet path lint rule while still swapping in the generated artifact path at runtime.

Example requests:

```bash
# Routing-only base with beta focus
curl "http://127.0.0.1:8081/report?report=deep_demos/imports/topic_drilldown.sql&Topic=routing&FocusVariant=beta" | jq .

# Ingest slice (different cache key and Parquet path)
curl "http://127.0.0.1:8081/report?report=deep_demos/imports/topic_drilldown.sql&Topic=ingest" | jq .
```

Because `Topic` is passed through to the import, changing it produces a new cache hash for both the imported artifact and the top-level `base_parquet`. Repeating the same URL reuses both files, giving a true “warm reload” path when you pin a URL in the browser.

## Performance tips visible in the demos

* Parquet artifacts refresh only when the cache TTL expires (300 seconds by default) or when parameter/config/import inputs change. Warm requests hit the existing files directly.
* The demo reports lean on `MATERIALIZE` for expensive work and keep predicates simple so DuckDB can stream to Parquet quickly; mirror that pattern in your own reports for interactive snappiness.
