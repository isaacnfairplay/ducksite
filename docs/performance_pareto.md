# Demo chart load Pareto snapshot

The `tools/performance_probe.py` script times a full demo build, starts the builtin threaded server, then measures initial and cached fetches for the assets that drive the charts alongside the three demo parquet reads. In this environment the DuckDB `httpfs` extension could not be fetched, so parquet reads fell back to the generated local files; the relative ordering still highlights the biggest contributors for a cold refresh and cached reloads.

## Initial refresh (no cache headers sent)

| Rank | Step | Duration (ms) | Share of total |
| --- | --- | --- | --- |
| 1 | `/index.html` | 13.19 | 52% |
| 2 | `/js/ducksite_contract.js` | 3.83 | 15% |
| 3 | `/js/duckdb-bundle.js` | 2.69 | 11% |
| 4 | `/js/echarts.min.js` | 2.23 | 9% |
| 5 | `demo-A query` | 1.57 | 6% |
| 6 | `demo-B query` | 0.84 | 3% |
| 7 | `demo-C query` | 0.78 | 3% |

## Cached reload (If-Modified-Since sent)

| Rank | Step | Duration (ms) | Share of total |
| --- | --- | --- | --- |
| 1 | `/index.html` (304) | 2.71 | 21% |
| 2 | `/js/ducksite_contract.js` (304) | 2.47 | 19% |
| 3 | `/js/duckdb-bundle.js` (304) | 2.38 | 18% |
| 4 | `/js/echarts.min.js` (304) | 2.28 | 18% |
| 5 | `demo-A query` | 1.57 | 12% |
| 6 | `demo-B query` | 0.84 | 6% |
| 7 | `demo-C query` | 0.78 | 6% |

## Cached reload (no validators sent)

| Rank | Step | Duration (ms) | Share of total |
| --- | --- | --- | --- |
| 1 | `/js/duckdb-bundle.js` | 3.23 | 23% |
| 2 | `/js/echarts.min.js` | 2.75 | 19% |
| 3 | `/index.html` | 2.50 | 18% |
| 4 | `/js/ducksite_contract.js` | 2.49 | 18% |
| 5 | `demo-A query` | 1.57 | 11% |
| 6 | `demo-B query` | 0.84 | 6% |
| 7 | `demo-C query` | 0.78 | 6% |

### Takeaways

* First paint remains dominated by HTML and JS; caching trims total load time from ~25 ms on first paint to ~13 ms when assets revalidate.
* Conditional reloads beat unconditional reloads by ~8% (13.0 ms vs. 14.2 ms total) because validators let the server reply with lightweight 304s instead of regenerating bodies.
* Gzip cuts JS payload sizes substantially: `duckdb-bundle.js` shrinks ~54% (1.9 KB → 0.9 KB) while the contract bundle drops ~27%.
* Keep-alive plus immutable caching keep connections warm and steer browsers away from redundant range/HEAD work; even when validators are omitted HTML/JS still dominate, so keeping those bundles cached and compressed matters most.
* Parquet scans stay under 2 ms even without HTTPFS; with the metadata cache enabled (when available) the query component should shrink further because repeated range lookups avoid extra HEAD probes.

## Branch progress to date

* Added coverage and server behavior for HTTP range handling, `If-Modified-Since` validation, and httpfs-backed DuckDB queries to keep reloads predictable.
* Introduced cache headers with long-lived immutable policies for JS/CSS/data assets while keeping HTML short-lived, removing most conditional fetch overhead on reloads.
* Instrumented demo performance probes to report Pareto breakdowns for cold and cached loads (with and without validators) and centralized shared asset/scheme strings to reduce regression risk in tests.
