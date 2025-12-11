# Ducksite User Manual (Feature-by-Feature)

This guide assumes you installed **ducksite** from GitHub as a Python package (`pip install git+https://github.com/...`). It walks through two starting points—refreshing the demo scaffold or creating a project from scratch—then explains how to configure `ducksite.toml`, add SQL query files, author dashboard markdown, use the TUY helpers, and serve the site.

## Project setup options

### Option A: build from scratch (recommended)
1. Start with an empty directory and run the barebones initializer:
   ```bash
   ducksite init --root /path/to/project
   ```
   This writes a minimal `ducksite.toml` plus `content/`, `sources_sql/`, and `static/forms/` directories with no demo files.
2. Place your SQL model files in `sources_sql/` and markdown dashboards in `content/` as described later. The build step populates `static/` on demand, so you do not need to pre-create the rest of that folder.

### Option B: extend the demo scaffold
1. Create or empty a working directory and run the demo bootstrapper:
   ```bash
   ducksite demo --root /path/to/project
   ```
2. The command first runs the same barebones initializer, then lays down `ducksite.toml`, demo SQL models under `sources_sql/`, markdown dashboards under `content/`, and sample Parquet data under `fake_upstream/`. You can edit or replace any of these files to suit your data while keeping the same layout.

## 1. Configure `ducksite.toml`

The configuration file lives at the project root and is required for every build. Ducksite loads it before any other step.

### Required structure
- `[dirs]` section: define reusable path variables (only `DIR_*` names are allowed). Common entries include:
  - `DIR_FAKE` or other upstream roots pointing to your raw files.
  - `DIR_FORMS` for CSV targets created by form submissions (often `static/forms`).
- Optional `[[file_sources]]` array: each entry declares where Ducksite should look for upstream data files and how to expose them as HTTP-visible paths and SQL views. Keys include `name`, `template_name`, `pattern`, `upstream_glob`, `union_mode`, `row_filter`, `row_filter_template`, and `on_empty`.

### Example (demo-style file source)
```toml
# ducksite.toml
[dirs]
DIR_FAKE = "fake_upstream"
DIR_FORMS = "static/forms"

[[file_sources]]
name = "demo"
template_name = "demo_[category]"
pattern = "data/demo/*.parquet"
upstream_glob = "${DIR_FAKE}/demo-*.parquet"
row_filter_template = "category = ?"
```
This setup mirrors every `fake_upstream/demo-*.parquet` file into the generated `static/data/demo/` namespace and automatically creates per-category templated views such as `demo_A` or `demo_B`. Ducksite no longer writes operating-system symlinks or copies of the data; it only records the real file locations in a virtual map that the HTTP server streams directly.

### Multi-level file sources (temporal or other rollups)
Add a `hierarchy` array when you want one logical source to stitch together multiple folder levels. The common case is temporal (day → month → year), but any ordered rollup works the same way:
```toml
[[file_sources]]
name = "orders"
template_name = "orders_[concat(region, '_', strftime(max_day, '%Y-%m-%d'))]"
row_filter = "active = true" # applied to every level
row_filter_template = "concat(region, '_', strftime(max_day, '%Y-%m-%d')) = ?" # appended for each templated view
template_values = ["emea_2024-12-05"] # seed per-value views when sampling is not possible
template_values_sql = """
  SELECT region, DATE '2024-12-06' AS max_day
  FROM (VALUES ('emea'), ('na')) t(region)
""" # optional: multi-column seed rows evaluated in DuckDB
hierarchy_before = [
  { pattern = "data/orders/day/*.parquet", row_filter = "period = 'edge-day'" },
]
hierarchy = [
  { pattern = "data/orders/day/*.parquet",   row_filter = "period = 'day'" },
  { pattern = "data/orders/month/*.parquet", row_filter = "period = 'month'" },
  { pattern = "data/orders/year/*.parquet",  row_filter = "period = 'year'" },
]
hierarchy_after = [
  { pattern = "data/orders/day/*.parquet", row_filter = "period = 'edge-day'" },
]
```
- Ducksite unions every level in order and ANDs the `row_filter` from the file source with the `row_filter` on each level.
- When `template_name` is set, Ducksite samples distinct values across all levels (via DuckDB) and emits per-value views whose predicates include both the base `row_filter` and the per-level filter.
- Provide `template_values` to force specific templated views to materialise even when build-time sampling cannot reach the data (for example, creating a view for a given date while the fresh daily file has not been uploaded yet, or pre-seeding a particular region/date pair). If you need a repeatable list of combinations, set `template_values_sql` to a DuckDB query that returns one or more columns; each row seeds a templated view and is substituted into the `row_filter_template` in order.
- Use `hierarchy_before` or `hierarchy_after` when you want higher-fidelity endpoints around a temporal rollup (e.g., day files for the first and last weeks) while keeping coarser month/year files in the middle of the list.
- If you omit `hierarchy`, Ducksite falls back to the legacy `pattern` field so existing projects keep working unchanged.

The hierarchy itself is agnostic to time: you can point it at fidelity levels (raw → cleaned → curated) or region rollups (store → country → global) as long as the list is ordered from most-granular to most-aggregated. For temporal data, the naming makes that ordering obvious; for other uses, consider documenting the level semantics alongside the config to avoid confusion.

This format helps you avoid scanning thousands of tiny day files for historical data. A representative benchmark on a small cloud VM with ~2.2 million rows spread across 731 day files showed that a naive "all days" scan took ~68 ms, while a hierarchical read that used December day files + month aggregates for the rest of 2024 + a single yearly file for 2023 returned the same result in ~7 ms (about 10x faster) because far fewer files were touched.

For smaller windows, the difference still shows up: a 35-day window that overlapped two months dropped from ~19 ms (35 day files) to ~6 ms (two monthly files plus five recent day files) on the same VM.

```mermaid
flowchart TD
  C[ducksite.toml with template_name<br/>and hierarchy levels] --> Q[build_file_source_queries()<br/>expands patterns]
  Q --> S[DuckDB DISTINCT sampler<br/>+ template_values seeding]
  S --> V[NamedQuery entries:<br/>base view + per-value templated views]
  V --> G[build_project()<br/>compile_query(...)]
  G --> W[static/sql/_global/*.sql<br/>and per-page SQL files]
  W --> M[static/sql/_manifest.json<br/>lists query kinds and deps]
  M --> B[Browser SQL editor fetches manifest]<br/>
  W --> H[HTTP server streams Parquet via virtual data map]
```

The demo project ships two hierarchy pages: one mirrors the simple day/month/year rollup (`/hierarchy/`), and another layers before/after endpoint days around the rollup with templated region/date names (`/hierarchy_window/`).

### How Ducksite interprets configuration
- `load_project_config` validates `ducksite.toml`, substitutes `DIR_*` variables inside `upstream_glob`, and prepares paths for the builder.
- During a build, `build_symlinks` scans each `upstream_glob`, preserves directory structure, and writes a virtual `.ducksite_data/data_map.json` (plus a SQLite-backed `.ducksite_data/data_map.sqlite` for fast lookups) outside the served `static/` directory. When the file-source configuration is unchanged, the builder reuses the existing map instead of rescanning large upstream directories. Missing matches are allowed and logged.
- If you omit `file_sources`, Ducksite still builds successfully; only the data map will be empty.

### Virtual hierarchies and plugins

Hierarchical file sources work with both static file maps and plugin-provided data. A plugin can emit the same `data_map.json` entries (or HTTP handlers) that the static builder writes, so templated hierarchy views resolve against virtual or remote storage without copying data locally. If you need to generate the hierarchy list dynamically (for example, using signed URLs per request), implement a plugin that mirrors the structure of `build_file_source_queries`—returning the level patterns, row filters, and template substitutions—and reuse the browser manifest flow to keep compiled SQL in sync.

## 2. Add SQL query files (`sources_sql/*.sql`)

SQL model files are optional but recommended for reusable logic. Ducksite reads every `*.sql` file under `sources_sql/` and extracts blocks marked with `-- name: <identifier>`. Each block becomes a named model that other models, file-source views, or page-level queries can reference.

### Authoring guidelines
- Use consecutive `-- name:` markers to start each model block; Ducksite treats the lines until the next marker (or EOF) as the SQL body and strips trailing semicolons.
- Models can depend on earlier ones by name (e.g., `SELECT * FROM base_model`). Cycles raise errors during compilation.
- Keep filenames meaningful; all models across files share one namespace.

### Example model file
```sql
-- name: numbers
select * from (values (1), (2), (3)) as t(n)

-- name: numbers_stats
select count(*) as total from numbers
```
After a build, the compiled SQL (with metrics headers) appears under `static/sql/_global/` and is listed in `static/sql/_manifest.json`.

## 3. Add dashboard markdown (`content/**/*.md`)

Every markdown file beneath `content/` becomes a page. Ducksite scans these files, extracts SQL, input, grid, and visualization blocks, compiles any per-page SQL, and emits HTML into `static/` preserving relative paths.

### Page anatomy
- SQL blocks: ```sql <id> ... ``` define page-level queries. They are compiled and written to `static/sql/<page_path>/<id>.sql` during the build.
- Visualization blocks (e.g., ```echart ...``` or ```table ...```) bind to `data_query` ids and render charts or tables in the generated HTML. See the demo pages for working patterns.
- Layout blocks: ```grid cols=<n> gap=<size>``` arrange components; cell ids must match the surrounding SQL or viz blocks.

### Example page
```markdown
# Inventory Dashboard

```sql inventory_rows
select * from numbers
```

```grid cols=12 gap=md
| inventory_rows:12 |
```
```
When built, this page becomes `static/index.html` with the compiled SQL beside it at `static/sql/index/inventory_rows.sql`.

### Filters and dropdown inputs

Declare filters with fenced `input` blocks in your markdown. A `visual_mode` of `dropdown` renders a `<select>` in the input bar and feeds values into `${params.*}` placeholders via the `expression_template`:

````markdown
```input category_filter
label: Category filter
visual_mode: dropdown
options_query: category_filter_options
expression_template: "category = ?"
all_label: ALL
all_expression: "TRUE"
```
````

Set `multiple: true` to allow multi-select dropdowns. The selected values are treated as arrays, synced to the URL as comma-separated strings, and substituted into the `expression_template` with quoting. For multi-select filters, prefer an `IN (?)` predicate so multiple values stay valid SQL:

````markdown
```input region_filter
label: Regions
visual_mode: dropdown
multiple: true
options_query: region_options
expression_template: "region IN (?)"
all_label: ALL
all_expression: "TRUE"
```
````

## 4. Build and serve the site

### Build
Run the builder whenever you change configuration, models, or content:
```bash
ducksite build --root /path/to/project
```
The command cleans `static/`, copies JS assets, writes the data map, compiles SQL, and emits HTML under `static/`.

### Auto-reload while authoring
```bash
ducksite build --root /path/to/project --reload
```
`--reload` watches for changes and rebuilds automatically.

### Serve
After building, start the local server from the project root:
```bash
ducksite serve --root /path/to/project --port 8080 --server builtin
```
- `--server builtin` uses the bundled threaded HTTP server; `--server uvicorn` is available if you installed the extra dependency.
- The server hosts `static/` and form endpoints, and logs any errors for missing models or forms. Stop with `Ctrl+C`.

## 5. TUY helpers for quick edits

Use the interactive TUY commands when you want to adjust common project files without opening an editor. The Textual interface supports multiline editing, color hints, and keyboard shortcuts (Tab/Ctrl+N/Ctrl+P to move, Ctrl+S to save) and validates every change with the same checks the build uses so errors surface immediately.

### `ducksite add|modify|remove toml`
- Operates on `ducksite.toml` at the project root and fails fast if that file is missing.
- `add` and `modify` prompt for either a `[dirs]` entry (DIR_* constants such as `DIR_FORMS = "static/forms"`, useful for templated paths like `{DIR_FORMS}/myform/*.csv`) or a `[[file_sources]]` block. Inputs are validated through `load_project_config`, so undefined `DIR_*` placeholders or malformed file-source options are caught before saving.
- `remove` asks for a directory constant or a file-source name and drops that block.
- Example session:
  ```bash
  ducksite add toml
  # prompts for name, pattern (defaults to data/*.parquet), template_name, and upstream_glob
  ```

### `ducksite add|modify|remove sql`
- Targets `sources_sql/models.sql`, creating `sources_sql/` if it does not exist.
- `add` appends a new `-- name:` block using the provided model name and SQL body. Multiline entry is supported.
- `modify` replaces the body of the named model; `remove` deletes the block entirely. Models are validated by loading them through the same path used during `ducksite build`.
- Example session:
  ```bash
  ducksite modify sql
  # prompts for the model name to change and the replacement SQL body
  ```

### `ducksite add|modify|remove md`
- Works on `content/page.md`, creating `content/` if needed.
- `add` appends a fenced block (SQL or visualization) with the specified type and id. `modify` rewrites the matching block body. `remove` deletes the matching block.
- Example session:
  ```bash
  ducksite remove md
  # prompts for block type (sql/echart/table) and the block id to drop
  ```

## Workflow checklist
1. Initialize (demo or scratch) and confirm `ducksite.toml` exists.
2. Fill in `[dirs]` and any `[[file_sources]]` entries, using `${DIR_*}` variables for portability.
3. Add or edit `sources_sql/*.sql` model blocks with `-- name:` headers.
4. Create `content/` markdown pages with SQL + viz + grid blocks.
5. Use `ducksite add|modify|remove toml|sql|md` for quick adjustments without manually editing files.
6. Run `ducksite build --root .` and open the generated `static/*.html` pages.
7. Serve locally with `ducksite serve --root . --port 8080` when you are ready to demo.
