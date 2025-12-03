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

### How Ducksite interprets configuration
- `load_project_config` validates `ducksite.toml`, substitutes `DIR_*` variables inside `upstream_glob`, and prepares paths for the builder.
- During a build, `build_symlinks` scans each `upstream_glob`, preserves directory structure, and writes a virtual `static/data_map.json` so HTTP requests (and DuckDB’s `read_parquet`) resolve against the real files. Missing matches are allowed and logged.
- If you omit `file_sources`, Ducksite still builds successfully; only the data map will be empty.

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

Use the interactive TUY commands when you want to adjust common project files without opening an editor. Each command prompts for the values it needs and validates the updated file before saving it.

### `ducksite add|modify|remove toml`
- Operates on `ducksite.toml` at the project root and fails fast if that file is missing.
- `add` and `modify` prompt for a file-source name, pattern, optional `template_name`, and optional `upstream_glob`, then write or replace a single `[[file_sources]]` block. `modify` replaces the block that matches the provided `name`.
- `remove` asks for a file-source name and drops that block.
- Example session:
  ```bash
  ducksite add toml
  # prompts for name, pattern (defaults to data/*.parquet), template_name, and upstream_glob
  ```

### `ducksite add|modify|remove sql`
- Targets `sources_sql/models.sql`, creating `sources_sql/` if it does not exist.
- `add` appends a new `-- name:` block using the provided model name and SQL body.
- `modify` replaces the body of the named model; `remove` deletes the block entirely.
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
