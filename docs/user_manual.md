# Ducksite User Manual (Feature-by-Feature)

This guide assumes you installed **ducksite** from GitHub as a Python package (`pip install git+https://github.com/...`). It walks through two starting points—refreshing the demo scaffold or creating a project from scratch—then explains how to configure `ducksite.toml`, add SQL query files, author dashboard markdown, and serve the site.

## Project setup options

### Option A: extend the demo scaffold
1. Create or empty a working directory and run the initializer:
   ```bash
   ducksite init --root /path/to/project
   ```
2. The command lays down `ducksite.toml`, demo SQL models under `sources_sql/`, markdown dashboards under `content/`, and sample Parquet data under `fake_upstream/`. You can edit or replace any of these files to suit your data while keeping the same layout. 【F:ducksite/init_project.py†L4-L31】【F:ducksite/demo_init_toml.py†L8-L43】

### Option B: build from scratch
1. Start with an empty directory and create the core folders:
   ```bash
   mkdir -p content sources_sql static/forms
   ```
2. Add a minimal `ducksite.toml` (see the detailed configuration section below). For a pure SQL-only site you can omit `file_sources` entirely and still build pages.
3. Place your SQL model files in `sources_sql/` and markdown dashboards in `content/` as described later. The build step populates `static/` on demand, so you do not need to pre-create the rest of that folder.

## 1. Configure `ducksite.toml`

The configuration file lives at the project root and is required for every build. Ducksite loads it before any other step. 【F:ducksite/config.py†L46-L76】

### Required structure
- `[dirs]` section: define reusable path variables (only `DIR_*` names are allowed). Common entries include:
  - `DIR_FAKE` or other upstream roots pointing to your raw files.
  - `DIR_FORMS` for CSV targets created by form submissions (often `static/forms`).
- Optional `[[file_sources]]` array: each entry declares where Ducksite should look for upstream data files and how to expose them as HTTP-visible paths and SQL views. Keys include `name`, `template_name`, `pattern`, `upstream_glob`, `union_mode`, `row_filter`, `row_filter_template`, and `on_empty`. 【F:ducksite/config.py†L52-L114】

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
This setup mirrors every `fake_upstream/demo-*.parquet` file into the generated `static/data/demo/` namespace and automatically creates per-category templated views such as `demo_A` or `demo_B`. Ducksite no longer writes operating-system symlinks or copies of the data; it only records the real file locations in a virtual map that the HTTP server streams directly. 【F:ducksite/demo_init_toml.py†L15-L43】【F:ducksite/symlinks.py†L54-L112】【F:ducksite/builder.py†L260-L370】

### How Ducksite interprets configuration
- `load_project_config` validates `ducksite.toml`, substitutes `DIR_*` variables inside `upstream_glob`, and prepares paths for the builder. 【F:ducksite/config.py†L78-L98】
- During a build, `build_symlinks` scans each `upstream_glob`, preserves directory structure, and writes a virtual `static/data_map.json` so HTTP requests (and DuckDB’s `read_parquet`) resolve against the real files. Missing matches are allowed and logged. 【F:ducksite/symlinks.py†L54-L112】
- If you omit `file_sources`, Ducksite still builds successfully; only the data map will be empty.

## 2. Add SQL query files (`sources_sql/*.sql`)

SQL model files are optional but recommended for reusable logic. Ducksite reads every `*.sql` file under `sources_sql/` and extracts blocks marked with `-- name: <identifier>`. Each block becomes a named model that other models, file-source views, or page-level queries can reference. 【F:ducksite/queries.py†L427-L455】

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
After a build, the compiled SQL (with metrics headers) appears under `static/sql/_global/` and is listed in `static/sql/_manifest.json`. 【F:ducksite/builder.py†L231-L246】【F:ducksite/cte_compiler.py†L193-L240】

## 3. Add dashboard markdown (`content/**/*.md`)

Every markdown file beneath `content/` becomes a page. Ducksite scans these files, extracts SQL, input, grid, and visualization blocks, compiles any per-page SQL, and emits HTML into `static/` preserving relative paths. 【F:ducksite/builder.py†L258-L302】

### Page anatomy
- SQL blocks: ```sql <id> ... ``` define page-level queries. They are compiled and written to `static/sql/<page_path>/<id>.sql` during the build.
- Visualization blocks (e.g., ```echart ...``` or ```table ...```) bind to `data_query` ids and render charts or tables in the generated HTML. See the demo pages for working patterns. 【F:ducksite/demo_init_content.py†L6-L64】
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
When built, this page becomes `static/index.html` with the compiled SQL beside it at `static/sql/index/inventory_rows.sql`. 【F:ducksite/builder.py†L277-L301】

## 4. Build and serve the site

### Build
Run the builder whenever you change configuration, models, or content:
```bash
ducksite build --root /path/to/project
```
The command cleans `static/`, copies JS assets, writes the data map, compiles SQL, and emits HTML under `static/`. 【F:ducksite/builder.py†L228-L305】

### Auto-reload while authoring
```bash
ducksite build --root /path/to/project --reload
```
`--reload` watches for changes and rebuilds automatically. 【F:ducksite/cli.py†L12-L39】

### Serve
After building, start the local server from the project root:
```bash
ducksite serve --root /path/to/project --port 8080 --server builtin
```
- `--server builtin` uses the bundled threaded HTTP server; `--server uvicorn` is available if you installed the extra dependency.
- The server hosts `static/` and form endpoints, and logs any errors for missing models or forms. Stop with `Ctrl+C`. 【F:ducksite/cli.py†L9-L39】【F:ducksite/builder.py†L310-L370】

## Workflow checklist
1. Initialize (demo or scratch) and confirm `ducksite.toml` exists.
2. Fill in `[dirs]` and any `[[file_sources]]` entries, using `${DIR_*}` variables for portability.
3. Add or edit `sources_sql/*.sql` model blocks with `-- name:` headers.
4. Create `content/` markdown pages with SQL + viz + grid blocks.
5. Run `ducksite build --root .` and open the generated `static/*.html` pages.
6. Serve locally with `ducksite serve --root . --port 8080` when you are ready to demo.
