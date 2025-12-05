# Plugging a virtual Parquet server into Ducksite

This note sketches how to let a Python "virtual Parquet server" plugin surface remote or generated datasets without forcing every plugin author to reimplement Ducksite plumbing. The goal is to let a plugin declare what Parquet files should exist (and any per-file filters) so Ducksite can expose them through the existing virtual data map and `read_parquet` expansion logic.

## What Ducksite already does for Parquet files
- `build_symlinks` writes `static/data_map.json`, mapping logical HTTP paths such as `data/<file_source>/<relative_path>.parquet` to physical locations. The HTTP server streams files using that map; no OS symlinks are needed.
- `build_file_source_queries` reads `data_map.json` (when present) to generate `read_parquet([...])` expressions and optional templated views so DuckDB-Wasm can fetch the files via httpfs.
- `load_project_config` already supports `[[file_sources]]` entries with `pattern`, `upstream_glob`, and optional templating or row filters, so new data sources can flow through a single configuration surface.

## Minimal plugin contract
A virtual Parquet plugin only needs to return a manifest that Ducksite can translate into `data_map.json` entries:

```python
@dataclass
class VirtualParquetFile:
    http_path: str        # e.g. "data/sales/2024-01.parquet"
    physical_path: str    # local path to stream (can be temp)
    row_filter: str | None = None  # optional static predicate to AND into queries

@dataclass
class VirtualParquetManifest:
    files: list[VirtualParquetFile]
    template_name: str | None = None  # carry through to ducksite FileSourceConfig
    row_filter_template: str | None = None
```

Plugins can implement a simple hook such as `def build_manifest(cfg: ProjectConfig) -> VirtualParquetManifest:` and are free to:
- Materialize Parquet files locally (e.g., download from S3/BigQuery to a temp folder) and hand back the absolute paths.
- Point to already-existing Parquet locations mounted on the host.
- Provide `row_filter` on any file when the plugin knows a static constraint (date range, tenant id, etc.).

Ducksite can then write the manifest into `data_map.json` and merge the filter hints into the generated `NamedQuery` SQL without any per-plugin code paths.

## Wiring steps inside Ducksite (no new complexity per plugin)
1. **Manifest loader:** add a `plugin` key to `[[file_sources]]` in `ducksite.toml` that points to either `<path/to/file.py>:<callable>` or an importable module path. At build time, import it and call `build_manifest` instead of globbing `upstream_glob`.
2. **Data map population:** for each `VirtualParquetFile`, insert `http_path -> physical_path` into the same `data_map.json` structure produced by `build_symlinks` so the HTTP server remains unchanged.
3. **Query generation:** when `VirtualParquetFile.row_filter` is present, AND it into the base query generated for that file source. Keep templating behaviour unchanged so `template_name` and `row_filter_template` continue to build per-value views from the returned file list.
4. **Optional filtering at source:** allow plugins to accept a `filter_expr` string when they fetch or materialize data. Ducksite can pass through page-level or file-source-level hints (e.g., `time_window`) so remote pulls stay bounded without new SQL paths.
5. **Testing surface:** exercise plugins through existing Python tests by asserting that (a) `data_map.json` contains the returned HTTP paths, (b) the compiled SQL uses those paths in `read_parquet([...])`, and (c) `row_filter` values appear in the final WHERE clause when provided.

## Example plugin outline
```python
# my_project/plugins/sales_lake.py
from pathlib import Path
from ducksite.config import ProjectConfig
from ducksite.virtual_parquet import VirtualParquetFile, VirtualParquetManifest

def build_manifest(cfg: ProjectConfig) -> VirtualParquetManifest:
    scratch = Path(cfg.root) / "tmp_sales"
    scratch.mkdir(exist_ok=True)
    parquet_path = scratch / "sales-2024-01.parquet"
    fetch_sales_month(parquet_path)  # plugin-specific download or conversion
    return VirtualParquetManifest(
        files=[VirtualParquetFile(
            http_path="data/sales/sales-2024-01.parquet",
            physical_path=str(parquet_path),
            row_filter="sale_date between '2024-01-01' and '2024-01-31'",
        )],
        template_name="sales_[region]",
        row_filter_template="region = ?",
    )
```

With `ducksite.toml` containing:
```toml
[[file_sources]]
name = "sales"
plugin = "plugins/sales_lake.py:build_manifest"  # relative to ducksite.toml
```
Ducksite would call the plugin during build, write the resulting paths into `static/data_map.json`, and compile SQL that reuses all the existing `read_parquet` and templating machinery without any plugin-specific code.

## Import and path resolution rules
- If `plugin` looks like a path (has `/`, `\`, or ends with `.py`), Ducksite loads that file directly with `importlib.util.spec_from_file_location`, temporarily prepending the plugin directory to `sys.path` so sibling imports such as `from helpers import SHARED_CONST` work even when the plugin lives outside the repo.
- Module-style refs (no slashes) go through `importlib.import_module` with the project root temporarily at the front of `sys.path` so `python -m` style execution matches build-time behaviour.
- The callable name defaults to `build_manifest` when no `:callable` suffix is provided.
- Row filters returned per file are persisted to `static/data_map_meta.json` and ANDed into generated SQL so plugin-enforced predicates survive template/view expansion.
