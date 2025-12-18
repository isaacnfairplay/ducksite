# Ducksite virtual parquet plugins (deep dive)

Plugins let you describe data that is not already sitting in a local Parquet folder. They return a manifest of HTTP paths and physical files, and Ducksite reuses the existing virtual data map to serve and query them—no custom web server or SQL compiler code is needed. This guide focuses only on plugins and walks through concrete examples you can copy.

## Quick recap: the plugin contract
- Write a callable (defaults to `build_manifest`) that returns a `VirtualParquetManifest` with a list of `VirtualParquetFile` entries. Each file has the HTTP-facing `http_path`, a `physical_path` that Ducksite will stream, and an optional `row_filter` that is ANDed into generated SQL.
- In `ducksite.toml`, set `plugin = "<module_or_path>[:callable]"` on a `[[file_sources]]` entry. When present, Ducksite calls the plugin instead of globbing `upstream_glob`.
- Templating still works: the manifest can include `template_name` and `row_filter_template` so per-value views get built from the returned file list.
- Import rules match normal Python: module-style refs go through `importlib.import_module`, and explicit file paths are loaded directly with the plugin directory temporarily added to `sys.path` so sibling imports work.

## Scaffold one quickly
Use the CLI helper to generate a blank plugin file under `plugins/`:

```bash
ducksite add plugin --name sales_loader --directory plugins
```

Fill in the generated function to return a manifest; Ducksite will pick it up the next time you build.

## Example 1: fixed Parquet files you already have
Use this when you just need to expose an existing folder without changing the files:

```python
# plugins/static_sales.py
from ducksite.virtual_parquet import VirtualParquetFile, VirtualParquetManifest

def build_manifest(cfg):
    base = cfg.root / "warehouse" / "sales_parquet"
    files = []
    for rel in ["emea.parquet", "na.parquet"]:
        files.append(VirtualParquetFile(
            http_path=f"data/sales/{rel}",
            physical_path=str(base / rel),
            row_filter=None,  # no static predicate needed
        ))
    return VirtualParquetManifest(
        files=files,
        template_name="sales_[region]",
        row_filter_template="region = ?",
    )
```

`ducksite.toml`:
```toml
[[file_sources]]
name = "sales"
plugin = "plugins/static_sales.py"
```

Ducksite will stream the two Parquet files under `data/sales/` and build templated views like `sales_emea` and `sales_na`.

## Example 2: fetch and cache remote data
If the source lives in cloud storage or an API, fetch it into a scratch folder and return the local paths:

```python
# plugins/daily_pull.py
from pathlib import Path
from ducksite.virtual_parquet import VirtualParquetFile, VirtualParquetManifest

def build_manifest(cfg):
    scratch = Path(cfg.root) / ".ducksite_cache"
    scratch.mkdir(exist_ok=True)
    parquet_path = scratch / "daily.parquet"
    download_to_parquet("s3://bucket/daily-2024-08-01", parquet_path)  # your code
    return VirtualParquetManifest(files=[
        VirtualParquetFile(
            http_path="data/daily/daily.parquet",
            physical_path=str(parquet_path),
            row_filter="day = DATE '2024-08-01'",
        )
    ])
```

The static `row_filter` is merged into every query that touches this file source, so the API-enforced day boundary stays in place even after templating and view expansion.

## Example 3: reuse an existing file source under a new prefix
When you want the same files reachable at another HTTP path (for a downstream tool or a branch preview), reuse the helpers instead of rewriting globs:

```python
# plugins/demo_proxy.py
from ducksite.virtual_parquet import manifest_from_file_source

def build_manifest(cfg):
    return manifest_from_file_source(cfg, "demo", http_prefix="data/demo_proxy")
```

This mirrors every Parquet already registered for `file_sources.name == "demo"` under `data/demo_proxy/...` with zero extra scans.

## Example 4: chain off compiled model views
To expose the Parquet dependencies of specific models (for example, to share them with another DuckDB instance), chain through the model graph:

```python
# plugins/model_chain.py
from ducksite.virtual_parquet import manifest_from_model_views

def build_manifest(cfg):
    return manifest_from_model_views(
        cfg,
        ["orders_base", "orders_curated"],
        http_prefix="data/orders_models",
    )
```

Ducksite compiles the models, extracts every `read_parquet('data/...')` reference, and emits a manifest that preserves the original relative paths under the new prefix.

## Example 5: hierarchical lists from an API
You can still lean on templating when the file list comes from an API instead of the filesystem:

```python
# plugins/tenant_listing.py
from ducksite.virtual_parquet import VirtualParquetFile, VirtualParquetManifest

def build_manifest(cfg):
    manifest_rows = fetch_tenants_with_paths()  # returns [{"tenant": "a", "path": "..."}]
    files = [
        VirtualParquetFile(
            http_path=f"data/tenant/{row['tenant']}.parquet",
            physical_path=row["path"],
            row_filter=f"tenant = '{row['tenant']}'",
        )
        for row in manifest_rows
    ]
    return VirtualParquetManifest(
        files=files,
        template_name="tenant_[tenant]",
        row_filter_template="tenant = ?",
    )
```

Each tenant is addressable at `data/tenant/<id>.parquet`, and templated views like `tenant_a` are generated automatically with the per-tenant predicate enforced.

## Testing tips
- Unit-test the callable directly: assert that `VirtualParquetManifest.files` contains the expected HTTP paths, physical paths, and row filters.
- For integration coverage, run the normal Ducksite build and check that `.ducksite_data/data_map.sqlite` contains the plugin paths and that compiled SQL includes the `read_parquet` entries you returned.
- When a plugin materializes temporary files, clean them up in a test teardown or write to a temp directory under the project root so CI runs stay isolated.
