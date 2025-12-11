from __future__ import annotations
from pathlib import Path
import glob
import hashlib
import json
import sqlite3

from .config import FileSourceConfig, ProjectConfig
from .data_map_paths import data_map_shard, data_map_sqlite_path
from .data_map_cache import load_fingerprint
from .virtual_parquet import (
    VirtualParquetManifest,
    load_virtual_parquet_manifest,
    write_row_filter_meta,
)
from .utils import ensure_dir


def _common_non_wild_root(pattern: str) -> Path:
    """
    Given a glob pattern like:

        /mnt/data/raw/defect/*.parquet
        /data/scrap/month=*/data.parquet
        C:/logs/station*/2025-*/events.csv

    return the longest prefix BEFORE the first wildcard segment.

    We only use this to compute a stable "base root" so we can preserve
    the *relative* directory structure of upstream files inside a
    logical root named after the file_source, for example:

        [[file_sources]]
        name = "defect_management_demo"
        upstream_glob = "/data/warehouse/defect/*.parquet"

    with files:

        /data/warehouse/defect/0.parquet
        /data/warehouse/defect/month=2025-01/data.parquet

    yields logical paths:

        data/defect_management_demo/0.parquet
        data/defect_management_demo/month=2025-01/data.parquet
    """
    p = Path(pattern)
    parts = p.parts
    out_parts = []

    for part in parts:
        if any(ch in part for ch in "*?[]"):
            break
        out_parts.append(part)

    if not out_parts:
        return p.parent

    return Path(*out_parts)


def build_symlinks(cfg: ProjectConfig) -> None:
    """
    Build a *virtual* symlink map from HTTP paths under /data/... to
    upstream filesystem paths, and write it to the private cache:

        <root>/.ducksite_data/data_map.sqlite

    The HTTP file power-router will then use this map at runtime to serve
    real files without creating any copies or OS-level symlinks.

    Layout of keys:

        data/<file_source_name>/<relative_path_under_base_root>

    Example:

        [[file_sources]]
        name = "demo"
        upstream_glob = "fake_upstream/demo-*.parquet"

      fake_upstream/demo-data.parquet
        -> key: "data/demo/demo-data.parquet"

        [[file_sources]]
        name = "defect_management_demo"
        upstream_glob = "\\\\server\\share\\data\\defect\\*.parquet"

      \\server\\share\\data\\defect\\0.parquet
        -> key: "data/defect_management_demo/0.parquet"

      \\server\\share\\data\\defect\\month=2025-01\\data.parquet
        -> key: "data/defect_management_demo/month=2025-01/data.parquet"

    These keys are exactly what DuckDB-Wasm will request via httpfs:

        read_parquet(['data/defect_management_demo/0.parquet', ...])

    The Python HTTP server sees `/data/defect_management_demo/0.parquet`,
    looks up the physical path in data_map.sqlite, and streams that file.

    NOTE:
      - We do not touch the filesystem under site_root/data at all.
      - This function is idempotent and safe to re-run on each build.
    """
    site_root = cfg.site_root
    ensure_dir(site_root)

    scan_cache = _collect_upstream_matches(cfg)

    print("[ducksite] data map: computing fingerprint")
    fingerprint = _file_source_fingerprint(cfg, scan_cache)
    existing_fp = _load_existing_fingerprint(site_root)
    sqlite_path = data_map_sqlite_path(site_root)

    if existing_fp == fingerprint and sqlite_path.exists():
        print("[ducksite] data map unchanged; reusing existing data_map.sqlite")
        return

    data_map: dict[str, str] = {}
    row_filters: dict[str, str] = {}

    def ingest_manifest(
        fs_cfg: FileSourceConfig | None, fs_name: str | None, manifest: VirtualParquetManifest
    ) -> None:
        if manifest.template_name and fs_cfg and not fs_cfg.template_name:
            fs_cfg.template_name = manifest.template_name
        if manifest.row_filter_template and fs_cfg and not fs_cfg.row_filter_template:
            fs_cfg.row_filter_template = manifest.row_filter_template

        fs_root = Path("data") / (fs_name or "")
        added = 0
        for f in manifest.files:
            key = (fs_root / f.http_path).as_posix() if not f.http_path.startswith("data/") else f.http_path
            if key in data_map and data_map[key] != f.physical_path:
                print(
                    f"[ducksite] WARNING: duplicate virtual path {key}; "
                    f"overwriting {data_map[key]} with {f.physical_path}"
                )
            data_map[key] = f.physical_path
            if f.row_filter:
                row_filters[key] = f.row_filter
            added += 1

        if added:
            print(
                f"[ducksite] data map: registered {added} plugin entries for "
                f"{fs_name or '<unnamed>'}"
            )

    for fs in cfg.file_sources:
        if fs.plugin:
            print(
                f"[ducksite] data map: loading plugin manifest for {fs.name or '<unnamed>'}"
            )
            manifest = load_virtual_parquet_manifest(fs.plugin, cfg)
            ingest_manifest(fs, fs.name, manifest)
            continue

        if not fs.upstream_glob:
            continue

        scan = scan_cache.get(id(fs))

        if scan is None:
            up = Path(fs.upstream_glob)
            pattern = str(up) if up.is_absolute() else str(cfg.root / up)
            matches = []
            scan_error = False
        else:
            pattern = scan["pattern"]
            matches = scan["matches"]
            scan_error = scan["error"]

        if scan_error:
            continue

        print(
            f"[ducksite] data map: scanning upstream files for {fs.name or '<unnamed>'} "
            f"(pattern: {pattern}, matches: {len(matches)})"
        )

        if not matches:
            print(
                f"[ducksite] INFO: no upstream files matched {pattern}; "
                f"file_source {fs.name or '<unnamed>'} will have no mirrored data."
            )
            continue

        base_root = _common_non_wild_root(pattern)

        # Logical root for this file_source under /data; default to flat if unnamed.
        fs_root = Path("data") / (fs.name or "")

        mapped_count = 0
        for src_path_str in matches:
            src = Path(src_path_str)
            if not src.is_file():
                continue

            # Preserve directory structure relative to base_root, but always
            # rooted under data/<fs.name>/...
            try:
                rel = src.relative_to(base_root)
            except ValueError:
                # Fallback: flatten if the root isn't actually a parent
                rel = Path(src.name)

            key_path = fs_root / rel
            key = key_path.as_posix()

            # Last one wins if there is a collision; we log it for visibility.
            if key in data_map and data_map[key] != str(src):
                print(
                    f"[ducksite] WARNING: duplicate virtual path {key}; "
                    f"overwriting {data_map[key]} with {src}"
                )

            data_map[key] = str(src)
            mapped_count += 1

        if mapped_count:
            print(
                f"[ducksite] data map: registered {mapped_count} entries for "
                f"{fs.name or '<unnamed>'}"
            )

    # Write the virtual symlink map for the HTTP server.
    print("[ducksite] data map: writing sqlite index")
    _write_sqlite_map(site_root, data_map)
    write_row_filter_meta(site_root, row_filters, fingerprint=fingerprint)
    print(f"[ducksite] wrote virtual data map ({len(data_map)} entries)")


if __name__ == "__main__":
    from .config import load_project_config

    root = Path(".").resolve()
    cfg = load_project_config(root)
    build_symlinks(cfg)
    print("Virtual data map built at", data_map_sqlite_path(cfg.site_root))


def _collect_upstream_matches(cfg: ProjectConfig) -> dict[int, dict[str, object]]:
    matches_by_fs: dict[int, dict[str, object]] = {}
    for fs in cfg.file_sources:
        if not fs.upstream_glob:
            continue

        up = Path(fs.upstream_glob)
        pattern = str(up) if up.is_absolute() else str(cfg.root / up)

        try:
            matches = glob.glob(pattern)
            error = False
        except OSError as e:
            print(
                f"[ducksite] WARNING: glob failed for pattern {pattern}: {e}; "
                f"skipping file_source {fs.name or '<unnamed>'}"
            )
            matches = []
            error = True

        matches_by_fs[id(fs)] = {"pattern": pattern, "matches": matches, "error": error}

    return matches_by_fs


def _file_source_fingerprint(
    cfg: ProjectConfig, upstream_matches: dict[int, dict[str, object]] | None = None
) -> str:
    payload: list[dict[str, object]] = []
    for fs in cfg.file_sources:
        upstream_state: list[str] | None = None
        if fs.upstream_glob:
            matches: list[str] | None = None
            if upstream_matches is not None:
                cached = upstream_matches.get(id(fs))
                if cached is not None:
                    matches = cached.get("matches") or []

            if matches is None:
                up = Path(fs.upstream_glob)
                pattern = str(up) if up.is_absolute() else str(cfg.root / up)
                try:
                    matches = glob.glob(pattern)
                except OSError:
                    matches = []

            upstream_state = sorted(
                str(Path(src_path_str))
                for src_path_str in matches
                if Path(src_path_str).is_file()
            )

        payload.append(
            {
                "name": fs.name,
                "pattern": fs.pattern,
                "upstream_glob": fs.upstream_glob,
                "plugin": fs.plugin,
                "template_name": fs.template_name,
                "row_filter": fs.row_filter,
                "row_filter_template": fs.row_filter_template,
                "hierarchy_before": [h.__dict__ for h in fs.hierarchy_before],
                "hierarchy": [h.__dict__ for h in fs.hierarchy],
                "hierarchy_after": [h.__dict__ for h in fs.hierarchy_after],
                "upstream_state": upstream_state,
            }
        )
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _load_existing_fingerprint(site_root: Path) -> str | None:
    return load_fingerprint(site_root)


def _write_sqlite_map(site_root: Path, data_map: dict[str, str]) -> None:
    sqlite_path = data_map_sqlite_path(site_root)
    ensure_dir(sqlite_path.parent)
    if sqlite_path.exists():
        sqlite_path.unlink()
    con = sqlite3.connect(sqlite_path)
    try:
        con.execute(
            "CREATE TABLE data_map (shard TEXT, http_path TEXT PRIMARY KEY, physical_path TEXT)"
        )
        con.execute("CREATE INDEX data_map_shard_idx ON data_map(shard)")
        con.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
        con.execute(
            "CREATE TABLE row_filters (http_path TEXT PRIMARY KEY, filter TEXT)"
        )
        con.executemany(
            "INSERT INTO data_map (shard, http_path, physical_path) VALUES (?, ?, ?)",
            ((data_map_shard(k), k, v) for k, v in data_map.items()),
        )
        con.commit()
    finally:
        con.close()
