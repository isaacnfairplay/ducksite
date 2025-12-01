from __future__ import annotations
from pathlib import Path
import glob
import json

from .config import ProjectConfig
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
    upstream filesystem paths, and write it to:

        <site_root>/data_map.json

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
    looks up the physical path in data_map.json, and streams that file.

    NOTE:
      - We do not touch the filesystem under site_root/data at all.
      - This function is idempotent and safe to re-run on each build.
    """
    site_root = cfg.site_root
    ensure_dir(site_root)

    data_map: dict[str, str] = {}

    for fs in cfg.file_sources:
        if not fs.upstream_glob:
            continue

        # Treat upstream_glob as absolute if it is an absolute/UNC path;
        # otherwise interpret it relative to the project root.
        up = Path(fs.upstream_glob)
        if up.is_absolute():
            pattern = str(up)
        else:
            pattern = str(cfg.root / up)

        base_root = _common_non_wild_root(pattern)

        try:
            matches = glob.glob(pattern)
        except OSError as e:
            print(
                f"[ducksite] WARNING: glob failed for pattern {pattern}: {e}; "
                f"skipping file_source {fs.name or '<unnamed>'}"
            )
            continue

        if not matches:
            print(
                f"[ducksite] INFO: no upstream files matched {pattern}; "
                f"file_source {fs.name or '<unnamed>'} will have no mirrored data."
            )
            continue

        # Logical root for this file_source under /data; default to flat if unnamed.
        fs_root = Path("data") / (fs.name or "")

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

    # Write the virtual symlink map for the HTTP server.
    out_path = site_root / "data_map.json"
    ensure_dir(out_path.parent)
    out_path.write_text(json.dumps(data_map, indent=2), encoding="utf-8")
    print(f"[ducksite] wrote virtual data map {out_path} ({len(data_map)} entries)")


if __name__ == "__main__":
    from .config import load_project_config

    root = Path(".").resolve()
    cfg = load_project_config(root)
    build_symlinks(cfg)
    print("Virtual data map built at", cfg.site_root / "data_map.json")
