from __future__ import annotations

from pathlib import Path


def data_map_dir(site_root: Path) -> Path:
    return site_root.parent / ".ducksite_data"


def data_map_sqlite_path(site_root: Path) -> Path:
    return data_map_dir(site_root) / "data_map.sqlite"


def data_map_shard(http_path: str) -> str:
    """Return the data source shard name for an HTTP path under data/."""

    if not http_path.startswith("data/"):
        return ""
    parts = http_path.split("/", 2)
    return parts[1] if len(parts) > 1 else ""
