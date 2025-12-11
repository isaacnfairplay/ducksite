from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Dict

import sqlite3

from .data_map_paths import data_map_shard, data_map_sqlite_path


def _data_map_signature(site_root: Path) -> float | None:
    sqlite_path = data_map_sqlite_path(site_root)

    try:
        return sqlite_path.stat().st_mtime
    except FileNotFoundError:
        return None


@lru_cache(maxsize=8)
def _load_data_map_cached(
    site_root: Path, sqlite_mtime: float | None, shard: str | None
) -> Dict[str, str]:
    sqlite_path = data_map_sqlite_path(site_root)
    if sqlite_mtime is None or not sqlite_path.exists():
        return {}

    try:
        con = sqlite3.connect(sqlite_path)
        query = "SELECT http_path, physical_path FROM data_map"
        params: tuple[str, ...] = ()
        if shard is not None:
            query += " WHERE shard = ?"
            params = (shard,)
        rows = con.execute(query, params).fetchall()
        con.close()
        return {str(k): str(v) for k, v in rows}
    except sqlite3.Error as e:
        print(f"[ducksite] WARNING: failed to read {sqlite_path}: {e}")
    return {}


def load_data_map(site_root: Path, shard_hint: str | None = None) -> Dict[str, str]:
    """
    Load the virtual data map produced by symlinks.build_symlinks().

    Results are cached by modification time and shard so large projects avoid
    repeated full reads during dependency resolution.
    """

    shard: str | None = None
    if shard_hint:
        shard = data_map_shard(shard_hint) if "/" in shard_hint else shard_hint
    sqlite_mtime = _data_map_signature(site_root)
    return _load_data_map_cached(site_root, sqlite_mtime, shard)


@lru_cache(maxsize=8)
def _load_row_filters_cached(
    site_root: Path, sqlite_mtime: float | None
) -> Dict[str, str]:
    sqlite_path = data_map_sqlite_path(site_root)
    if sqlite_mtime is None or not sqlite_path.exists():
        return {}

    try:
        con = sqlite3.connect(sqlite_path)
        rows = con.execute("SELECT http_path, filter FROM row_filters").fetchall()
        con.close()
        return {str(k): str(v) for k, v in rows}
    except sqlite3.Error as e:
        print(f"[ducksite] WARNING: failed to read row filters from {sqlite_path}: {e}")
    return {}


def load_row_filters(site_root: Path) -> Dict[str, str]:
    sqlite_mtime = _data_map_signature(site_root)
    return _load_row_filters_cached(site_root, sqlite_mtime)


@lru_cache(maxsize=8)
def _load_fingerprint_cached(
    site_root: Path, sqlite_mtime: float | None
) -> str | None:
    sqlite_path = data_map_sqlite_path(site_root)
    if sqlite_mtime is None or not sqlite_path.exists():
        return None

    try:
        con = sqlite3.connect(sqlite_path)
        row = con.execute(
            "SELECT value FROM meta WHERE key = 'fingerprint'"
        ).fetchone()
        con.close()
        return str(row[0]) if row else None
    except sqlite3.Error as e:
        print(f"[ducksite] WARNING: failed to read fingerprint from {sqlite_path}: {e}")
    return None


def load_fingerprint(site_root: Path) -> str | None:
    sqlite_mtime = _data_map_signature(site_root)
    return _load_fingerprint_cached(site_root, sqlite_mtime)


def clear_cache() -> None:
    _load_data_map_cached.cache_clear()
    _load_row_filters_cached.cache_clear()
    _load_fingerprint_cached.cache_clear()
