from __future__ import annotations

import time
from pathlib import Path

import duckdb

from ducksite import data_map_cache
from ducksite.cte_compiler import compile_query
from ducksite.data_map_paths import data_map_shard, data_map_sqlite_path
from ducksite.queries import NamedQuery


def _write_sqlite_map(site_root: Path, rows: list[tuple[str, str]]) -> None:
    sqlite_path = data_map_sqlite_path(site_root)
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    con = data_map_cache.sqlite3.connect(sqlite_path)
    try:
        con.execute("DROP TABLE IF EXISTS data_map")
        con.execute(
            "CREATE TABLE data_map (shard TEXT, http_path TEXT PRIMARY KEY, physical_path TEXT)"
        )
        con.execute("CREATE INDEX data_map_shard_idx ON data_map(shard)")
        con.executemany(
            "INSERT INTO data_map (shard, http_path, physical_path) VALUES (?, ?, ?)",
            ((data_map_shard(h), h, p) for h, p in rows),
        )
        con.commit()
    finally:
        con.close()


def test_load_data_map_cached_until_mtime_changes(monkeypatch, tmp_path) -> None:
    data_map_cache.clear_cache()
    site_root = tmp_path / "static"
    _write_sqlite_map(site_root, [("data/demo/demo.parquet", "/phys.parquet")])

    calls = {"count": 0}
    real_connect = data_map_cache.sqlite3.connect

    def counting_connect(path: str | bytes | Path, *args, **kwargs):  # type: ignore[override]
        calls["count"] += 1
        return real_connect(path, *args, **kwargs)

    monkeypatch.setattr(data_map_cache.sqlite3, "connect", counting_connect)

    first = data_map_cache.load_data_map(site_root)
    second = data_map_cache.load_data_map(site_root)

    assert first == second
    assert calls["count"] == 1

    time.sleep(1.05)
    monkeypatch.setattr(data_map_cache.sqlite3, "connect", real_connect)
    _write_sqlite_map(site_root, [("data/demo/new.parquet", "/phys2.parquet")])

    calls["count"] = 1
    monkeypatch.setattr(data_map_cache.sqlite3, "connect", counting_connect)

    refreshed = data_map_cache.load_data_map(site_root)

    assert calls["count"] == 2
    assert refreshed == {"data/demo/new.parquet": "/phys2.parquet"}


def test_compile_query_reuses_data_map_cache(monkeypatch, tmp_path) -> None:
    data_map_cache.clear_cache()
    site_root = tmp_path / "static"
    _write_sqlite_map(site_root, [("data/demo/demo.parquet", "/phys.parquet")])

    calls = {"count": 0}
    real_connect = data_map_cache.sqlite3.connect

    def counting_connect(path: str | bytes | Path, *args, **kwargs):  # type: ignore[override]
        calls["count"] += 1
        return real_connect(path, *args, **kwargs)

    monkeypatch.setattr(data_map_cache.sqlite3, "connect", counting_connect)

    con = duckdb.connect()
    queries = {
        "base": NamedQuery(name="base", sql="SELECT 1 AS value", kind="model"),
        "top": NamedQuery(name="top", sql="SELECT * FROM base", kind="model"),
    }

    compile_query(site_root, con, queries, "top")
    con.close()

    assert calls["count"] == 1
