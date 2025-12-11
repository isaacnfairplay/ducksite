from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from ducksite.compile_cache import (
    cache_signature,
    load_compile_cache,
    record_compiled_query,
    save_compile_cache,
)
from ducksite.cte_compiler import NetworkMetrics


def test_compile_cache_roundtrip(tmp_path):
    cache = {}
    metrics = NetworkMetrics(
        num_files=1,
        total_bytes_cold=10,
        largest_file_bytes=10,
        two_largest_bytes=10,
        avg_file_bytes=10.0,
        sql_bytes=4,
    )

    record_compiled_query(cache, "demo", "sig", "SQL", metrics, ["dep"])
    save_compile_cache(tmp_path, cache)

    loaded = load_compile_cache(tmp_path)
    assert loaded["demo"]["signature"] == "sig"
    assert loaded["demo"]["compiled_sql"] == "SQL"
    assert loaded["demo"]["deps"] == ["dep"]
    assert loaded["demo"]["metrics"]["num_files"] == 1


def test_cache_signature_changes_with_fingerprint():
    base = cache_signature("abc", "token1")
    assert base != cache_signature("abc", "token2")
