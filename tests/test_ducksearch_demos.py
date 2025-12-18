from __future__ import annotations

import shutil
from pathlib import Path

import duckdb

from ducksearch.runtime import execute_report


def _copy_demo_root(tmp_path: Path) -> Path:
    repo_root = Path(__file__).resolve().parent.parent
    src = repo_root / "ducksearch" / "demos" / "deep_demos"
    dest = tmp_path / "deep_demos"
    shutil.copytree(src, dest)
    return dest


def _read_parquet(path: Path) -> list[tuple]:
    conn = duckdb.connect(database=":memory:")
    return conn.execute(f"select * from parquet_scan('{path.as_posix()}')").fetchall()


def test_deep_demo_reports_execute_and_cache(tmp_path: Path):
    root = _copy_demo_root(tmp_path)
    reports = [
        ("reports/deep_demos/speed/rolling_latency.sql", {"Region": ["north"], "DayWindow": ["2"]}),
        ("reports/deep_demos/bindings/segment_focus.sql", {"Segment": ["alpha"], "Shard": ["2"]}),
        (
            "reports/deep_demos/imports/topic_drilldown.sql",
            {"Topic": ["routing"], "FocusVariant": ["beta"]},
        ),
    ]

    for rel, payload in reports:
        report_path = root / rel
        first = execute_report(root, report_path, payload=payload)
        assert first.base.exists()
        second = execute_report(root, report_path, payload=payload)
        assert first.base == second.base


def test_hybrid_client_payload_skips_server_filter(tmp_path: Path):
    root = _copy_demo_root(tmp_path)
    report = root / "reports/deep_demos/bindings/segment_focus.sql"

    server_filtered = execute_report(root, report, payload={"Segment": ["alpha"], "Shard": ["2"]})
    client_broad = execute_report(
        root, report, payload={"Segment": ["alpha"], "__client__Shard": ["2"]}
    )

    assert len(_read_parquet(server_filtered.base)) < len(_read_parquet(client_broad.base))


def test_import_cache_changes_with_passed_parameters(tmp_path: Path):
    root = _copy_demo_root(tmp_path)
    report = root / "reports/deep_demos/imports/topic_drilldown.sql"

    routing = execute_report(root, report, payload={"Topic": ["routing"]})
    ingest = execute_report(root, report, payload={"Topic": ["ingest"]})

    assert routing.base != ingest.base
