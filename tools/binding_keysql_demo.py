"""Demonstrate binding key_sql reducing scan size.

Run with:

    python tools/binding_keysql_demo.py

Sample output (numbers will vary):

    Generated 800 partitions; target prefix has 5 partitions
    Baseline scanned 800 files in 0.210s
    Optimized scanned 5 files in 0.038s
    --- Baseline plan (first 20 lines) ---
    analyzed_plan
    ┌─────────────────────────────────────┐
    │┌───────────────────────────────────┐│
    ...
"""

from __future__ import annotations

import argparse
import tempfile
import time
from pathlib import Path

import duckdb

from ducksearch.loader import CACHE_SUBDIRS
from ducksearch.runtime import execute_report


def _ensure_root(root: Path) -> None:
    (root / "config.toml").write_text("name='binding-demo'\n")
    for name in ["reports", "composites"]:
        (root / name).mkdir(parents=True, exist_ok=True)
    for child in CACHE_SUBDIRS:
        (root / "cache" / child).mkdir(parents=True, exist_ok=True)


def _make_partitions(conn: duckdb.DuckDBPyConnection, data_dir: Path, total: int, target_prefix: str) -> list[Path]:
    data_dir.mkdir(parents=True, exist_ok=True)
    target_files: list[Path] = []

    for idx in range(total):
        path = data_dir / f"partition_{idx:04d}.parquet"
        prefix = target_prefix if idx < max(1, total // 160) else f"OTHER{idx:04d}".ljust(20, "X")
        rows = 50 if prefix == target_prefix else 10
        conn.execute(
            f"""
            copy (
                select
                    {idx} as partition_id,
                    {idx % 10} as panel_id,
                    {idx % 5} as board_num,
                    '{prefix}' || lpad(cast(row_num as varchar), 8, '0') as barcode
                from range({rows}) as t(row_num)
            ) to ?
            (format 'parquet')
            """,
            [path.as_posix()],
        )
        if prefix == target_prefix:
            target_files.append(path)

    return target_files


def _write_binding_map(conn: duckdb.DuckDBPyConnection, binding_path: Path, prefix: str, files: list[Path]) -> None:
    values_sql = ", ".join(["(?, ?)"] * len(files))
    params = []
    for path in files:
        params.extend([prefix, path.as_posix()])
    conn.execute(
        f"copy (select * from (values {values_sql}) as t(prefix20, file_path)) to ? (format 'parquet')",
        [*params, binding_path.as_posix()],
    )


def _write_reports(root: Path, data_dir: Path, binding_map: Path) -> tuple[Path, Path]:
    reports_dir = root / "reports" / "demo"
    reports_dir.mkdir(parents=True, exist_ok=True)

    baseline = reports_dir / "baseline.sql"
    baseline.write_text(
        f"""
/***PARAMS
Barcode:
  type: str
  scope: data
***/
EXPLAIN ANALYZE
SELECT count(*) AS matches
FROM parquet_scan('{data_dir.as_posix()}/partition_*.parquet')
WHERE barcode LIKE substr({{param Barcode}}, 1, 20) || '%';
"""
    )

    optimized = reports_dir / "optimized.sql"
    optimized.write_text(
        f"""
/***PARAMS
Barcode:
  type: str
  scope: data
***/
/***BINDINGS
- id: partitions
  source: binding_source
  key_sql: "select substr({{param Barcode}}, 1, 20) as key"
  key_column: prefix20
  value_column: file_path
  value_mode: list
  kind: partition
***/
WITH binding_source AS MATERIALIZE_CLOSED (
  SELECT * FROM parquet_scan('{binding_map.as_posix()}')
)
EXPLAIN ANALYZE
SELECT count(*) AS matches
FROM parquet_scan({{bind partitions}})
WHERE barcode LIKE substr({{param Barcode}}, 1, 20) || '%';
"""
    )

    return baseline, optimized


def _run_report(path: Path, root: Path, barcode: str) -> tuple[float, Path]:
    start = time.perf_counter()
    result = execute_report(root, path, payload={"Barcode": [barcode]})
    elapsed = time.perf_counter() - start
    return elapsed, result.base


def _read_plan(path: Path) -> str:
    conn = duckdb.connect(database=":memory:")
    rows = conn.execute(f"select * from parquet_scan('{path.as_posix()}')").fetchall()
    if not rows:
        return ""
    return "\n".join(str(row[0]) for row in rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Demonstrate binding key_sql reducing scans.")
    parser.add_argument("--partitions", type=int, default=800, help="Total parquet partitions to generate.")
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        _ensure_root(root)

        conn = duckdb.connect(database=":memory:")
        data_dir = root / "data"
        target_prefix = "TARGETPREFIX00000000"
        target_files = _make_partitions(conn, data_dir, args.partitions, target_prefix)
        binding_map = root / "binding_map.parquet"
        _write_binding_map(conn, binding_map, target_prefix, target_files)

        baseline_report, optimized_report = _write_reports(root, data_dir, binding_map)
        barcode_value = f"{target_prefix}XYZ9999"

        baseline_time, baseline_plan_path = _run_report(baseline_report, root, barcode_value)
        optimized_time, optimized_plan_path = _run_report(optimized_report, root, barcode_value)

        print(f"Generated {args.partitions} partitions; target prefix has {len(target_files)} partitions")
        print(f"Baseline scanned {args.partitions} files in {baseline_time:.3f}s")
        print(f"Optimized scanned {len(target_files)} files in {optimized_time:.3f}s")

        baseline_plan = _read_plan(baseline_plan_path).splitlines()
        optimized_plan = _read_plan(optimized_plan_path).splitlines()
        print("--- Baseline plan (first 20 lines) ---")
        print("\n".join(baseline_plan[:20]))
        print("--- Optimized plan (first 20 lines) ---")
        print("\n".join(optimized_plan[:20]))


if __name__ == "__main__":
    main()
