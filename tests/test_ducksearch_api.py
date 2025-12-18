from pathlib import Path

import duckdb

import ducksearch


def _make_root(tmp_path: Path) -> tuple[Path, Path]:
    (tmp_path / "config.toml").write_text("name='demo'\n")
    (tmp_path / "reports").mkdir(parents=True, exist_ok=True)
    (tmp_path / "composites").mkdir(parents=True, exist_ok=True)
    for child in ducksearch.CACHE_SUBDIRS:
        (tmp_path / "cache" / child).mkdir(parents=True, exist_ok=True)

    report = tmp_path / "reports" / "demo" / "example.sql"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text("select 1 as value\n")
    return tmp_path, report


def _read_parquet(path: Path) -> list[tuple]:
    conn = duckdb.connect(database=":memory:")
    return conn.execute(f"select * from parquet_scan('{path.as_posix()}')").fetchall()


def test_public_api_exports(tmp_path: Path):
    root, report = _make_root(tmp_path)

    layout = ducksearch.validate_root(root)
    parsed = ducksearch.parse_report_sql(report)
    assert parsed.sql.strip().lower().startswith("select")

    result = ducksearch.execute_report(layout.root, report)
    assert isinstance(result, ducksearch.ExecutionResult)
    assert _read_parquet(result.base) == [(1,)]
