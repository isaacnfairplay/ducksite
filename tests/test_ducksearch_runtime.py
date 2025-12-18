from pathlib import Path

import duckdb
import pytest

from ducksearch.loader import CACHE_SUBDIRS
from ducksearch.report_parser import LintError
from ducksearch.runtime import ExecutionError, execute_report


def _make_root(tmp_path: Path, sql: str, *, config_text: str | None = None) -> tuple[Path, Path]:
    (tmp_path / "config.toml").write_text(config_text or "name='demo'\n")

    for name in ["reports", "composites"]:
        (tmp_path / name).mkdir(parents=True, exist_ok=True)

    for child in CACHE_SUBDIRS:
        (tmp_path / "cache" / child).mkdir(parents=True, exist_ok=True)

    report_path = tmp_path / "reports/demo/example.sql"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(sql)
    return tmp_path, report_path


def _read_parquet(path: Path) -> list[tuple]:
    conn = duckdb.connect(database=":memory:")
    return conn.execute(f"select * from parquet_scan('{path.as_posix()}')").fetchall()


def test_execute_report_rejects_duplicate_param_casing(tmp_path: Path):
    sql = "SELECT 1;\n"
    root, report = _make_root(tmp_path, sql)

    with pytest.raises(ExecutionError, match="Duplicate parameter key"):
        execute_report(root, report, payload={"Widget": "1", "widget": "2"})


def test_execute_report_applies_data_parameters(tmp_path: Path):
    sql = """
/***PARAMS
Widget:
  type: Optional[int]
  scope: data
***/
WITH base AS (
  SELECT * FROM (VALUES (1),(2)) AS t(id)
)
SELECT id FROM base WHERE {{param Widget}} IS NULL OR id = {{param Widget}};
"""
    root, report = _make_root(tmp_path, sql)

    result = execute_report(root, report, payload={"Widget": ["2"]})
    assert _read_parquet(result.base) == [(2,)]

    refreshed = execute_report(root, report, payload={"Widget": ["1"]})
    assert result.base != refreshed.base


def test_execute_report_ignores_client_only_hybrid_params(tmp_path: Path):
    sql = """
/***PARAMS
Widget:
  type: Optional[int]
  scope: hybrid
***/
WITH base AS (
  SELECT * FROM (VALUES (1),(2)) AS t(id)
)
SELECT id FROM base WHERE {{param Widget}} IS NULL OR id = {{param Widget}};
"""
    root, report = _make_root(tmp_path, sql)

    baseline = execute_report(root, report)
    client_only = execute_report(root, report, payload={"__client__Widget": ["2"]})

    assert _read_parquet(baseline.base) == [(1,), (2,)]
    assert _read_parquet(client_only.base) == [(1,), (2,)]
    assert client_only.base == baseline.base


def test_execute_report_applies_hybrid_param_when_forced_server(tmp_path: Path):
    sql = """
/***PARAMS
Widget:
  type: Optional[int]
  scope: hybrid
***/
WITH base AS (
  SELECT * FROM (VALUES (1),(2)) AS t(id)
)
SELECT id FROM base WHERE {{param Widget}} IS NULL OR id = {{param Widget}};
"""
    root, report = _make_root(tmp_path, sql)

    client_only = execute_report(root, report, payload={"__client__Widget": ["2"]})
    server_filtered = execute_report(root, report, payload={"Widget": ["2"]})

    assert _read_parquet(server_filtered.base) == [(2,)]
    assert _read_parquet(client_only.base) == [(1,), (2,)]
    assert client_only.base != server_filtered.base


def test_execute_report_handles_ident_and_path_placeholders(tmp_path: Path):
    data_path = tmp_path / "data" / "demo.parquet"
    data_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(database=":memory:")
    conn.execute("copy (select 7 as value) to ? (format 'parquet')", [data_path.as_posix()])

    sql = """
/***PARAMS
FilePath:
  type: str
  scope: data
ColumnName:
  type: InjectedIdentLiteral['value']
  scope: data
***/
SELECT {{ident ColumnName}}, {{path FilePath}} FROM (VALUES (7)) AS t(value);
"""
    root, report = _make_root(tmp_path, sql)

    result = execute_report(
        root,
        report,
        payload={"FilePath": [data_path.as_posix()], "ColumnName": ["value"]},
    )

    assert _read_parquet(result.base) == [(7, data_path.as_posix())]


def test_execute_report_resolves_config_and_bindings(tmp_path: Path):
    data_root = tmp_path / "data"
    data_root.mkdir(parents=True, exist_ok=True)

    sql = """
/***CONFIG
BASE_PATH: InjectedPathStr
***/
/***PARAMS
LookupKey:
  type: int
  scope: data
***/
/***BINDINGS
- id: key_lookup
  source: binding_values
  key_param: LookupKey
  key_column: key
  value_column: value
  kind: demo
***/
WITH binding_values AS MATERIALIZE_CLOSED (
  SELECT * FROM (VALUES (1, 'alpha'), (2, 'beta')) AS t(key, value)
)
SELECT '{{config BASE_PATH}}/{{bind key_lookup}}' AS resolved;
"""

    config_text = f"name='demo'\nBASE_PATH='{data_root.as_posix()}'\n"
    root, report = _make_root(tmp_path, sql, config_text=config_text)

    result = execute_report(root, report, payload={"LookupKey": ["2"]})
    assert _read_parquet(result.base) == [(f"{data_root.as_posix()}/beta",)]


def test_binding_key_sql_resolves_value(tmp_path: Path):
    sql = """
/***PARAMS
Barcode:
  type: str
  scope: data
***/
/***BINDINGS
- id: key_lookup
  source: binding_values
  key_sql: "select substr({{param Barcode}}, 1, 3) as key"
  key_column: key
  value_column: value
  kind: demo
***/
WITH binding_values AS MATERIALIZE_CLOSED (
  SELECT * FROM (VALUES ('ABC', 'alpha'), ('XYZ', 'omega')) AS t(key, value)
)
SELECT '{{bind key_lookup}}' AS bound_value;
"""
    root, report = _make_root(tmp_path, sql)

    result = execute_report(root, report, payload={"Barcode": ["ABC-123"]})
    assert _read_parquet(result.base) == [("alpha",)]


def test_binding_key_sql_list_mode(tmp_path: Path):
    sql = """
/***PARAMS
Barcode:
  type: str
  scope: data
***/
/***BINDINGS
- id: files
  source: binding_values
  key_sql: |
    select substr({{param Barcode}}, 1, 2) || suffix as key
    from (VALUES ('01'), ('02')) AS t(suffix)
  key_column: key
  value_column: file_path
  value_mode: list
  kind: demo
***/
WITH binding_values AS MATERIALIZE_CLOSED (
  SELECT * FROM (VALUES ('AA01', 'file1.parquet'), ('AA02', 'file2.parquet')) AS t(key, file_path)
)
SELECT {{bind files}} AS selected_files;
"""
    root, report = _make_root(tmp_path, sql)

    result = execute_report(root, report, payload={"Barcode": ["AA999"]})
    files_list = _read_parquet(result.base)[0][0]
    assert set(files_list) == {"file1.parquet", "file2.parquet"}


def test_binding_key_sql_rejects_dual_keys(tmp_path: Path):
    sql = """
/***PARAMS
Widget:
  type: str
  scope: data
***/
/***BINDINGS
- id: invalid
  source: binding_values
  key_param: Widget
  key_sql: "select {{param Widget}} as key"
  key_column: key
  value_column: value
  kind: demo
***/
WITH binding_values AS MATERIALIZE_CLOSED (
  SELECT * FROM (VALUES ('a', 'alpha')) AS t(key, value)
)
SELECT 1;
"""
    root, report = _make_root(tmp_path, sql)

    with pytest.raises(LintError, match="cannot set both key_param and key_sql"):
        execute_report(root, report, payload={"Widget": ["a"]})


def test_binding_key_sql_requires_server_param(tmp_path: Path):
    sql = """
/***PARAMS
ClientOnly:
  type: str
  scope: view
***/
/***BINDINGS
- id: client
  source: binding_values
  key_sql: "select {{param ClientOnly}} as key"
  key_column: key
  value_column: value
  kind: demo
***/
WITH binding_values AS MATERIALIZE_CLOSED (
  SELECT * FROM (VALUES ('value', 'ok')) AS t(key, value)
)
SELECT 1;
"""
    root, report = _make_root(tmp_path, sql)

    with pytest.raises(ExecutionError, match="requires server parameter ClientOnly"):
        execute_report(root, report, payload={"ClientOnly": ["value"]})


def test_binding_key_sql_requires_keys(tmp_path: Path):
    sql = """
/***PARAMS
Barcode:
  type: str
  scope: data
***/
/***BINDINGS
- id: missing
  source: binding_values
  key_sql: "select * from (select 1 as key) where 1=0"
  key_column: key
  value_column: value
  kind: demo
***/
WITH binding_values AS MATERIALIZE_CLOSED (
  SELECT * FROM (VALUES (1, 'value')) AS t(key, value)
)
SELECT 1;
"""
    root, report = _make_root(tmp_path, sql)

    with pytest.raises(ExecutionError, match="No binding keys produced"):
        execute_report(root, report, payload={"Barcode": ["any"]})


def test_binding_key_sql_multiple_values_single_mode(tmp_path: Path):
    sql = """
/***PARAMS
Barcode:
  type: str
  scope: data
***/
/***BINDINGS
- id: multi
  source: binding_values
  key_sql: "select substr({{param Barcode}}, 1, 2) as key union all select substr({{param Barcode}}, 1, 2)"
  key_column: key
  value_column: value
  kind: demo
***/
WITH binding_values AS MATERIALIZE_CLOSED (
  SELECT * FROM (VALUES ('AB', 'first'), ('AB', 'second')) AS t(key, value)
)
SELECT 1;
"""
    root, report = _make_root(tmp_path, sql)

    with pytest.raises(ExecutionError, match="Multiple binding values for multi"):
        execute_report(root, report, payload={"Barcode": ["AB-001"]})
