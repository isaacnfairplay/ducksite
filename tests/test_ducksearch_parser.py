from pathlib import Path

import pytest

from ducksearch.report_parser import (
    LintError,
    ParameterType,
    infer_scope,
    parse_param_type,
    parse_report_sql,
)


def _write_report(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "report.sql"
    path.write_text(content)
    return path


def test_parse_report_extracts_metadata_and_scope(tmp_path: Path):
    sql = """
/***PARAMS
Widget:
  type: Optional[int]
  applies_to:
    cte: demo
    mode: inline
***/
WITH demo AS (
  SELECT 1 AS value
)
SELECT value FROM demo WHERE value = {{param Widget}}
"""
    report = parse_report_sql(_write_report(tmp_path, sql))
    assert report.metadata["PARAMS"]["Widget"]["type"] == "Optional[int]"
    assert report.parameters[0].scope == "data"
    assert report.parameters[0].applies_to is not None
    assert report.parameters[0].applies_to.cte == "demo"


def test_wrapper_applies_to_requires_base_cte(tmp_path: Path):
    sql = """
/***PARAMS
Name:
  type: Optional[str]
  applies_to:
    cte: filtered
    mode: wrapper
***/
WITH filtered_base AS (
  SELECT 1 AS id
),
filtered AS (
  SELECT * FROM filtered_base WHERE {{param Name}}
)
SELECT * FROM filtered
"""
    report = parse_report_sql(_write_report(tmp_path, sql))
    assert report.parameters[0].applies_to is not None
    assert report.parameters[0].applies_to.mode == "wrapper"


def test_missing_wrapper_base_cte_rejected(tmp_path: Path):
    sql = """
/***PARAMS
Name:
  type: Optional[str]
  applies_to:
    cte: filtered
    mode: wrapper
***/
WITH filtered AS (
  SELECT 1
)
SELECT * FROM filtered
"""
    with pytest.raises(LintError):
        parse_report_sql(_write_report(tmp_path, sql))


def test_duplicate_param_casing_rejected(tmp_path: Path):
    sql = """
/***PARAMS
Foo:
  type: int
foo:
  type: int
***/
SELECT 1
"""
    with pytest.raises(LintError):
        parse_report_sql(_write_report(tmp_path, sql))


def test_single_statement_enforced(tmp_path: Path):
    sql = "SELECT 1; SELECT 2;"
    with pytest.raises(LintError):
        parse_report_sql(_write_report(tmp_path, sql))


def test_semicolon_inside_literal_and_comment_allowed(tmp_path: Path):
    sql = """
    SELECT 'a; -- not a delimiter'; -- trailing comment with ; should not split
    """
    report = parse_report_sql(_write_report(tmp_path, sql))
    assert report.sql.strip().startswith("SELECT 'a; -- not a delimiter';")


def test_param_type_parsing():
    opt_literal = parse_param_type("Optional[Literal['A','B']]")
    assert opt_literal.kind == "optional"
    assert opt_literal.inner is not None
    assert opt_literal.inner.kind == "literal"
    assert opt_literal.inner.literals == ("A", "B")

    injected_ident = parse_param_type("InjectedIdentLiteral['col']")
    assert injected_ident.kind == "injected_ident_literal"
    assert injected_ident.literals == ("col",)

    scope = infer_scope("Widget", "SELECT * FROM t WHERE {{param Widget}}")
    assert scope == "data"


def test_parquet_scan_rejects_concat(tmp_path: Path):
    sql = """
/***CONFIG
DATA_ROOT: InjectedPathStr
***/
SELECT * FROM parquet_scan('{{config DATA_ROOT}}/' || 'demo.parquet');
"""
    with pytest.raises(LintError) as err:
        parse_report_sql(_write_report(tmp_path, sql))
    assert "DS011" in str(err.value)


def test_illegal_sql_construct_rejected(tmp_path: Path):
    sql = "ATTACH 'db.duckdb';"
    with pytest.raises(LintError) as err:
        parse_report_sql(_write_report(tmp_path, sql))
    assert "DS012" in str(err.value)


def test_placeholder_requires_metadata(tmp_path: Path):
    sql = "SELECT {{config MISSING}};"
    with pytest.raises(LintError) as err:
        parse_report_sql(_write_report(tmp_path, sql))
    assert "DS010" in str(err.value)


def test_invalid_placeholder_type(tmp_path: Path):
    sql = "SELECT {{foo bar}};"
    with pytest.raises(LintError) as err:
        parse_report_sql(_write_report(tmp_path, sql))
    assert "DS009" in str(err.value)


def test_binding_cycle_detected(tmp_path: Path):
    sql = """
/***PARAMS
Key:
  type: int
***/
/***BINDINGS
- id: first
  source: second
  key_param: Key
  key_column: k
  value_column: v
  kind: demo
- id: second
  source: first
  key_param: Key
  key_column: k
  value_column: v
  kind: demo
***/
SELECT 1;
"""
    with pytest.raises(LintError) as err:
        parse_report_sql(_write_report(tmp_path, sql))
    assert "DS013" in str(err.value)
