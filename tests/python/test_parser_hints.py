import duckdb

from ducksite.cte_compiler import _apply_parser_hints, compile_query
from ducksite.queries import NamedQuery


def test_apply_parser_hints_autofixes_common_join_typos():
    sql, warnings = _apply_parser_hints(
        "SELECT * FROM t LEFT ASOF JOIN u ON t.ts = u.ts LEFT ATNI JOIN v ON TRUE RIGHT ASOF JOIN x ON TRUE ATNI JOIN y ON TRUE"
    )

    assert sql.count("ASOF JOIN") >= 2
    assert "ATNI" not in sql
    assert any("ASOF" in w for w in warnings)
    assert any("ANTI" in w for w in warnings)


def test_apply_parser_hints_handles_outer_asof_and_using_without_parens():
    sql, warnings = _apply_parser_hints(
        "SELECT * FROM a LEFT OUTER ASOF JOIN b USING id"
    )

    assert "LEFT OUTER" not in sql
    assert "ASOF JOIN" in sql
    assert "USING (id)" in sql
    assert any("USING clause" in w for w in warnings)


def test_apply_parser_hints_flags_missing_join_condition():
    _, warnings = _apply_parser_hints("SELECT * FROM foo LEFT JOIN bar; LEFT JOIN baz USING(id)")
    assert any("missing an ON/USING" in w for w in warnings)


def test_compile_query_prints_parser_warning_on_autofix(tmp_path, capsys):
    con = duckdb.connect()
    con.execute("CREATE TABLE t(ts INT)")
    con.execute("CREATE TABLE u(ts INT)")

    queries = {
        "q": NamedQuery(
            name="q",
            sql="SELECT * FROM t LEFT ASOF JOIN u ON t.ts <= u.ts",
            kind="model",
        )
    }

    sql, _, _ = compile_query(tmp_path, con, queries, "q")

    captured = capsys.readouterr()
    assert "ParserWarning" in captured.out
    assert "ASOF JOIN" in sql
