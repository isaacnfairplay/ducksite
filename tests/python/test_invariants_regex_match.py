from pathlib import Path

from ducksite.config import _substitute_dirs
from ducksite.forms import substitute_inputs
from ducksite.cte_compiler import _rewrite_virtual_paths_for_explain


def test_substitute_dirs_preserves_dir_variables() -> None:
    dirs = {"DIR_FAKE": "fake_upstream"}
    s = "${DIR_FAKE}/demo-*.parquet"
    out = _substitute_dirs(s, dirs)
    assert out == "fake_upstream/demo-*.parquet"


def test_substitute_inputs_quotes_and_nulls() -> None:
    template = "select ${inputs.name} as n, ${inputs.missing} as m"
    out = substitute_inputs(template, {"name": "O'Reilly"})
    assert "O''Reilly" in out
    # missing value becomes NULL literal, not quoted string
    assert "NULL as m" in out or "NULL)" in out


def test_rewrite_virtual_paths_for_explain_is_idempotent_without_data_map(tmp_path: Path) -> None:
    sql = "select * from read_parquet(['data/demo/demo-A.parquet'])"
    # No data_map.json, so function should return sql unchanged
    out = _rewrite_virtual_paths_for_explain(tmp_path, sql)
    assert out == sql
