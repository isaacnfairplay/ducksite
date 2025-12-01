from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from ducksite.markdown_parser import parse_markdown_page, build_page_config
from ducksite.forms import FormSpec, substitute_inputs, evaluate_form_sql, append_rows_to_csv, process_form_submission
from ducksite.config import ProjectConfig


def make_cfg(tmp_path: Path) -> ProjectConfig:
    return ProjectConfig(root=tmp_path, dirs={})


def test_parse_form_block(tmp_path):
    md = tmp_path / "page.md"
    md.write_text(
        """
```form feedback
label: Test
inputs: ["a"]
target_csv: "data.csv"
sql_relation_query: |
  select 1 as x
```
""",
        encoding="utf-8",
    )
    pq = parse_markdown_page(md, Path("."))
    cfg_json = json.loads(build_page_config(pq))
    assert cfg_json["forms"][0]["id"] == "feedback"
    assert "sql_relation_query" in cfg_json["forms"][0]


def test_substitute_inputs_quotes():
    sql = "select ${inputs.name} as n"
    out = substitute_inputs(sql, {"name": "O'Reilly"})
    assert "O''Reilly" in out


def test_append_rows_creates_csv(tmp_path):
    csv_path = tmp_path / "out.csv"
    append_rows_to_csv(csv_path, [{"a": 1, "email": "x@example.com"}])
    text = csv_path.read_text()
    assert "a,email" in text
    append_rows_to_csv(csv_path, [{"a": 2, "email": "x@example.com", "b": "new"}], max_rows_per_user=5, user_email="x@example.com")
    text2 = csv_path.read_text()
    assert "new" in text2


def test_process_form_submission(tmp_path):
    cfg = make_cfg(tmp_path)
    form = FormSpec(
        id="demo",
        label="Demo",
        target_csv=str(tmp_path / "t.csv"),
        inputs=["v"],
        sql_relation_query="select ${inputs.v} as v, ${inputs._user_email} as submitted_by",
    )
    result = process_form_submission(
        cfg,
        form,
        {"inputs": {"v": "abc", "_user_email": "u@example.com"}},
    )
    assert result["rows_appended"] == 1
    text = (tmp_path / "t.csv").read_text()
    assert "u@example.com" in text
