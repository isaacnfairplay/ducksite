from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import pytest

from ducksite.markdown_parser import parse_markdown_page, build_page_config
from ducksite.forms import (
    FormSpec,
    append_rows_to_csv,
    ensure_form_target_csvs,
    evaluate_form_sql,
    process_form_submission,
    substitute_inputs,
)
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


def test_process_form_submission_requires_auth_email(tmp_path):
    cfg = make_cfg(tmp_path)
    form = FormSpec(
        id="demo",
        label="Demo",
        target_csv=str(tmp_path / "t.csv"),
        inputs=["v"],
        sql_relation_query="select ${inputs.v} as v",
        auth_required=True,
    )

    with pytest.raises(ValueError):
        process_form_submission(cfg, form, {"inputs": {"v": "abc"}})


def test_process_form_submission_rejects_disallowed_domain(tmp_path):
    cfg = make_cfg(tmp_path)
    form = FormSpec(
        id="demo",
        label="Demo",
        target_csv=str(tmp_path / "t.csv"),
        inputs=["v"],
        sql_relation_query="select ${inputs.v} as v",
        allowed_email_domains="example.com",
    )

    with pytest.raises(ValueError):
        process_form_submission(
            cfg,
            form,
            {"inputs": {"v": "abc", "_user_email": "user@other.net"}},
        )


def test_initial_password_set_then_required(tmp_path: Path) -> None:
    cfg = make_cfg(tmp_path)
    form = FormSpec(
        id="demo",
        label="Demo",
        target_csv=str(tmp_path / "t.csv"),
        inputs=["v"],
        sql_relation_query="select ${inputs.v} as v, ${inputs._user_email} as submitted_by",
        auth_required=True,
    )

    result = process_form_submission(
        cfg,
        form,
        {"inputs": {"v": "abc", "_user_email": "u@example.com", "_user_password": "secret123"}},
    )
    assert result["rows_appended"] == 1

    with pytest.raises(ValueError):
        process_form_submission(
            cfg,
            form,
            {"inputs": {"v": "def", "_user_email": "u@example.com", "_user_password": "wrong"}},
        )

    result2 = process_form_submission(
        cfg,
        form,
        {"inputs": {"v": "def", "_user_email": "u@example.com", "_user_password": "secret123"}},
    )
    assert result2["rows_appended"] == 1


def test_formspec_from_dict_and_stub_csv(tmp_path: Path) -> None:
    cfg = ProjectConfig(root=tmp_path, dirs={"DIR_FORMS": "static/forms"})
    content_dir = cfg.root / "content"
    content_dir.mkdir(parents=True)
    md = content_dir / "forms.md"
    md.write_text(
        """
```form f1
label: Demo
target_csv: "${DIR_FORMS}/demo.csv"
inputs: ["a", "b"]
sql_relation_query: |
  select ${inputs.a} as a, ${inputs.b} as b
```

""",
        encoding="utf-8",
    )

    ensure_form_target_csvs(cfg)
    csv_path = tmp_path / "static" / "forms" / "demo.csv"
    assert csv_path.exists()
    header = csv_path.read_text(encoding="utf-8").strip()
    assert "submitted_by" in header
    assert "submitted_at" in header
