from __future__ import annotations

from pathlib import Path
import textwrap

from ducksite.config import load_project_config
from ducksite.markdown_parser import parse_markdown_page
from ducksite.queries import load_model_queries
from ducksite.tuy_md import (
    add_markdown_block,
    modify_markdown_block,
    remove_markdown_block,
    rename_sql_block,
)
from ducksite.tuy_sql import add_model_block, modify_model_block, remove_model_block
from ducksite.tuy_toml import add_file_source_block, modify_file_source_block, remove_file_source_block


def test_toml_add_modify_remove(tmp_path: Path) -> None:
    base = textwrap.dedent(
        """
        [dirs]
        DIR_DEMO = "demo"

        [[file_sources]]
        name = "demo"
        pattern = "data/demo/*.parquet"
        """
    )

    new_block = textwrap.dedent(
        """
        [[file_sources]]
        name = "extra"
        pattern = "data/extra/*.parquet"
        """
    )

    updated = add_file_source_block(base, new_block, tmp_path)
    (tmp_path / "ducksite.toml").write_text(updated, encoding="utf-8")
    cfg = load_project_config(tmp_path)
    assert {fs.name for fs in cfg.file_sources} == {"demo", "extra"}

    modified_block = textwrap.dedent(
        """
        [[file_sources]]
        name = "demo"
        pattern = "data/demo/new-*.parquet"
        """
    )
    updated = modify_file_source_block(updated, modified_block, tmp_path)
    (tmp_path / "ducksite.toml").write_text(updated, encoding="utf-8")
    cfg = load_project_config(tmp_path)
    demo = [fs for fs in cfg.file_sources if fs.name == "demo"][0]
    assert demo.pattern == "data/demo/new-*.parquet"

    updated = remove_file_source_block(updated, "demo", tmp_path)
    (tmp_path / "ducksite.toml").write_text(updated, encoding="utf-8")
    cfg = load_project_config(tmp_path)
    assert {fs.name for fs in cfg.file_sources} == {"extra"}


def test_sql_add_modify_remove(tmp_path: Path) -> None:
    base_sql = "-- name: base\nSELECT 1"
    updated = add_model_block(base_sql, "other", "SELECT 2")
    sql_dir = tmp_path / "sources_sql"
    sql_dir.mkdir()
    (sql_dir / "models.sql").write_text(updated, encoding="utf-8")
    (tmp_path / "ducksite.toml").write_text("[dirs]\n", encoding="utf-8")
    cfg = load_project_config(tmp_path)
    models = load_model_queries(cfg)
    assert set(models) == {"base", "other"}

    updated = modify_model_block(updated, "base", "SELECT 3")
    (sql_dir / "models.sql").write_text(updated, encoding="utf-8")
    models = load_model_queries(cfg)
    assert models["base"].sql == "SELECT 3"

    updated = remove_model_block(updated, "other")
    (sql_dir / "models.sql").write_text(updated, encoding="utf-8")
    models = load_model_queries(cfg)
    assert set(models) == {"base"}


def test_markdown_add_modify_remove_and_rename(tmp_path: Path) -> None:
    base_md = (
        """
        # Demo

        ```sql demo_query
        select 1
        ```

        ```table demo_table
        query: demo_query
        ```
        """
    )

    updated = add_markdown_block(base_md, "echart", "chart_one", "data_query: demo_query")
    updated = modify_markdown_block(updated, "table", "demo_table", "query: demo_query\nlimit: 10")

    renamed = rename_sql_block(updated, "demo_query", "renamed_query")
    md_path = tmp_path / "content"
    md_path.mkdir()
    page_path = md_path / "page.md"
    page_path.write_text(renamed, encoding="utf-8")

    parsed = parse_markdown_page(page_path, Path("page.md"))
    assert "renamed_query" in parsed.sql_blocks
    assert parsed.table_blocks["demo_table"]["query"] == "renamed_query"
    assert parsed.echart_blocks["chart_one"]["data_query"] == "renamed_query"

    cleaned = remove_markdown_block(renamed, "echart", "chart_one")
    page_path.write_text(cleaned, encoding="utf-8")
    parsed = parse_markdown_page(page_path, Path("page.md"))
    assert "chart_one" not in parsed.echart_blocks
