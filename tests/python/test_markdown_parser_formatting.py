import json
from pathlib import Path

import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from ducksite.demo_init_content import init_demo_content
from ducksite.markdown_parser import parse_markdown_page, build_page_config


@pytest.fixture()
def demo_content(tmp_path: Path) -> Path:
    init_demo_content(tmp_path)
    return tmp_path / "content"


def test_echart_blocks_parsed_without_format(demo_content: Path):
    gallery = demo_content / "gallery" / "index.md"
    pq = parse_markdown_page(gallery, Path("gallery/index.md"))
    cfg = json.loads(build_page_config(pq))

    visualizations = cfg["visualizations"]
    assert len(visualizations) >= 50

    sample = visualizations.get("gallery_01")
    assert sample
    assert sample["data_query"] == "gallery_q1_totals"
    assert sample["type"] == "bar"
    assert "format" not in sample

    assert all("format" not in spec for spec in visualizations.values())
    assert "tables" not in cfg


def test_grid_specs_unchanged(demo_content: Path):
    minor = demo_content / "minor" / "index.md"
    pq = parse_markdown_page(minor, Path("minor/index.md"))

    assert pq.grid_specs
    html = pq.html
    assert "layout-grid" in html
    assert "data-table-id=\"minor_rows\"" in html


def test_no_tables_entry_yet(demo_content: Path):
    major = demo_content / "major" / "index.md"
    pq = parse_markdown_page(major, Path("major/index.md"))
    cfg = json.loads(build_page_config(pq))

    assert "tables" not in cfg


def test_echart_format_block_parsed(tmp_path: Path):
    md = tmp_path / "page.md"
    md.write_text(
        """
```sql major_summary
SELECT 'A' AS category, 10 AS total_value
UNION ALL SELECT 'B', 25;
```

```echart major_chart
data_query: major_summary
type: bar
x: category
y: total_value
title: "Major Sum by Category (with formatting)"

format:
  total_value:
    color_expr: "CASE WHEN total_value >= 20 THEN '#f97373' ELSE '#22c55e' END"
    highlight_expr: "total_value >= 20"
```
""",
        encoding="utf-8",
    )

    pq = parse_markdown_page(md, Path("page.md"))
    cfg = json.loads(build_page_config(pq))
    viz = cfg["visualizations"]["major_chart"]

    assert viz["data_query"] == "major_summary"
    assert viz["format"]["total_value"]["color_expr"].startswith("CASE WHEN total_value")
    assert viz["format"]["total_value"]["highlight_expr"] == "total_value >= 20"


def test_table_format_block_parsed(tmp_path: Path):
    md = tmp_path / "page.md"
    md.write_text(
        """
```sql demo_rows
SELECT 'A' AS category, 5 AS value, 1 AS other_metric
UNION ALL SELECT 'B', 20, 15;
```

```table demo_table
query: demo_rows
format:
  value:
    bg_color_expr: "CASE WHEN value >= 20 THEN '#f97373' ELSE NULL END"
    fg_color_expr: "CASE WHEN other_metric > 0 THEN '#0b1120' END"
    highlight_expr: "other_metric > 10"
  category:
    bg_color_expr: "CASE WHEN category = 'A' THEN '#1d283a' END"
    fg_color_expr: "CASE WHEN category = 'A' THEN '#38bdf8' END"
```
""",
        encoding="utf-8",
    )

    pq = parse_markdown_page(md, Path("page.md"))
    cfg = json.loads(build_page_config(pq))

    tables = cfg.get("tables", {})
    assert "demo_table" in tables
    tbl = tables["demo_table"]
    assert tbl["query"] == "demo_rows"
    assert tbl["format"]["value"]["bg_color_expr"].startswith("CASE WHEN value >= 20")
    assert tbl["format"]["value"]["fg_color_expr"].startswith("CASE WHEN other_metric")
    assert tbl["format"]["value"]["highlight_expr"] == "other_metric > 10"
