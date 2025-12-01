from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parent.parent))

from ducksite import js_assets
from ducksite.builder import build_project
from ducksite.config import FileSourceConfig, ProjectConfig
from ducksite.symlinks import build_symlinks


@pytest.fixture
def demo_root(tmp_path: Path) -> Path:
    """Create a temporary ducksite project root with minimal config."""
    cfg = """
[dirs]
"""
    (tmp_path / "ducksite.toml").write_text(cfg, encoding="utf-8")
    return tmp_path


def test_build_project_generates_site_and_configs(monkeypatch: pytest.MonkeyPatch, demo_root: Path) -> None:
    def fake_download(url: str, dest: Path) -> None:  # noqa: ARG001
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text("// stub echarts", encoding="utf-8")

    monkeypatch.setattr(js_assets, "_download_with_ssl_bypass", fake_download)

    content = demo_root / "content"
    (content / "section").mkdir(parents=True, exist_ok=True)

    (content / "index.md").write_text(
        """
# Welcome

Intro copy for the homepage.

```sql page_query
SELECT 1 AS value
```

```echart chart1
title: Demo Chart
```

```input country
label: Country
```

```grid cols=2 gap=lg
| chart1 | table1 |
| .      | table1:2 |
```
""",
        encoding="utf-8",
    )

    (content / "section" / "index.md").write_text("Section content", encoding="utf-8")

    build_project(demo_root)

    site_root = demo_root / "static"
    index_html = site_root / "index.html"
    section_html = site_root / "section" / "index.html"

    assert index_html.exists()
    assert section_html.exists()

    html_text = index_html.read_text(encoding="utf-8")
    assert "(current: /index.html)" in html_text
    assert "/section/index.html" in html_text
    assert "page-config-json" in html_text

    section_text = section_html.read_text(encoding="utf-8")
    assert "(current: /section/index.html)" in section_text

    sitemap = json.loads((site_root / "sitemap.json").read_text(encoding="utf-8"))
    assert sitemap["routes"] == ["/index.html", "/section/index.html"]

    data_map = json.loads((site_root / "data_map.json").read_text(encoding="utf-8"))
    assert data_map == {}

    sql_path = site_root / "sql" / "page_query.sql"
    assert sql_path.exists()
    sql_text = sql_path.read_text(encoding="utf-8")
    assert sql_text.startswith("-- METRICS:")
    assert "SELECT 1 AS value" in sql_text

    contract = (site_root / "js" / "ducksite_contract.js").read_text(encoding="utf-8")
    assert "layoutGrid" in contract
    assert "pageConfigJson" in contract
    assert (site_root / "js" / "echarts.min.js").exists()


def test_symlinks_map_upstream_files(tmp_path: Path) -> None:
    upstream = tmp_path / "upstream"
    (upstream / "cat1").mkdir(parents=True, exist_ok=True)
    (upstream / "cat1" / "first.parquet").write_text("demo", encoding="utf-8")
    (upstream / "cat1" / "second.parquet").write_text("demo", encoding="utf-8")

    pattern = str(upstream / "cat*" / "*.parquet")
    cfg = ProjectConfig(
        root=tmp_path,
        dirs={},
        file_sources=[FileSourceConfig(name="demo", upstream_glob=pattern)],
    )

    build_symlinks(cfg)

    data_map_path = cfg.site_root / "data_map.json"
    assert data_map_path.exists()

    data_map = json.loads(data_map_path.read_text(encoding="utf-8"))
    expected_keys = {
        f"data/demo/cat1/{name}" for name in ["first.parquet", "second.parquet"]
    }
    assert set(data_map.keys()) == expected_keys
    for value in data_map.values():
        assert value.startswith(str(upstream))
