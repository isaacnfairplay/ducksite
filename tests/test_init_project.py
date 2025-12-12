from __future__ import annotations

from pathlib import Path

from ducksite.config import load_project_config
import ducksite.demo_init_fake_parquet as fake_parquet
from ducksite.init_project import init_demo_project, init_project
from ducksite.markdown_parser import parse_markdown_page


def _list_files(root: Path) -> set[str]:
    return {
        p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()
    }


def test_init_demo_project_files(tmp_path: Path, monkeypatch) -> None:
    def _fake_download(dest: Path) -> bool:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"")
        return True

    monkeypatch.setattr(fake_parquet, "_download_nytaxi_parquet", _fake_download)

    init_demo_project(tmp_path)

    expected = {
        "ducksite.toml",
        "content/index.md",
        "content/major/index.md",
        "content/minor/index.md",
        "content/filters/index.md",
        "content/cross_filters/index.md",
        "content/derived_filters/index.md",
        "content/hierarchy/index.md",
        "content/hierarchy_window/index.md",
        "content/models/index.md",
        "content/template/index.md",
        "content/forms/index.md",
        "content/gallery/index.md",
        "static/forms/feedback.csv",
        "sources_sql/demo_models.sql",
        "sources_sql/demo_template_[category].sql",
        "fake_upstream/demo-A.parquet",
        "fake_upstream/demo-B.parquet",
        "fake_upstream/demo-C.parquet",
        "fake_upstream/demo_hierarchy/day/hier-day.parquet",
        "fake_upstream/demo_hierarchy/month/hier-month.parquet",
        "fake_upstream/demo_hierarchy/year/hier-year.parquet",
        "fake_upstream/demo_hierarchy_window/day/hier-edge-day.parquet",
        "fake_upstream/demo_hierarchy_window/day_end/hier-edge-end.parquet",
        "fake_upstream/demo_hierarchy_window/day_start/hier-edge-start.parquet",
            "fake_upstream/demo_hierarchy_window/month/hier-edge-month.parquet",
            "fake_upstream/demo_hierarchy_window/year/hier-edge-year.parquet",
            "fake_upstream/nytaxi-2023-01.parquet",
            "plugins/demo_plugin.py",
            "plugins/demo_plugin_chain.py",
        }

    assert _list_files(tmp_path) == expected

    cfg = load_project_config(tmp_path)
    demo_fs = next(fs for fs in cfg.file_sources if fs.name == "demo")
    assert demo_fs.template_name == "demo_[category]"
    assert demo_fs.upstream_glob.endswith("demo-*.parquet")

    plugin_fs = next(fs for fs in cfg.file_sources if fs.name == "demo_plugin")
    assert plugin_fs.plugin == "plugins/demo_plugin.py"

    nytaxi_fs = next(fs for fs in cfg.file_sources if fs.name == "nytaxi")
    assert nytaxi_fs.upstream_glob.endswith("nytaxi-*.parquet")
    assert nytaxi_fs.pattern == "data/nytaxi/*.parquet"

    parsed = parse_markdown_page(tmp_path / "content" / "index.md", Path("index.md"))
    assert "demo_summary" in parsed.sql_blocks
    assert parsed.echart_blocks["category_chart"]["data_query"] == "demo_summary"


def test_init_project_barebones_minimal(tmp_path: Path) -> None:
    init_project(tmp_path)

    assert _list_files(tmp_path) == {"ducksite.toml"}
    assert (tmp_path / "content").is_dir()
    assert (tmp_path / "sources_sql").is_dir()
    assert (tmp_path / "static" / "forms").is_dir()
    assert not (tmp_path / "fake_upstream").exists()
    assert not any((tmp_path / "content").iterdir())
    assert not any((tmp_path / "sources_sql").iterdir())
