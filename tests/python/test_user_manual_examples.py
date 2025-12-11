from __future__ import annotations

import json
from pathlib import Path

from ducksite.builder import build_project
from ducksite.config import load_project_config
from ducksite.data_map_paths import data_map_sqlite_path
from ducksite.init_project import init_demo_project, init_project


def test_demo_scaffold_builds(tmp_path: Path, monkeypatch) -> None:
    """Init the demo scaffold and ensure a build succeeds."""

    def _fake_download(_url: str, dest: Path) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text("", encoding="utf-8")

    monkeypatch.setattr("ducksite.js_assets._download_with_ssl_bypass", _fake_download)

    init_demo_project(tmp_path)
    cfg = load_project_config(tmp_path)
    assert cfg.file_sources, "demo scaffold should define at least one file source"

    build_project(tmp_path)

    site_root = tmp_path / "static"
    assert (site_root / "index.html").exists()
    assert data_map_sqlite_path(site_root).exists()


def test_scratch_project_builds_without_file_sources(tmp_path: Path, monkeypatch) -> None:
    """Create a minimal project from scratch and build it."""

    def _fake_download(_url: str, dest: Path) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text("", encoding="utf-8")

    monkeypatch.setattr("ducksite.js_assets._download_with_ssl_bypass", _fake_download)

    root = tmp_path
    (root / "content").mkdir()
    (root / "sources_sql").mkdir()

    (root / "ducksite.toml").write_text(
        """
        [dirs]
        DIR_FORMS = "static/forms"
        """.strip()
        + "\n",
        encoding="utf-8",
    )

    (root / "sources_sql" / "models.sql").write_text(
        """
        -- name: numbers
        select * from (values (1), (2)) as t(n)

        -- name: numbers_stats
        select count(*) as total from numbers
        """.strip()
        + "\n",
        encoding="utf-8",
    )

    (root / "content" / "index.md").write_text(
        """
        # Custom Site

        ```sql inventory_rows
        select * from numbers
        ```

        ```grid cols=12 gap=md
        | inventory_rows:12 |
        ```
        """.strip()
        + "\n",
        encoding="utf-8",
    )

    build_project(root)

    site_root = root / "static"
    page_sql = site_root / "sql" / "inventory_rows.sql"
    manifest = json.loads((site_root / "sql" / "_manifest.json").read_text(encoding="utf-8"))

    assert (site_root / "index.html").exists()
    assert page_sql.exists()
    assert "numbers_stats" in manifest.get("views", {})
