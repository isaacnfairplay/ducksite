from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

from ducksite.data_map_cache import load_data_map, load_fingerprints, load_row_filters
from ducksite.data_map_paths import data_map_sqlite_path
from ducksite.config import load_project_config
from ducksite.queries import build_file_source_queries
from ducksite.symlinks import build_symlinks
from ducksite.tuy_plugin import write_blank_plugin
from ducksite.virtual_parquet import DEFAULT_PLUGIN_CALLABLE, _split_plugin_ref


def test_plugin_loaded_from_external_path(tmp_path: Path) -> None:
    project_root = tmp_path / "proj"
    plugin_root = tmp_path / "external"
    project_root.mkdir()
    plugin_root.mkdir()

    helpers = plugin_root / "helpers.py"
    helpers.write_text("ROW_FILTER = \"region = 'APAC'\"\n", encoding="utf-8")

    plugin_file = plugin_root / "plugin.py"
    plugin_file.write_text(
        "\n".join(
            [
                "from ducksite.virtual_parquet import VirtualParquetManifest, VirtualParquetFile",
                "from helpers import ROW_FILTER",
                "",
                "def build_manifest(cfg):",
                "    return VirtualParquetManifest(",
                "        files=[",
                "            VirtualParquetFile(",
                "                http_path='table/data.parquet',",
                "                physical_path=str(cfg.root / 'upstream' / 'data.parquet'),",
                "                row_filter=ROW_FILTER,",
                "            )",
                "        ],",
                "        template_name='by_region_[region]',",
                "        row_filter_template='region = ?',",
                "    )",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    plugin_ref = os.path.relpath(plugin_file, project_root)
    (project_root / "ducksite.toml").write_text(
        "\n".join(
            [
                "[[file_sources]]",
                "name = 'virtual'",
                f"plugin = '{plugin_ref}'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    cfg = load_project_config(project_root)
    build_symlinks(cfg)

    data_map = load_data_map(cfg.site_root)
    filters = load_row_filters(cfg.site_root)

    http_path = "data/virtual/table/data.parquet"
    assert data_map[http_path].endswith("upstream/data.parquet")
    assert filters[http_path] == "region = 'APAC'"
    assert cfg.file_sources[0].template_name == "by_region_[region]"
    assert cfg.file_sources[0].row_filter_template == "region = ?"

    queries = build_file_source_queries(cfg)
    assert http_path in queries["virtual"].sql
    assert "region = 'APAC'" in queries["virtual"].sql

    assert str(plugin_root) not in sys.path
    assert data_map_sqlite_path(cfg.site_root).exists()


def test_plugin_callable_must_be_callable(tmp_path: Path) -> None:
    project_root = tmp_path / "proj"
    project_root.mkdir()

    plugin_file = tmp_path / "plugin.py"
    plugin_file.write_text("NOT_CALLABLE = 1\n", encoding="utf-8")

    plugin_ref = os.path.relpath(plugin_file, project_root)
    (project_root / "ducksite.toml").write_text(
        "\n".join(
            [
                "[[file_sources]]",
                "name = 'virtual'",
                f"plugin = '{plugin_ref}:NOT_CALLABLE'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    cfg = load_project_config(project_root)
    with pytest.raises(TypeError):
        build_symlinks(cfg)


def test_plugin_target_missing(tmp_path: Path) -> None:
    project_root = tmp_path / "proj"
    project_root.mkdir()

    plugin_file = tmp_path / "plugin.py"
    plugin_file.write_text("# missing build_manifest\n", encoding="utf-8")

    plugin_ref = os.path.relpath(plugin_file, project_root)
    (project_root / "ducksite.toml").write_text(
        "\n".join(
            [
                "[[file_sources]]",
                "name = 'virtual'",
                f"plugin = '{plugin_ref}'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    cfg = load_project_config(project_root)
    with pytest.raises(ImportError):
        build_symlinks(cfg)


def test_split_plugin_ref_windows_drive_letter(tmp_path: Path) -> None:
    module_ref, attr = _split_plugin_ref(r"C:\\plugins\\demo.py")

    assert module_ref == r"C:\\plugins\\demo.py"
    assert attr == DEFAULT_PLUGIN_CALLABLE


def test_split_plugin_ref_windows_drive_with_callable(tmp_path: Path) -> None:
    module_ref, attr = _split_plugin_ref(r"C:\\plugins\\demo.py:custom")

    assert module_ref == r"C:\\plugins\\demo.py"
    assert attr == "custom"


def test_blank_plugin_scaffold_is_loadable(tmp_path: Path) -> None:
    project_root = tmp_path / "proj"
    project_root.mkdir()

    plugin_path = write_blank_plugin(project_root, "blank_demo")

    plugin_ref = plugin_path.relative_to(project_root)
    (project_root / "ducksite.toml").write_text(
        "\n".join(
            [
                "[[file_sources]]",
                "name = 'blank'",
                f"plugin = '{plugin_ref.as_posix()}'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    cfg = load_project_config(project_root)
    build_symlinks(cfg)

    data_map_path = data_map_sqlite_path(cfg.site_root)
    assert data_map_path.exists()
    assert load_data_map(cfg.site_root) == {}
    fingerprints = load_fingerprints(cfg.site_root)
    assert fingerprints == {"blank": fingerprints.get("blank")}
