from pathlib import Path
from textwrap import dedent

import duckdb
import pytest

from ducksite.config import load_project_config
from ducksite.data_map_cache import load_data_map
from ducksite.queries import build_file_source_queries
from ducksite.symlinks import build_symlinks
from ducksite.virtual_parquet import (
    manifest_from_model_views,
    manifest_from_parquet_dir,
)


def _write_demo_parquet(path: Path, category: str, value: int) -> None:
    con = duckdb.connect()
    try:
        con.execute(
            f"""
            COPY (
              SELECT '{category}'::VARCHAR AS category, {value}::INT AS value
            ) TO '{path}' (FORMAT 'parquet');
            """
        )
    finally:
        con.close()


def test_manifest_helpers_chain_file_and_model_views(tmp_path: Path) -> None:
    project_root = tmp_path / "proj"
    upstream = project_root / "upstream"
    plugins = project_root / "plugins"
    sources_sql = project_root / "sources_sql"

    upstream.mkdir(parents=True)
    plugins.mkdir(parents=True)
    sources_sql.mkdir(parents=True)

    _write_demo_parquet(upstream / "one.parquet", "A", 1)
    _write_demo_parquet(upstream / "two.parquet", "B", 2)

    (sources_sql / "models.sql").write_text(
        "\n".join(
            [
                "-- name: base_model",
                "SELECT * FROM demo",
                "",
            ]
        ),
        encoding="utf-8",
    )

    plugin_path = plugins / "proxy.py"
    plugin_path.write_text(
        dedent(
            """
            from ducksite.virtual_parquet import (
                VirtualParquetManifest,
                manifest_from_file_source,
                manifest_from_model_views,
            )


            def build_manifest(cfg):
                base = manifest_from_file_source(cfg, "demo", http_prefix="data/proxy/files")
                view = manifest_from_model_views(cfg, "base_model", http_prefix="data/proxy/views")
                return VirtualParquetManifest(files=base.files + view.files)
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    (project_root / "ducksite.toml").write_text(
        "\n".join(
            [
                "[[file_sources]]",
                "name = 'demo'",
                "pattern = 'data/demo/*.parquet'",
                "upstream_glob = 'upstream/*.parquet'",
                "",
                "[[file_sources]]",
                "name = 'proxy'",
                "pattern = 'data/proxy/**/*.parquet'",
                "plugin = 'plugins/proxy.py'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    cfg = load_project_config(project_root)
    build_symlinks(cfg)

    data_map = load_data_map(cfg.site_root)

    proxy_keys = [k for k in data_map if k.startswith("data/proxy/")]
    assert proxy_keys
    assert "data/proxy/files/demo/one.parquet" in proxy_keys
    assert "data/proxy/views/demo/two.parquet" in proxy_keys

    queries = build_file_source_queries(cfg)
    proxy_sql = queries["proxy"].sql
    assert "data/proxy/files/demo/one.parquet" in proxy_sql
    assert "data/proxy/views/demo/one.parquet" in proxy_sql


def test_manifest_from_parquet_dir_handles_nested_parquets(tmp_path: Path) -> None:
    project_root = tmp_path / "proj"
    upstream = project_root / "upstream"
    nested = upstream / "nested"
    project_root.mkdir(parents=True)
    (project_root / "ducksite.toml").write_text("\n", encoding="utf-8")

    nested.mkdir(parents=True)
    _write_demo_parquet(upstream / "root.parquet", "A", 1)
    _write_demo_parquet(nested / "child.parquet", "B", 2)

    cfg = load_project_config(project_root)
    result = manifest_from_parquet_dir(
        cfg, upstream, http_prefix="data/cache", recursive=True
    )

    http_paths = [f.http_path for f in result.files]
    assert http_paths == [
        "data/cache/nested/child.parquet",
        "data/cache/root.parquet",
    ]
    assert all(Path(f.physical_path).exists() for f in result.files)


def test_manifest_from_parquet_dir_empty_and_missing(tmp_path: Path) -> None:
    project_root = tmp_path / "proj"
    upstream = project_root / "upstream"
    project_root.mkdir(parents=True)
    (project_root / "ducksite.toml").write_text("\n", encoding="utf-8")
    cfg = load_project_config(project_root)

    missing = upstream / "missing"
    with pytest.raises(FileNotFoundError):
        manifest_from_parquet_dir(
            cfg,
            missing,
            http_prefix="data/cache",
        )

    upstream.mkdir(parents=True)
    with pytest.raises(ValueError):
        manifest_from_parquet_dir(
            cfg,
            upstream,
            http_prefix="data/cache",
        )


def test_manifest_from_model_views_rejects_empty_view_list(tmp_path: Path) -> None:
    project_root = tmp_path / "proj"
    project_root.mkdir(parents=True)
    (project_root / "ducksite.toml").write_text("\n", encoding="utf-8")
    cfg = load_project_config(project_root)

    with pytest.raises(ValueError):
        manifest_from_model_views(cfg, [])
