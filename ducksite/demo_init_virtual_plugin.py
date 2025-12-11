from __future__ import annotations

from pathlib import Path
from textwrap import dedent

from .demo_init_common import write_if_missing
from .utils import ensure_dir

DEMO_PLUGIN_NAME = "demo_plugin"
CHAIN_PLUGIN_NAME = "demo_plugin_chain"


def _render_demo_plugin() -> str:
    template = dedent(
        '''
        from pathlib import Path

        from ducksite.virtual_parquet import VirtualParquetManifest, manifest_from_parquet_dir


        def build_manifest(cfg):
            """
            Demo virtual parquet plugin.

            It mirrors the demo-A/B/C Parquet files under a separate logical
            root so you can see plugin-driven file sources without extra
            configuration.
            """
            return manifest_from_parquet_dir(
                cfg,
                Path(cfg.root) / "fake_upstream",
                http_prefix="data/{plugin_name}",
                template_name="demo_plugin_[category]",
                row_filter_template="category = ?",
            )
        '''
    ).format(plugin_name=DEMO_PLUGIN_NAME)
    return template.lstrip() + "\n"


def _render_chained_plugin() -> str:
    template = dedent(
        '''
        from ducksite.virtual_parquet import (
            VirtualParquetManifest,
            manifest_from_file_source,
            manifest_from_model_views,
        )


        def build_manifest(cfg):
            """
            Chain demo views and file lists without repeating the globs.

            - Reuses the demo file source under data/{chain_name}/files/...
            - Reuses demo model views under data/{chain_name}/views/...
            """

            base_files = manifest_from_file_source(
                cfg, "demo", http_prefix="data/{chain_name}/files"
            )
            model_files = manifest_from_model_views(
                cfg,
                ["demo_chain_base", "demo_chain_agg"],
                http_prefix="data/{chain_name}/views",
            )
            return VirtualParquetManifest(files=base_files.files + model_files.files)
        '''
    ).format(chain_name=CHAIN_PLUGIN_NAME)
    return template.lstrip() + "\n"


def init_demo_virtual_plugin(root: Path) -> Path:
    plugin_dir = root / "plugins"
    ensure_dir(plugin_dir)
    dest = plugin_dir / f"{DEMO_PLUGIN_NAME}.py"
    write_if_missing(dest, _render_demo_plugin())
    chained_dest = plugin_dir / f"{CHAIN_PLUGIN_NAME}.py"
    write_if_missing(chained_dest, _render_chained_plugin())
    return dest
