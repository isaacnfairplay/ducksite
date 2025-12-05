from __future__ import annotations

from pathlib import Path
from textwrap import dedent

from .demo_init_common import write_if_missing
from .utils import ensure_dir

DEMO_PLUGIN_NAME = "demo_plugin"


def _render_demo_plugin() -> str:
    template = dedent(
        '''
        from pathlib import Path

        from ducksite.virtual_parquet import VirtualParquetFile, VirtualParquetManifest


        def build_manifest(cfg):
            """
            Demo virtual parquet plugin.

            It mirrors the demo-A/B/C Parquet files under a separate logical
            root so you can see plugin-driven file sources without extra
            configuration.
            """
            upstream = Path(cfg.root) / "fake_upstream"
            files = []
            for path in sorted(upstream.glob("demo-*.parquet")):
                files.append(
                    VirtualParquetFile(
                        http_path="data/{plugin_name}/" + path.name,
                        physical_path=str(path),
                    )
                )
            return VirtualParquetManifest(
                files=files,
                template_name="demo_plugin_[category]",
                row_filter_template="category = ?",
            )
        '''
    ).format(plugin_name=DEMO_PLUGIN_NAME)
    return template.lstrip() + "\n"


def init_demo_virtual_plugin(root: Path) -> Path:
    plugin_dir = root / "plugins"
    ensure_dir(plugin_dir)
    dest = plugin_dir / f"{DEMO_PLUGIN_NAME}.py"
    write_if_missing(dest, _render_demo_plugin())
    return dest
