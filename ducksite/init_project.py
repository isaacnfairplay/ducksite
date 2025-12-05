from __future__ import annotations
from pathlib import Path

from .demo_init_toml import init_demo_toml
from .demo_init_content import init_demo_content
from .demo_init_sources_sql import init_demo_sources_sql
from .demo_init_fake_parquet import init_demo_fake_parquet
from .demo_init_virtual_plugin import init_demo_virtual_plugin
from .demo_init_common import write_if_missing
from .tuy_toml import render_config_text
from .utils import ensure_dir


def init_project(root: Path) -> None:
    """Initialise a *barebones* ducksite project structure under `root`."""

    print(f"[ducksite:init] initializing barebones project under {root}")
    _init_barebones(root)
    print("[ducksite:init] done.")


def init_demo_project(root: Path) -> None:
    """Initialise a barebones project, then lay down the richer demo assets."""

    print(f"[ducksite:demo] initializing demo project under {root}")
    _init_barebones(root)
    _init_demo(root)
    print("[ducksite:demo] done.")


def _init_demo(root: Path) -> None:
    init_demo_toml(root)
    init_demo_content(root)
    init_demo_sources_sql(root)
    init_demo_fake_parquet(root)
    init_demo_virtual_plugin(root)


def _init_barebones(root: Path) -> None:
    ensure_dir(root / "content")
    ensure_dir(root / "sources_sql")
    ensure_dir(root / "static" / "forms")

    toml_path = root / "ducksite.toml"
    content = render_config_text(root, dirs={"DIR_FORMS": "static/forms"})

    write_if_missing(toml_path, content)


if __name__ == "__main__":
    init_project(Path(".").resolve())
