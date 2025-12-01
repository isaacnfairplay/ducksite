from __future__ import annotations
from pathlib import Path

from .demo_init_toml import init_demo_toml
from .demo_init_content import init_demo_content
from .demo_init_sources_sql import init_demo_sources_sql
from .demo_init_fake_parquet import init_demo_fake_parquet


def init_project(root: Path) -> None:
    """
    Initialise a demo ducksite project structure under `root`.

    Creates:
      - ducksite.toml
      - content/index.md
      - content/major/index.md
      - content/minor/index.md
      - content/filters/index.md
      - content/cross_filters/index.md
      - content/forms/index.md
      - content/models/index.md
      - content/template/index.md
      - sources_sql/demo_models.sql
      - sources_sql/demo_template_[category].sql
      - fake_upstream/demo-data.parquet
    """
    print(f"[ducksite:init] initializing project under {root}")
    init_demo_toml(root)
    init_demo_content(root)
    init_demo_sources_sql(root)
    init_demo_fake_parquet(root)
    print("[ducksite:init] done.")


if __name__ == "__main__":
    init_project(Path(".").resolve())
