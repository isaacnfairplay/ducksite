from __future__ import annotations
from pathlib import Path
import textwrap

from .demo_init_common import write_if_missing
from .tuy_sql import add_model_block
from .utils import ensure_dir


def init_demo_sources_sql(root: Path) -> None:
    """
    Ensure the sources_sql directory exists and contains small demos
    that exercise:

      - chained model views
      - a templated model using ${params.*}
      - a filename-template example: demo_template_[category].sql
    """
    sql_root = root / "sources_sql"
    ensure_dir(sql_root)
    print(f"[ducksite:init] ensured {sql_root}")

    demo_models = sql_root / "demo_models.sql"
    model_blocks = [
        (
            "demo_chain_base",
            """
            SELECT
              category,
              value
            FROM demo;
            """,
        ),
        (
            "demo_chain_agg",
            """
            SELECT
              category,
              SUM(value) AS total_value
            FROM demo_chain_base
            GROUP BY category
            ORDER BY category;
            """,
        ),
        (
            "demo_chain_options",
            """
            SELECT DISTINCT
              category AS value,
              category AS label
            FROM demo_chain_base
            ORDER BY value;
            """,
        ),
        (
            "demo_chain_filtered",
            """
            SELECT
              category,
              value
            FROM demo_chain_base
            WHERE ${params.category_filter}
            ORDER BY category, value;
            """,
        ),
    ]

    text = "-- demo_models.sql - chained and templated models for ducksite demos\n\n"
    for name, body in model_blocks:
        text = add_model_block(text, name, body)
    write_if_missing(demo_models, text)

    demo_template = sql_root / "demo_template_[category].sql"
    template_blocks = [
        (
            "demo_template_base",
            """
            SELECT
              category,
              value
            FROM demo;
            """,
        ),
        (
            "demo_template_options",
            """
            SELECT DISTINCT
              category AS value,
              category AS label
            FROM demo_template_base
            ORDER BY value;
            """,
        ),
        (
            "demo_template_filtered",
            """
            SELECT
              category,
              value
            FROM demo_template_base
            WHERE category = 'A'
            ORDER BY category, value;
            """,
        ),
    ]

    text = textwrap.dedent(
        """
        -- demo_template_[category].sql
        --
        -- Example of a model file that uses a filename template-style pattern.
        --
        -- The `[category]` segment in the filename is a real DuckDB
        -- expression (a simple column reference). For your own project
        -- you could use something richer like [left(Barcode,10)].
        """
    ).strip() + "\n\n"
    for name, body in template_blocks:
        text = add_model_block(text, name, body)
    write_if_missing(demo_template, text)
