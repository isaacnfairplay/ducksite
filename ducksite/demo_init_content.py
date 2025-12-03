from __future__ import annotations
from pathlib import Path
import textwrap

from .demo_init_common import write_if_missing
from .tuy_md import add_markdown_block
from .utils import ensure_dir


def _render_page(title: str, intro: str, blocks: list[tuple[str, str, str]]) -> str:
    text = f"{title}\n\n{intro.strip()}\n"
    for kind, block_id, body in blocks:
        text = add_markdown_block(text, kind, block_id, body)
    return text


def _init_root_page(root: Path) -> None:
    """
    Create the top-level index markdown page:

      /content/index.md
    """
    index_md = root / "content" / "index.md"
    content = _render_page(
        "# Demo Dashboard (root)",
        textwrap.dedent(
            """
            This is the top-level dashboard. See:
            - [Major summary](/major/index.html)
            - [Minor detail](/minor/index.html)
            - [Filters demo](/filters/index.html)
            - [Cross filters demo](/cross_filters/index.html)
            - [Derived templated demo](/derived_filters/index.html)
            - [Models demo](/models/index.html)
            - [Template models demo](/template/index.html)
            - [Chart gallery](/gallery/index.html)
            - [Forms demo](/forms/index.html)
            """
        ),
        [
            (
                "sql",
                "demo_summary",
                textwrap.dedent(
                    """
                    SELECT
                      category,
                      SUM(value) AS total_value
                    FROM demo
                    GROUP BY category
                    ORDER BY category;
                    """
                ),
            ),
            (
                "echart",
                "category_chart",
                textwrap.dedent(
                    """
                    data_query: demo_summary
                    type: bar
                    x: category
                    y: total_value
                    title: "Total Value by Category (Root)"
                    """
                ),
            ),
            (
                "grid",
                "cols=12 gap=md",
                "| category_chart:12 |",
            ),
        ],
    )
    write_if_missing(index_md, content)


def _init_major_page(root: Path) -> None:
    """
    Create a "major" summary page at:

      /content/major/index.md
    """
    major_md = root / "content" / "major" / "index.md"
    content = _render_page(
        "# Major Summary",
        "",
        [
            (
                "sql",
                "major_summary",
                textwrap.dedent(
                    """
                    SELECT
                      category,
                      SUM(value) AS total_value
                    FROM demo
                    GROUP BY category
                    ORDER BY total_value DESC;
                    """
                ),
            ),
            (
                "echart",
                "major_chart",
                textwrap.dedent(
                    """
                    data_query: major_summary
                    type: bar
                    x: category
                    y: total_value
                    title: "Major Sum by Category"
                    """
                ),
            ),
            (
                "grid",
                "cols=12 gap=md",
                "| major_chart:12 |",
            ),
        ],
    )
    write_if_missing(major_md, content)


def _init_minor_page(root: Path) -> None:
    """
    Create a "minor" detail page at:

      /content/minor/index.md
    """
    minor_md = root / "content" / "minor" / "index.md"
    content = _render_page(
        "# Minor Detail",
        "",
        [
            (
                "sql",
                "minor_rows",
                textwrap.dedent(
                    """
                    SELECT
                      category,
                      value
                    FROM demo
                    ORDER BY category, value;
                    """
                ),
            ),
            (
                "grid",
                "cols=12 gap=md",
                "| minor_rows:12 |",
            ),
        ],
    )
    write_if_missing(minor_md, content)


def _init_filters_page(root: Path) -> None:
    """
    Create a filters demo page at:

      /content/filters/index.md
    """
    filters_md = root / "content" / "filters" / "index.md"
    intro = "Filters paired to downstream SQL and charts."
    content = _render_page(
        "# Filters Demo",
        intro,
        [
            (
                "input",
                "category_filter",
                textwrap.dedent(
                    """
                    label: Category filter
                    visual_mode: dropdown
                    url_key: category
                    options_query: category_filter_options
                    expression_template: "category = ?"
                    all_label: ALL
                    all_expression: "TRUE"
                    default: ALL
                    """
                ),
            ),
            (
                "sql",
                "category_filter_options",
                textwrap.dedent(
                    """
                    SELECT DISTINCT
                      category AS value
                    FROM demo
                    ORDER BY value;
                    """
                ),
            ),
            (
                "sql",
                "filtered_demo",
                textwrap.dedent(
                    """
                    SELECT
                      category,
                      value
                    FROM demo
                    WHERE ${params.category_filter}
                    ORDER BY category, value;
                    """
                ),
            ),
            (
                "echart",
                "filtered_chart",
                textwrap.dedent(
                    """
                    data_query: filtered_demo
                    type: bar
                    x: category
                    y: value
                    title: "Filtered Values by Category"
                    """
                ),
            ),
            (
                "grid",
                "cols=12 gap=md",
                "| filtered_chart:8 | filtered_demo:4 |",
            ),
        ],
    )
    write_if_missing(filters_md, content)


def _init_cross_filters_page(root: Path) -> None:
    """
    Create a cross-filters demo page at:

      /content/cross_filters/index.md
    """
    cf_md = root / "content" / "cross_filters" / "index.md"
    content = _render_page(
        "# Cross Filters Demo",
        "Linked dropdowns and SQL queries cross-filtering each other.",
        [
            (
                "input",
                "cf_category",
                textwrap.dedent(
                    """
                    label: Category
                    visual_mode: dropdown
                    url_key: cf_category
                    options_query: cf_category_options
                    expression_template: "category = ?"
                    all_label: ALL
                    all_expression: "TRUE"
                    default: ALL
                    """
                ),
            ),
            (
                "input",
                "cf_value_min",
                textwrap.dedent(
                    """
                    label: Minimum value
                    visual_mode: dropdown
                    url_key: cf_min
                    options_query: cf_value_options
                    expression_template: "value >= ?"
                    all_label: ALL
                    all_expression: "TRUE"
                    default: ALL
                    """
                ),
            ),
            (
                "sql",
                "cf_category_options",
                textwrap.dedent(
                    """
                    SELECT DISTINCT
                      category AS value,
                      category AS label
                    FROM demo
                    WHERE ${params.cf_value_min}
                    ORDER BY value;
                    """
                ),
            ),
            (
                "sql",
                "cf_value_options",
                textwrap.dedent(
                    """
                    SELECT DISTINCT
                      value AS value,
                      CAST(value AS VARCHAR) AS label
                    FROM demo
                    WHERE ${params.cf_category}
                    ORDER BY value;
                    """
                ),
            ),
            (
                "sql",
                "cf_filtered_rows",
                textwrap.dedent(
                    """
                    SELECT
                      category,
                      value
                    FROM demo
                    WHERE ${params.cf_category}
                      AND ${params.cf_value_min}
                    ORDER BY category, value;
                    """
                ),
            ),
            (
                "echart",
                "cf_chart",
                textwrap.dedent(
                    """
                    data_query: cf_filtered_rows
                    type: bar
                    x: category
                    y: value
                    title: "Cross-filtered values by category"
                    """
                ),
            ),
            (
                "grid",
                "cols=12 gap=md",
                "| cf_chart:8 | cf_filtered_rows:4 |",
            ),
        ],
    )
    write_if_missing(cf_md, content)


def _init_derived_filters_page(root: Path) -> None:
    """
    Create a derived-filters demo page at:

      /content/derived_filters/index.md

    This demonstrates selecting a **templated Parquet source view** based on a
    text input, using the file-source template:

        [[file_sources]]
        name = "demo"
        template_name = "demo_[category]"
        pattern = "data/demo/*.parquet"
        upstream_glob = "${DIR_FAKE}/demo-*.parquet"
        row_filter_template = "category = ?"
    """
    df_md = root / "content" / "derived_filters" / "index.md"
    intro = textwrap.dedent(
        """
        This page shows how a **text input** can pick a compiled templated view
        (demo_[category]) generated from the Parquet file-source template.
        The first character of the barcode drives both a SQL parameter and the
        selected global model name (demo_A, demo_B, ...).
        """
    )

    content = _render_page(
        "# Derived Templated Demo (text input → templated Parquet view)",
        intro,
        [
            (
                "input",
                "barcode",
                textwrap.dedent(
                    """
                    label: Barcode (first char = category)
                    visual_mode: text
                    url_key: barcode
                    placeholder: "Scan or type a code like A1234567890"
                    param_name: barcode_prefix
                    param_template: "left(?, 1)"
                    """
                ),
            ),
            (
                "echart",
                "barcode_templated_chart",
                textwrap.dedent(
                    """
                    data_query: "global:demo_${inputs.barcode_prefix}"
                    type: bar
                    x: category
                    y: value
                    title: "Templated Parquet view: demo_${inputs.barcode_prefix}"
                    """
                ),
            ),
            (
                "grid",
                "cols=12 gap=md",
                "| barcode_templated_chart:12 |",
            ),
        ],
    )
    write_if_missing(df_md, content)


def _init_models_page(root: Path) -> None:
    """
    Create a models demo page at:

      /content/models/index.md
    """
    models_md = root / "content" / "models" / "index.md"
    content = _render_page(
        "# Models Demo (sources_sql)",
        "Chained models plus a filtered branch driven by a dropdown.",
        [
            (
                "input",
                "category_filter",
                textwrap.dedent(
                    """
                    label: Category filter (models)
                    visual_mode: dropdown
                    url_key: category
                    options_query: model_options
                    expression_template: "category = ?"
                    all_label: ALL
                    all_expression: "TRUE"
                    default: ALL
                    """
                ),
            ),
            (
                "sql",
                "model_options",
                textwrap.dedent(
                    """
                    SELECT
                      value,
                      label
                    FROM demo_chain_options;
                    """
                ),
            ),
            (
                "sql",
                "model_chain_page",
                textwrap.dedent(
                    """
                    SELECT
                      category,
                      total_value
                    FROM demo_chain_agg
                    ORDER BY category;
                    """
                ),
            ),
            (
                "sql",
                "model_filtered_page",
                textwrap.dedent(
                    """
                    SELECT
                      category,
                      value
                    FROM demo_chain_filtered
                    ORDER BY category, value;
                    """
                ),
            ),
            (
                "echart",
                "model_chain_chart",
                textwrap.dedent(
                    """
                    data_query: model_chain_page
                    type: bar
                    x: category
                    y: total_value
                    title: "Chained model: demo_chain_agg"
                    """
                ),
            ),
            (
                "grid",
                "cols=12 gap=md",
                "| model_chain_chart:8 | model_filtered_page:4 |",
            ),
        ],
    )
    write_if_missing(models_md, content)


def _init_template_page(root: Path) -> None:
    """
    Create a template models demo page at:

      /content/template/index.md
    """
    tmpl_md = root / "content" / "template" / "index.md"
    content = _render_page(
        "# Template Models Demo (file-source expansion + dropdown)",
        "Pick a templated model compiled from the demo Parquet source.",
        [
            (
                "input",
                "template_view",
                textwrap.dedent(
                    """
                    label: Templated view
                    visual_mode: dropdown
                    url_key: template_view
                    options_query: template_view_options
                    default: demo
                    """
                ),
            ),
            (
                "sql",
                "template_view_options",
                textwrap.dedent(
                    """
                    SELECT 'demo'      AS value, 'All categories (demo)' AS label
                    UNION ALL
                    SELECT 'demo_' || category AS value,
                           'Category ' || category AS label
                    FROM (
                      SELECT DISTINCT category
                      FROM demo
                    )
                    ORDER BY value;
                    """
                ),
            ),
            (
                "sql",
                "template_demo_all",
                textwrap.dedent(
                    """
                    SELECT
                      category,
                      value
                    FROM demo
                    ORDER BY category, value;
                    """
                ),
            ),
            (
                "echart",
                "template_chart_selected",
                textwrap.dedent(
                    """
                    data_query: "global:${inputs.template_view}"
                    type: bar
                    x: category
                    y: value
                    title: "Templated view (global: ${inputs.template_view})"
                    """
                ),
            ),
            (
                "grid",
                "cols=12 gap=md",
                "| template_demo_all:6 | template_chart_selected:6 |",
            ),
        ],
    )
    write_if_missing(tmpl_md, content)


def _init_forms_page(root: Path) -> None:
    """Create a forms demo that appends to a CSV and reads it back."""

    csv_path = root / "static" / "forms" / "feedback.csv"
    if not csv_path.exists():
        ensure_dir(csv_path.parent)
        csv_path.write_text(
            """category,comment,severity,submitted_by,submitted_at
General,"Existing demo feedback","3","demo@example.com","2024-01-01T00:00:00Z"
""",
            encoding="utf-8",
        )
        print(f"[ducksite:init] wrote {csv_path}")

    forms_md = root / "content" / "forms" / "index.md"
    content = _render_page(
        "# Forms Demo",
        "This page reuses Ducksite inputs to collect new rows into a CSV.",
        [
            (
                "input",
                "feedback_category",
                textwrap.dedent(
                    """
                    label: Feedback category
                    visual_mode: dropdown
                    options_query: feedback_categories
                    default: "General"
                    """
                ),
            ),
            (
                "input",
                "feedback_text",
                textwrap.dedent(
                    """
                    label: Feedback comment
                    visual_mode: text
                    placeholder: "Share a quick note"
                    """
                ),
            ),
            (
                "input",
                "feedback_severity",
                textwrap.dedent(
                    """
                    label: Severity (1-5)
                    visual_mode: text
                    placeholder: "3"
                    """
                ),
            ),
            (
                "sql",
                "feedback_categories",
                textwrap.dedent(
                    """
                    SELECT 'General' AS value, 'General' AS label
                    UNION ALL SELECT 'Bug', 'Bug'
                    UNION ALL SELECT 'Idea', 'Idea';
                    """
                ),
            ),
            (
                "form",
                "feedback_form",
                textwrap.dedent(
                    """
                    label: "Submit feedback"
                    target_csv: "${DIR_FORMS}/feedback.csv"
                    auth_required: true
                    inputs: ["feedback_category", "feedback_text", "feedback_severity"]
                    max_rows_per_user: 10
                    sql_relation_query: |
                      SELECT
                        ${inputs.feedback_category} AS category,
                        ${inputs.feedback_text} AS comment,
                        ${inputs.feedback_severity} AS severity,
                        ${inputs._user_email} AS submitted_by,
                        now() AS submitted_at
                    """
                ),
            ),
            (
                "sql",
                "feedback_rows",
                textwrap.dedent(
                    """
                    SELECT *
                    FROM read_csv_auto('forms/feedback.csv', HEADER=TRUE, ALL_VARCHAR=TRUE)
                    ORDER BY submitted_at DESC;
                    """
                ),
            ),
            (
                "grid",
                "cols=12 gap=md",
                "| feedback_rows:12 |",
            ),
        ],
    )
    write_if_missing(forms_md, content)




def _init_gallery_page(root: Path) -> None:
    """
    Create a chart gallery page at:

      /content/gallery/index.md

    This version keeps a small, easy-to-read sample of chart types while
    relying on TUY helpers to assemble the markdown.
    """
    gallery_md = root / "content" / "gallery" / "index.md"
    intro = "Compact gallery of common chart shapes using TUY markdown helpers."

    base_blocks = [
        (
            "sql",
            "gallery_q1_totals",
            """
            SELECT
              category,
              SUM(value) AS total_value
            FROM demo
            GROUP BY category
            ORDER BY category;
            """,
        ),
        (
            "sql",
            "gallery_q2_average",
            """
            SELECT
              category,
              AVG(value) AS avg_value
            FROM demo
            GROUP BY category
            ORDER BY category;
            """,
        ),
        (
            "sql",
            "gallery_q3_hist",
            """
            SELECT
              value AS bucket,
              COUNT(*) AS row_count
            FROM demo
            GROUP BY value
            ORDER BY value;
            """,
        ),
        (
            "sql",
            "gallery_q4_cumulative",
            """
            SELECT
              category,
              SUM(value) AS total_value,
              SUM(SUM(value)) OVER (ORDER BY category) AS running_total
            FROM demo
            GROUP BY category
            ORDER BY category;
            """,
        ),
        (
            "sql",
            "gallery_q5_scaled",
            """
            SELECT
              category,
              value * 2 AS double_value
            FROM demo
            ORDER BY category, value;
            """,
        ),
    ]

    chart_templates = [
        ("gallery_q1_totals", "bar", "category", "total_value", "Totals"),
        ("gallery_q1_totals", "line", "category", "total_value", "Totals"),
        ("gallery_q2_average", "bar", "category", "avg_value", "Averages"),
        ("gallery_q2_average", "line", "category", "avg_value", "Averages"),
        ("gallery_q3_hist", "bar", "bucket", "row_count", "Histogram"),
        ("gallery_q3_hist", "line", "bucket", "row_count", "Histogram"),
        ("gallery_q4_cumulative", "bar", "category", "running_total", "Cumulative"),
        ("gallery_q4_cumulative", "line", "category", "running_total", "Cumulative"),
        ("gallery_q5_scaled", "bar", "category", "double_value", "Scaled"),
        ("gallery_q5_scaled", "line", "category", "double_value", "Scaled"),
    ]

    chart_blocks: list[tuple[str, str, str]] = []
    for idx in range(50):
        query, chart_type, x_field, y_field, title_prefix = chart_templates[idx % len(chart_templates)]
        chart_id = f"gallery_{idx + 1:02d}"
        body = textwrap.dedent(
            f"""
            data_query: {query}
            type: {chart_type}
            x: {x_field}
            y: {y_field}
            title: "{title_prefix} ({chart_type}) {idx + 1:02d}"
            """
        )
        chart_blocks.append(("echart", chart_id, body))

    content = _render_page(
        "# Chart Gallery",
        intro,
        base_blocks + chart_blocks,
    )
    write_if_missing(gallery_md, content)


def init_demo_content(root: Path) -> None:
    """
    Ensure all demo content pages exist.
    """
    _init_root_page(root)
    _init_major_page(root)
    _init_minor_page(root)
    _init_filters_page(root)
    _init_cross_filters_page(root)
    _init_derived_filters_page(root)
    _init_models_page(root)
    _init_template_page(root)
    _init_forms_page(root)
    _init_gallery_page(root)
