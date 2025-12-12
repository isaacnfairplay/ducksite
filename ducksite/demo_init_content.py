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
            - [Hierarchy endpoints demo](/hierarchy_window/index.html)
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


def _init_hierarchy_page(root: Path) -> None:
    """
    Create a hierarchy demo page at:

      /content/hierarchy/index.md

    This demonstrates combining day/month/year Parquet rolls into a single
    logical source using the hierarchy file-source format.
    """

    hier_md = root / "content" / "hierarchy" / "index.md"
    intro = textwrap.dedent(
        """
        The hierarchy demo shows how one logical source can stitch together
        multiple physical roots. Here we expose recent rows from daily files,
        older rows from monthly files, and archives from yearly files.
        """
    )

    content = _render_page(
        "# Hierarchy Demo (day/month/year rollups)",
        intro,
        [
            (
                "sql",
                "hierarchy_all",
                textwrap.dedent(
                    """
                    SELECT category, period, value
                    FROM demo_hierarchy
                    ORDER BY value;
                    """
                ),
            ),
            (
                "echart",
                "hierarchy_chart",
                textwrap.dedent(
                    """
                    data_query: hierarchy_all
                    type: bar
                    x: category
                    y: value
                    title: "Hierarchy source (daily + monthly + yearly)"
                    """
                ),
            ),
            (
                "grid",
                "cols=12 gap=md",
                "| hierarchy_chart:8 | hierarchy_all:4 |",
            ),
        ],
    )
    write_if_missing(hier_md, content)


def _init_hierarchy_window_page(root: Path) -> None:
    """
    Create an advanced hierarchy demo page at:

      /content/hierarchy_window/index.md

    This highlights before/after endpoints around a templated
    day/month/year rollup.
    """

    window_md = root / "content" / "hierarchy_window" / "index.md"
    intro = textwrap.dedent(
        """
        This page layers higher-fidelity day endpoints around a
        month/year rollup. It also uses templated naming to build
        per-region/per-date variants while keeping all levels stitched
        together in one logical view.
        """
    )

    content = _render_page(
        "# Hierarchy Demo (before/after endpoints + templating)",
        intro,
        [
            (
                "sql",
                "hierarchy_window_all",
                textwrap.dedent(
                    """
                    SELECT
                      region,
                      strftime(max_day, '%Y-%m-%d') AS max_day,
                      period,
                      value
                    FROM demo_hierarchy_window
                    ORDER BY max_day DESC, period;
                    """
                ),
            ),
            (
                "echart",
                "hierarchy_window_chart",
                textwrap.dedent(
                    """
                    data_query: hierarchy_window_all
                    type: bar
                    x: period
                    y: value
                    series: region
                    title: "Templated hierarchy with before/after day windows"
                    """
                ),
            ),
            (
                "grid",
                "cols=12 gap=md",
                "| hierarchy_window_chart:7 | hierarchy_window_all:5 |",
            ),
        ],
    )
    write_if_missing(window_md, content)


def _init_models_page(root: Path) -> None:
    """
    Create a models demo page at:

      /content/models/index.md
    """
    models_md = root / "content" / "models" / "index.md"
    content = _render_page(
        "# Models Demo (sources_sql)",
        "Chained models plus a filtered branch driven by a dropdown. The demo also wires the "
        "demo_plugin_chain plugin so you can pull the same views and file list from a plugin-managed prefix.",
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

    The gallery now shows one clear example for every chart type using the
    NYC taxi parquet (or the bundled fallback sample) so each shape highlights
    its speciality instead of repeating the same bar/line slices.
    """
    gallery_md = root / "content" / "gallery" / "index.md"
    intro = textwrap.dedent(
        """
        One focused example per chart type, driven by the NYC taxi parquet
        downloaded during `ducksite init` (or the bundled fallback sample).
        Each chart highlights a capability: conditional color, trend lines,
        special labels, and option-column examples for combos.
        """
    )

    payment_label = """
        CASE payment_type
          WHEN 1 THEN 'Credit card'
          WHEN 2 THEN 'Cash'
          WHEN 3 THEN 'No charge'
          WHEN 4 THEN 'Dispute'
          ELSE 'Other'
        END
    """

    sql_blocks: list[tuple[str, str, str]] = [
        (
            "sql",
            "nytaxi_hourly_metrics",
            textwrap.dedent(
                f"""
                WITH trips AS (
                  SELECT
                    CAST(strftime(tpep_pickup_datetime, '%H') AS INT) AS hour,
                    trip_distance * 1.60934 AS distance_km,
                    total_amount,
                    passenger_count
                  FROM nytaxi
                  WHERE total_amount IS NOT NULL AND trip_distance IS NOT NULL
                )
                SELECT
                  hour,
                  COUNT(*) AS trip_count,
                  ROUND(AVG(distance_km), 2) AS avg_distance_km,
                  ROUND(SUM(total_amount), 2) AS total_fare
                FROM trips
                GROUP BY hour
                ORDER BY hour;
                """
            ),
        ),
        (
            "sql",
            "nytaxi_payment_split",
            textwrap.dedent(
                f"""
                SELECT
                  {payment_label} AS payment_label,
                  COUNT(*) AS trip_count,
                  ROUND(SUM(total_amount), 2) AS total_fare
                FROM nytaxi
                GROUP BY payment_label
                ORDER BY trip_count DESC;
                """
            ),
        ),
        (
            "sql",
            "nytaxi_scatter_points",
            textwrap.dedent(
                """
                WITH base AS (
                  SELECT
                    trip_distance * 1.60934 AS distance_km,
                    total_amount
                  FROM nytaxi
                  WHERE trip_distance > 0 AND total_amount > 0
                )
                SELECT
                  distance_km * (1 + n * 0.05) AS distance_km,
                  total_amount * (1 + n * 0.03) AS total_amount
                FROM base
                CROSS JOIN range(0, 3) AS n
                LIMIT 180;
                """
            ),
        ),
        (
            "sql",
            "nytaxi_heatmap",
            textwrap.dedent(
                """
                WITH base AS (
                  SELECT
                    CAST(strftime(tpep_pickup_datetime, '%H') AS INT) AS hour,
                    passenger_count
                  FROM nytaxi
                )
                SELECT
                  hour,
                  CASE WHEN passenger_count = 1 THEN 'Solo' ELSE 'Group' END AS passenger_bucket,
                  COUNT(*) AS ride_count
                FROM base
                GROUP BY hour, passenger_bucket
                ORDER BY hour, passenger_bucket;
                """
            ),
        ),
        (
            "sql",
            "nytaxi_tip_percent",
            textwrap.dedent(
                """
                SELECT
                  ROUND(
                    100 * AVG(NULLIF(tip_amount, 0) / NULLIF(total_amount, 0)),
                    1
                  ) AS value
                FROM nytaxi
                WHERE total_amount > 0;
                """
            ),
        ),
        (
            "sql",
            "nytaxi_trip_funnel",
            textwrap.dedent(
                """
                WITH base AS (
                  SELECT * FROM nytaxi WHERE total_amount > 0
                )
                SELECT 'All trips' AS stage, COUNT(*) AS value FROM base
                UNION ALL
                SELECT '2+ passengers', COUNT(*) FROM base WHERE passenger_count >= 2
                UNION ALL
                SELECT 'Over 5 km', COUNT(*) FROM base WHERE trip_distance * 1.60934 > 5
                UNION ALL
                SELECT 'Tipped rides', COUNT(*) FROM base WHERE tip_amount > 0
                ORDER BY value DESC;
                """
            ),
        ),
        (
            "sql",
            "nytaxi_passenger_symbols",
            textwrap.dedent(
                """
                SELECT
                  CASE
                    WHEN passenger_count = 1 THEN 'Solo'
                    WHEN passenger_count = 2 THEN 'Pair'
                    ELSE 'Group'
                  END AS label,
                  ROUND(AVG(total_amount), 2) AS height
                FROM nytaxi
                WHERE total_amount > 0
                GROUP BY label
                ORDER BY height DESC;
                """
            ),
        ),
        (
            "sql",
            "nytaxi_flow_payment",
            textwrap.dedent(
                f"""
                WITH base AS (
                  SELECT
                    CASE WHEN passenger_count = 1 THEN 'Solo' ELSE 'Group' END AS passenger_bucket,
                    {payment_label} AS payment_label
                  FROM nytaxi
                )
                SELECT passenger_bucket AS source, payment_label AS target, COUNT(*) AS value
                FROM base
                GROUP BY passenger_bucket, payment_label
                ORDER BY value DESC;
                """
            ),
        ),
        (
            "sql",
            "nytaxi_box_stats",
            textwrap.dedent(
                f"""
                WITH base AS (
                  SELECT {payment_label} AS payment_label, total_amount
                  FROM nytaxi
                  WHERE total_amount > 0
                )
                SELECT
                  payment_label AS name,
                  MIN(total_amount) AS low,
                  quantile_cont(total_amount, 0.25) AS q1,
                  quantile_cont(total_amount, 0.5) AS median,
                  quantile_cont(total_amount, 0.75) AS q3,
                  MAX(total_amount) AS high
                FROM base
                GROUP BY payment_label
                ORDER BY median DESC;
                """
            ),
        ),
        (
            "sql",
            "nytaxi_candles",
            textwrap.dedent(
                """
                WITH base AS (
                  SELECT
                    date(tpep_pickup_datetime) AS trip_day,
                    total_amount,
                    row_number() OVER (PARTITION BY date(tpep_pickup_datetime) ORDER BY tpep_pickup_datetime) AS rn,
                    row_number() OVER (
                      PARTITION BY date(tpep_pickup_datetime)
                      ORDER BY tpep_pickup_datetime DESC
                    ) AS rn_desc
                  FROM nytaxi
                  WHERE total_amount > 0
                )
                SELECT
                  CAST(trip_day AS VARCHAR) AS name,
                  MAX(CASE WHEN rn = 1 THEN total_amount END) AS open,
                  MAX(CASE WHEN rn_desc = 1 THEN total_amount END) AS close,
                  MIN(total_amount) AS low,
                  MAX(total_amount) AS high
                FROM base
                GROUP BY trip_day
                ORDER BY trip_day;
                """
            ),
        ),
        (
            "sql",
            "nytaxi_radar",
            textwrap.dedent(
                """
                SELECT 'Avg distance (km)' AS indicator, ROUND(AVG(trip_distance * 1.60934), 2) AS value, 60 AS max
                FROM nytaxi
                UNION ALL
                SELECT 'Avg fare ($)', ROUND(AVG(total_amount), 2), 80
                FROM nytaxi
                UNION ALL
                SELECT 'Passengers', ROUND(AVG(passenger_count), 2), 4
                FROM nytaxi;
                """
            ),
        ),
        (
            "sql",
            "nytaxi_treemap",
            textwrap.dedent(
                f"""
                SELECT {payment_label} AS name, ROUND(SUM(total_amount), 2) AS value
                FROM nytaxi
                GROUP BY name
                ORDER BY value DESC;
                """
            ),
        ),
        (
            "sql",
            "nytaxi_sunburst",
            textwrap.dedent(
                """
                SELECT
                  CASE
                    WHEN passenger_count = 1 THEN 'Solo riders'
                    WHEN passenger_count = 2 THEN 'Pairs'
                    ELSE 'Groups'
                  END AS name,
                  COUNT(*) AS value
                FROM nytaxi
                GROUP BY name
                ORDER BY value DESC;
                """
            ),
        ),
    ]

    chart_blocks = [
        (
            "echart",
            "gallery_hourly_bar",
            textwrap.dedent(
                """
                data_query: nytaxi_hourly_metrics
                type: bar
                x: hour
                y: trip_count
                title: "Trips per pickup hour"
                format:
                  trip_count:
                    color_expr: "CASE WHEN trip_count >= 50 THEN '#fb923c' ELSE '#38bdf8' END"
                    highlight_expr: "trip_count >= 50"
                """
            ),
        ),
        (
            "echart",
            "gallery_hourly_line",
            textwrap.dedent(
                """
                data_query: nytaxi_hourly_metrics
                type: line
                x: hour
                y: avg_distance_km
                title: "Average trip distance by hour"
                """
            ),
        ),
        (
            "echart",
            "gallery_scatter_trend",
            textwrap.dedent(
                """
                data_query: nytaxi_scatter_points
                type: scatter
                x: distance_km
                y: total_amount
                trendline: linear
                title: "Fare vs. distance with trend"
                """
            ),
        ),
        (
            "echart",
            "gallery_payment_pie",
            textwrap.dedent(
                """
                data_query: nytaxi_payment_split
                type: pie
                name: payment_label
                value: trip_count
                title: "Payment mix"
                """
            ),
        ),
        (
            "echart",
            "gallery_heatmap",
            textwrap.dedent(
                """
                data_query: nytaxi_heatmap
                type: heatmap
                x: hour
                y: passenger_bucket
                value: ride_count
                title: "Passenger mix by hour"
                """
            ),
        ),
        (
            "echart",
            "gallery_tip_gauge",
            textwrap.dedent(
                """
                data_query: nytaxi_tip_percent
                type: gauge
                name: "Average tip %"
                value: value
                title: "Avg tip share"
                """
            ),
        ),
        (
            "echart",
            "gallery_trip_funnel",
            textwrap.dedent(
                """
                data_query: nytaxi_trip_funnel
                type: funnel
                name: stage
                value: value
                title: "Trip funnel"
                """
            ),
        ),
        (
            "echart",
            "gallery_pictorial",
            textwrap.dedent(
                """
                data_query: nytaxi_passenger_symbols
                type: pictorialBar
                x: label
                y: height
                title: "Average fare by party size"
                """
            ),
        ),
        (
            "echart",
            "gallery_sankey",
            textwrap.dedent(
                """
                data_query: nytaxi_flow_payment
                type: sankey
                source: source
                target: target
                value: value
                title: "Party size to payment type"
                """
            ),
        ),
        (
            "echart",
            "gallery_graph",
            textwrap.dedent(
                """
                data_query: nytaxi_flow_payment
                type: graph
                source: source
                target: target
                value: value
                layout: circular
                title: "Payment relationship graph"
                """
            ),
        ),
        (
            "echart",
            "gallery_boxplot",
            textwrap.dedent(
                """
                data_query: nytaxi_box_stats
                type: boxplot
                name: name
                low: low
                q1: q1
                median: median
                q3: q3
                high: high
                title: "Fare distribution by payment"
                """
            ),
        ),
        (
            "echart",
            "gallery_candles",
            textwrap.dedent(
                """
                data_query: nytaxi_candles
                type: candlestick
                name: name
                open: open
                close: close
                low: low
                high: high
                title: "Daily fare candles"
                """
            ),
        ),
        (
            "echart",
            "gallery_radar",
            textwrap.dedent(
                """
                data_query: nytaxi_radar
                type: radar
                indicator: indicator
                value: value
                max: max
                title: "Taxi metric radar"
                """
            ),
        ),
        (
            "echart",
            "gallery_treemap",
            textwrap.dedent(
                """
                data_query: nytaxi_treemap
                type: treemap
                name: name
                value: value
                title: "Fare share treemap"
                """
            ),
        ),
        (
            "echart",
            "gallery_sunburst",
            textwrap.dedent(
                """
                data_query: nytaxi_sunburst
                type: sunburst
                name: name
                value: value
                title: "Rider group sunburst"
                """
            ),
        ),
    ]

    grids = [
        "| gallery_hourly_bar:6 | gallery_hourly_line:6 |",
        "| gallery_scatter_trend:12 |",
        "| gallery_payment_pie:4 | gallery_heatmap:4 | gallery_tip_gauge:4 |",
        "| gallery_trip_funnel:4 | gallery_pictorial:4 | gallery_radar:4 |",
        "| gallery_sankey:6 | gallery_graph:6 |",
        "| gallery_boxplot:6 | gallery_candles:6 |",
        "| gallery_treemap:6 | gallery_sunburst:6 |",
    ]

    grid_blocks = [
        (
            "grid",
            f"cols=12 gap=md row={idx + 1:02d}",
            row,
        )
        for idx, row in enumerate(grids)
    ]

    content = _render_page(
        "# Chart Gallery",
        intro,
        sql_blocks + chart_blocks + grid_blocks,
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
    _init_hierarchy_page(root)
    _init_hierarchy_window_page(root)
    _init_models_page(root)
    _init_template_page(root)
    _init_forms_page(root)
    _init_gallery_page(root)
