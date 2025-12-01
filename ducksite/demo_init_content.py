from __future__ import annotations
from pathlib import Path
import textwrap

from .demo_init_common import write_if_missing


def _init_root_page(root: Path) -> None:
    """
    Create the top-level index markdown page:

      /content/index.md
    """
    index_md = root / "content" / "index.md"
    content = (
        textwrap.dedent(
            """
            # Demo Dashboard (root)

            This is the top-level dashboard. See:
            - [Major summary](/major/index.html)
            - [Minor detail](/minor/index.html)
            - [Filters demo](/filters/index.html)
            - [Cross filters demo](/cross_filters/index.html)
            - [Derived templated demo](/derived_filters/index.html)
            - [Models demo](/models/index.html)
            - [Template models demo](/template/index.html)
            - [Chart gallery](/gallery/index.html)

            ```sql demo_summary
            SELECT
              category,
              SUM(value) AS total_value
            FROM demo
            GROUP BY category
            ORDER BY category;
            ```

            ```echart category_chart
            data_query: demo_summary
            type: bar
            x: category
            y: total_value
            title: "Total Value by Category (Root)"
            ```

            ```grid cols=12 gap=md
            | category_chart:12 |
            ```
            """
        ).strip()
        + "\n"
    )
    write_if_missing(index_md, content)


def _init_major_page(root: Path) -> None:
    """
    Create a "major" summary page at:

      /content/major/index.md
    """
    major_md = root / "content" / "major" / "index.md"
    content = (
        textwrap.dedent(
            """
            # Major Summary

            ```sql major_summary
            SELECT
              category,
              SUM(value) AS total_value
            FROM demo
            GROUP BY category
            ORDER BY total_value DESC;
            ```

            ```echart major_chart
            data_query: major_summary
            type: bar
            x: category
            y: total_value
            title: "Major Sum by Category"
            ```

            ```grid cols=12 gap=md
            | major_chart:12 |
            ```
            """
        ).strip()
        + "\n"
    )
    write_if_missing(major_md, content)


def _init_minor_page(root: Path) -> None:
    """
    Create a "minor" detail page at:

      /content/minor/index.md
    """
    minor_md = root / "content" / "minor" / "index.md"
    content = (
        textwrap.dedent(
            """
            # Minor Detail

            ```sql minor_rows
            SELECT
              category,
              value
            FROM demo
            ORDER BY category, value;
            ```

            ```grid cols=12 gap=md
            | minor_rows:12 |
            ```
            """
        ).strip()
        + "\n"
    )
    write_if_missing(minor_md, content)


def _init_filters_page(root: Path) -> None:
    """
    Create a filters demo page at:

      /content/filters/index.md
    """
    filters_md = root / "content" / "filters" / "index.md"
    content = (
        textwrap.dedent(
            """
            # Filters Demo

            ```input category_filter
            label: Category filter
            visual_mode: dropdown
            url_key: category
            options_query: category_filter_options
            expression_template: "category = ?"
            all_label: ALL
            all_expression: "TRUE"
            default: ALL
            ```

            ```sql category_filter_options
            SELECT DISTINCT
              category AS value
            FROM demo
            ORDER BY value;
            ```

            ```sql filtered_demo
            SELECT
              category,
              value
            FROM demo
            WHERE ${params.category_filter}
            ORDER BY category, value;
            ```

            ```echart filtered_chart
            data_query: filtered_demo
            type: bar
            x: category
            y: value
            title: "Filtered Values by Category"
            ```

            ```grid cols=12 gap=md
            | filtered_chart:8 | filtered_demo:4 |
            ```
            """
        ).strip()
        + "\n"
    )
    write_if_missing(filters_md, content)


def _init_cross_filters_page(root: Path) -> None:
    """
    Create a cross-filters demo page at:

      /content/cross_filters/index.md
    """
    cf_md = root / "content" / "cross_filters" / "index.md"
    content = (
        textwrap.dedent(
            """
            # Cross Filters Demo

            ```input cf_category
            label: Category
            visual_mode: dropdown
            url_key: cf_category
            options_query: cf_category_options
            expression_template: "category = ?"
            all_label: ALL
            all_expression: "TRUE"
            default: ALL
            ```

            ```input cf_value_min
            label: Minimum value
            visual_mode: dropdown
            url_key: cf_min
            options_query: cf_value_options
            expression_template: "value >= ?"
            all_label: ALL
            all_expression: "TRUE"
            default: ALL
            ```

            ```sql cf_category_options
            SELECT DISTINCT
              category AS value,
              category AS label
            FROM demo
            WHERE ${params.cf_value_min}
            ORDER BY value;
            ```

            ```sql cf_value_options
            SELECT DISTINCT
              value AS value,
              CAST(value AS VARCHAR) AS label
            FROM demo
            WHERE ${params.cf_category}
            ORDER BY value;
            ```

            ```sql cf_filtered_rows
            SELECT
              category,
              value
            FROM demo
            WHERE ${params.cf_category}
              AND ${params.cf_value_min}
            ORDER BY category, value;
            ```

            ```echart cf_chart
            data_query: cf_filtered_rows
            type: bar
            x: category
            y: value
            title: "Cross-filtered values by category"
            ```

            ```grid cols=12 gap=md
            | cf_chart:8 | cf_filtered_rows:4 |
            ```
            """
        ).strip()
        + "\n"
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
    content = (
        textwrap.dedent(
            """
            # Derived Templated Demo (text input → templated Parquet view)

            This page shows how a **text input** can choose *which compiled
            templated Parquet view* to use at runtime.

            ## Input definition (barcode → prefix)

            ```input barcode
            label: Barcode (first char = category)
            visual_mode: text
            url_key: barcode
            placeholder: "Scan or type a code like A1234567890"
            # Derived SQL+ID parameter:
            #   params.barcode_prefix  = left('<value>', 1)
            #   inputs.barcode_prefix  = '<value>'[0]
            param_name: barcode_prefix
            param_template: "left(?, 1)"
            ```

            If you type `A1234567890`:

            - `inputs.barcode        = "A1234567890"`
            - `params.barcode_prefix = "left('A1234567890', 1)"`
            - `inputs.barcode_prefix = "A"`

            and Ducksite can use:

            - `${params.barcode_prefix}` inside SQL, and
            - `${inputs.barcode_prefix}` to choose the SQL file name.

            ## Templated view selection via data_query

            The file-source template:

            ```text
            [[file_sources]]
            name = "demo"
            template_name = "demo_[category]"
            pattern = "data/demo/*.parquet"
            upstream_glob = "${DIR_FAKE}/demo-*.parquet"
            union_mode = "union_all_by_name"
            row_filter_template = "category = ?"
            ```

            expands into global views:

            - `demo_A` → only `demo-A.parquet`
            - `demo_B` → only `demo-B.parquet`
            - `demo_C` → only `demo-C.parquet`

            compiled as:

            - `/sql/_global/demo_A.sql`
            - `/sql/_global/demo_B.sql`
            - `/sql/_global/demo_C.sql`

            The chart points directly at those by interpolating the derived
            prefix into the **query id**:

            ```echart barcode_templated_chart
            data_query: "global:demo_${inputs.barcode_prefix}"
            type: bar
            x: category
            y: value
            title: "Templated Parquet view: demo_${inputs.barcode_prefix}"
            ```

            So:

            - `A123…` → `global:demo_A` → `/sql/_global/demo_A.sql`
            - `B999…` → `global:demo_B` → `/sql/_global/demo_B.sql`

            ```grid cols=12 gap=md
            | barcode_templated_chart:12 |
            ```
            """
        ).strip()
        + "\n"
    )
    write_if_missing(df_md, content)


def _init_models_page(root: Path) -> None:
    """
    Create a models demo page at:

      /content/models/index.md
    """
    models_md = root / "content" / "models" / "index.md"
    content = (
        textwrap.dedent(
            """
            # Models Demo (sources_sql)

            ```input category_filter
            label: Category filter (models)
            visual_mode: dropdown
            url_key: category
            options_query: model_options
            expression_template: "category = ?"
            all_label: ALL
            all_expression: "TRUE"
            default: ALL
            ```

            ```sql model_options
            SELECT
              value,
              label
            FROM demo_chain_options;
            ```

            ```sql model_chain_page
            SELECT
              category,
              total_value
            FROM demo_chain_agg
            ORDER BY category;
            ```

            ```sql model_filtered_page
            SELECT
              category,
              value
            FROM demo_chain_filtered
            ORDER BY category, value;
            ```

            ```echart model_chain_chart
            data_query: model_chain_page
            type: bar
            x: category
            y: total_value
            title: "Chained model: demo_chain_agg"
            ```

            ```grid cols=12 gap=md
            | model_chain_chart:8 | model_filtered_page:4 |
            ```
            """
        ).strip()
        + "\n"
    )
    write_if_missing(models_md, content)


def _init_template_page(root: Path) -> None:
    """
    Create a template models demo page at:

      /content/template/index.md
    """
    tmpl_md = root / "content" / "template" / "index.md"
    content = (
        textwrap.dedent(
            """
            # Template Models Demo (file-source expansion + dropdown)

            ```input template_view
            label: Templated view
            visual_mode: dropdown
            url_key: template_view
            options_query: template_view_options
            default: demo
            ```

            ```sql template_view_options
            SELECT 'demo'      AS value, 'All categories (demo)' AS label
            UNION ALL
            SELECT 'demo_' || category AS value,
                   'Category ' || category AS label
            FROM (
              SELECT DISTINCT category
              FROM demo
            )
            ORDER BY value;
            ```

            ```sql template_demo_all
            SELECT
              category,
              value
            FROM demo
            ORDER BY category, value;
            ```

            ```echart template_chart_selected
            data_query: "global:${inputs.template_view}"
            type: bar
            x: category
            y: value
            title: "Templated view (global: ${inputs.template_view})"
            ```

            ```grid cols=12 gap=md
            | template_demo_all:6 | template_chart_selected:6 |
            ```
            """
        ).strip()
        + "\n"
    )
    write_if_missing(tmpl_md, content)


def _init_gallery_page(root: Path) -> None:
    """
    Create a chart gallery page at:

      /content/gallery/index.md

    This page showcases:
      - 50 existing bar/line charts (stress test)
      - Additional examples for other ECharts series types, including:
          scatter, effectScatter, pie, pictorialBar, heatmap, radar,
          boxplot, candlestick, funnel, gauge, sankey, graph
      - An "option_column" demo for tree/treemap/sunburst/etc. using
        full ECharts JSON supplied from SQL (no JSON in the DSL itself).
    """
    gallery_md = root / "content" / "gallery" / "index.md"
    content = (
        textwrap.dedent(
            """
            # Chart Gallery

            This page exercises the chart runtime in two ways:

            1. A **50-chart bar/line stress test** using a simple contract:
               single-series bar/line, `x` category and `y` numeric metric.
            2. A **series-type showcase** for many other ECharts types:
               scatter, effectScatter, pie, pictorialBar, heatmap, radar,
               boxplot, candlestick, funnel, gauge, sankey, graph, plus an
               example of the `option_column` mode to support *all* ECharts
               chart types via JSON coming from SQL (but never from the DSL).

            ---

            ## Base queries for the 50-chart stress test

            ```sql gallery_q1_totals
            SELECT
              category,
              SUM(value) AS total_value
            FROM demo
            GROUP BY category
            ORDER BY category;
            ```

            ```sql gallery_q2_average
            SELECT
              category,
              AVG(value) AS avg_value
            FROM demo
            GROUP BY category
            ORDER BY category;
            ```

            ```sql gallery_q3_hist
            SELECT
              value AS bucket,
              COUNT(*) AS row_count
            FROM demo
            GROUP BY value
            ORDER BY value;
            ```

            ```sql gallery_q4_cumulative
            SELECT
              category,
              SUM(value) AS total_value,
              SUM(SUM(value)) OVER (ORDER BY category) AS running_total
            FROM demo
            GROUP BY category
            ORDER BY category;
            ```

            ```sql gallery_q5_scaled
            SELECT
              category,
              value * 2 AS double_value
            FROM demo
            ORDER BY category, value;
            ```

            ## 50 chart variants (bar + line)

            Charts 1–10: totals (gallery_q1_totals, total_value)  
            Charts 11–20: averages (gallery_q2_average, avg_value)  
            Charts 21–30: histogram (gallery_q3_hist, row_count)  
            Charts 31–40: cumulative (gallery_q4_cumulative, running_total)  
            Charts 41–50: scaled values (gallery_q5_scaled, double_value)

            We alternate bar / line so both series types are exercised.

            ```echart gallery_01
            data_query: gallery_q1_totals
            type: bar
            x: category
            y: total_value
            title: "01 · Totals (bar)"
            ```

            ```echart gallery_02
            data_query: gallery_q1_totals
            type: line
            x: category
            y: total_value
            title: "02 · Totals (line)"
            ```

            ```echart gallery_03
            data_query: gallery_q1_totals
            type: bar
            x: category
            y: total_value
            title: "03 · Totals (bar)"
            ```

            ```echart gallery_04
            data_query: gallery_q1_totals
            type: line
            x: category
            y: total_value
            title: "04 · Totals (line)"
            ```

            ```echart gallery_05
            data_query: gallery_q1_totals
            type: bar
            x: category
            y: total_value
            title: "05 · Totals (bar)"
            ```

            ```echart gallery_06
            data_query: gallery_q1_totals
            type: line
            x: category
            y: total_value
            title: "06 · Totals (line)"
            ```

            ```echart gallery_07
            data_query: gallery_q1_totals
            type: bar
            x: category
            y: total_value
            title: "07 · Totals (bar)"
            ```

            ```echart gallery_08
            data_query: gallery_q1_totals
            type: line
            x: category
            y: total_value
            title: "08 · Totals (line)"
            ```

            ```echart gallery_09
            data_query: gallery_q1_totals
            type: bar
            x: category
            y: total_value
            title: "09 · Totals (bar)"
            ```

            ```echart gallery_10
            data_query: gallery_q1_totals
            type: line
            x: category
            y: total_value
            title: "10 · Totals (line)"
            ```

            ```echart gallery_11
            data_query: gallery_q2_average
            type: bar
            x: category
            y: avg_value
            title: "11 · Average (bar)"
            ```

            ```echart gallery_12
            data_query: gallery_q2_average
            type: line
            x: category
            y: avg_value
            title: "12 · Average (line)"
            ```

            ```echart gallery_13
            data_query: gallery_q2_average
            type: bar
            x: category
            y: avg_value
            title: "13 · Average (bar)"
            ```

            ```echart gallery_14
            data_query: gallery_q2_average
            type: line
            x: category
            y: avg_value
            title: "14 · Average (line)"
            ```

            ```echart gallery_15
            data_query: gallery_q2_average
            type: bar
            x: category
            y: avg_value
            title: "15 · Average (bar)"
            ```

            ```echart gallery_16
            data_query: gallery_q2_average
            type: line
            x: category
            y: avg_value
            title: "16 · Average (line)"
            ```

            ```echart gallery_17
            data_query: gallery_q2_average
            type: bar
            x: category
            y: avg_value
            title: "17 · Average (bar)"
            ```

            ```echart gallery_18
            data_query: gallery_q2_average
            type: line
            x: category
            y: avg_value
            title: "18 · Average (line)"
            ```

            ```echart gallery_19
            data_query: gallery_q2_average
            type: bar
            x: category
            y: avg_value
            title: "19 · Average (bar)"
            ```

            ```echart gallery_20
            data_query: gallery_q2_average
            type: line
            x: category
            y: avg_value
            title: "20 · Average (line)"
            ```

            ```echart gallery_21
            data_query: gallery_q3_hist
            type: bar
            x: bucket
            y: row_count
            title: "21 · Histogram (bar)"
            ```

            ```echart gallery_22
            data_query: gallery_q3_hist
            type: line
            x: bucket
            y: row_count
            title: "22 · Histogram (line)"
            ```

            ```echart gallery_23
            data_query: gallery_q3_hist
            type: bar
            x: bucket
            y: row_count
            title: "23 · Histogram (bar)"
            ```

            ```echart gallery_24
            data_query: gallery_q3_hist
            type: line
            x: bucket
            y: row_count
            title: "24 · Histogram (line)"
            ```

            ```echart gallery_25
            data_query: gallery_q3_hist
            type: bar
            x: bucket
            y: row_count
            title: "25 · Histogram (bar)"
            ```

            ```echart gallery_26
            data_query: gallery_q3_hist
            type: line
            x: bucket
            y: row_count
            title: "26 · Histogram (line)"
            ```

            ```echart gallery_27
            data_query: gallery_q3_hist
            type: bar
            x: bucket
            y: row_count
            title: "27 · Histogram (bar)"
            ```

            ```echart gallery_28
            data_query: gallery_q3_hist
            type: line
            x: bucket
            y: row_count
            title: "28 · Histogram (line)"
            ```

            ```echart gallery_29
            data_query: gallery_q3_hist
            type: bar
            x: bucket
            y: row_count
            title: "29 · Histogram (bar)"
            ```

            ```echart gallery_30
            data_query: gallery_q3_hist
            type: line
            x: bucket
            y: row_count
            title: "30 · Histogram (line)"
            ```

            ```echart gallery_31
            data_query: gallery_q4_cumulative
            type: bar
            x: category
            y: running_total
            title: "31 · Cumulative (bar)"
            ```

            ```echart gallery_32
            data_query: gallery_q4_cumulative
            type: line
            x: category
            y: running_total
            title: "32 · Cumulative (line)"
            ```

            ```echart gallery_33
            data_query: gallery_q4_cumulative
            type: bar
            x: category
            y: running_total
            title: "33 · Cumulative (bar)"
            ```

            ```echart gallery_34
            data_query: gallery_q4_cumulative
            type: line
            x: category
            y: running_total
            title: "34 · Cumulative (line)"
            ```

            ```echart gallery_35
            data_query: gallery_q4_cumulative
            type: bar
            x: category
            y: running_total
            title: "35 · Cumulative (bar)"
            ```

            ```echart gallery_36
            data_query: gallery_q4_cumulative
            type: line
            x: category
            y: running_total
            title: "36 · Cumulative (line)"
            ```

            ```echart gallery_37
            data_query: gallery_q4_cumulative
            type: bar
            x: category
            y: running_total
            title: "37 · Cumulative (bar)"
            ```

            ```echart gallery_38
            data_query: gallery_q4_cumulative
            type: line
            x: category
            y: running_total
            title: "38 · Cumulative (line)"
            ```

            ```echart gallery_39
            data_query: gallery_q4_cumulative
            type: bar
            x: category
            y: running_total
            title: "39 · Cumulative (bar)"
            ```

            ```echart gallery_40
            data_query: gallery_q4_cumulative
            type: line
            x: category
            y: running_total
            title: "40 · Cumulative (line)"
            ```

            ```echart gallery_41
            data_query: gallery_q5_scaled
            type: bar
            x: category
            y: double_value
            title: "41 · Scaled (bar)"
            ```

            ```echart gallery_42
            data_query: gallery_q5_scaled
            type: line
            x: category
            y: double_value
            title: "42 · Scaled (line)"
            ```

            ```echart gallery_43
            data_query: gallery_q5_scaled
            type: bar
            x: category
            y: double_value
            title: "43 · Scaled (bar)"
            ```

            ```echart gallery_44
            data_query: gallery_q5_scaled
            type: line
            x: category
            y: double_value
            title: "44 · Scaled (line)"
            ```

            ```echart gallery_45
            data_query: gallery_q5_scaled
            type: bar
            x: category
            y: double_value
            title: "45 · Scaled (bar)"
            ```

            ```echart gallery_46
            data_query: gallery_q5_scaled
            type: line
            x: category
            y: double_value
            title: "46 · Scaled (line)"
            ```

            ```echart gallery_47
            data_query: gallery_q5_scaled
            type: bar
            x: category
            y: double_value
            title: "47 · Scaled (bar)"
            ```

            ```echart gallery_48
            data_query: gallery_q5_scaled
            type: line
            x: category
            y: double_value
            title: "48 · Scaled (line)"
            ```

            ```echart gallery_49
            data_query: gallery_q5_scaled
            type: bar
            x: category
            y: double_value
            title: "49 · Scaled (bar)"
            ```

            ```echart gallery_50
            data_query: gallery_q5_scaled
            type: line
            x: category
            y: double_value
            title: "50 · Scaled (line)"
            ```

            ## Layout (10 rows × 5 charts = 50)

            ```grid cols=12 gap=md
            | gallery_01:2 | gallery_02:2 | gallery_03:2 | gallery_04:2 | gallery_05:2 |
            | gallery_06:2 | gallery_07:2 | gallery_08:2 | gallery_09:2 | gallery_10:2 |
            | gallery_11:2 | gallery_12:2 | gallery_13:2 | gallery_14:2 | gallery_15:2 |
            | gallery_16:2 | gallery_17:2 | gallery_18:2 | gallery_19:2 | gallery_20:2 |
            | gallery_21:2 | gallery_22:2 | gallery_23:2 | gallery_24:2 | gallery_25:2 |
            | gallery_26:2 | gallery_27:2 | gallery_28:2 | gallery_29:2 | gallery_30:2 |
            | gallery_31:2 | gallery_32:2 | gallery_33:2 | gallery_34:2 | gallery_35:2 |
            | gallery_36:2 | gallery_37:2 | gallery_38:2 | gallery_39:2 | gallery_40:2 |
            | gallery_41:2 | gallery_42:2 | gallery_43:2 | gallery_44:2 | gallery_45:2 |
            | gallery_46:2 | gallery_47:2 | gallery_48:2 | gallery_49:2 | gallery_50:2 |
            ```

            ---

            ## Series-type showcase (simple DSL, no JSON in markdown)

            The following examples exercise *other* ECharts series types using
            only the YAML-like DSL keys inside ```echart blocks. No JSON
            appears in the DSL; the runtime builds options automatically.

            ### Scatter / effectScatter

            ```sql gallery_scatter_points
            SELECT
              category || CAST(value AS VARCHAR) AS id,
              value * 1.0 AS x,
              value * 1.5 AS y
            FROM demo
            ORDER BY id;
            ```

            ```echart gallery_scatter
            data_query: gallery_scatter_points
            type: scatter
            x: x
            y: y
            title: "Scatter: x vs y"
            ```

            ```echart gallery_effect_scatter
            data_query: gallery_scatter_points
            type: effectScatter
            x: x
            y: y
            title: "Effect scatter: x vs y"
            ```

            ### Pie / doughnut (pie with inner radius)

            ```sql gallery_pie
            SELECT
              category,
              SUM(value) AS total_value
            FROM demo
            GROUP BY category
            ORDER BY category;
            ```

            ```echart gallery_pie_simple
            data_query: gallery_pie
            type: pie
            name: category
            value: total_value
            title: "Pie: share by category"
            ```

            ```echart gallery_pie_donut
            data_query: gallery_pie
            type: pie
            name: category
            value: total_value
            inner_radius: 40%
            outer_radius: 70%
            title: "Doughnut: share by category"
            ```

            ### Pictorial bar

            ```echart gallery_pictorial_bar
            data_query: gallery_pie
            type: pictorialBar
            x: category
            y: total_value
            symbol: roundRect
            title: "Pictorial bar: blocks per category"
            ```

            ### Heatmap

            ```sql gallery_heatmap
            SELECT
              category AS x,
              value     AS y,
              value * 2 AS z
            FROM demo
            ORDER BY category, value;
            ```

            ```echart gallery_heatmap_chart
            data_query: gallery_heatmap
            type: heatmap
            x: x
            y: y
            value: z
            title: "Heatmap: category vs value"
            ```

            ### Radar

            ```sql gallery_radar
            SELECT
              category      AS indicator,
              AVG(value)    AS avg_value
            FROM demo
            GROUP BY category
            ORDER BY category;
            ```

            ```echart gallery_radar_chart
            data_query: gallery_radar
            type: radar
            indicator: indicator
            value: avg_value
            title: "Radar: average by category"
            series_name: Demo
            ```

            ### Boxplot

            ```sql gallery_box_data
            SELECT
              category AS name,
              MIN(value) AS low,
              quantile_cont(value, 0.25) AS q1,
              quantile_cont(value, 0.50) AS median,
              quantile_cont(value, 0.75) AS q3,
              MAX(value) AS high
            FROM demo
            GROUP BY category
            ORDER BY category;
            ```

            ```echart gallery_boxplot_chart
            data_query: gallery_box_data
            type: boxplot
            name: name
            low: low
            q1: q1
            median: median
            q3: q3
            high: high
            title: "Boxplot: value distribution by category"
            ```

            ### Candlestick

            ```sql gallery_candlestick_data
            SELECT
              category AS name,
              MIN(value) AS open,
              MAX(value) AS close,
              MIN(value) AS low,
              MAX(value) AS high
            FROM demo
            GROUP BY category
            ORDER BY category;
            ```

            ```echart gallery_candlestick_chart
            data_query: gallery_candlestick_data
            type: candlestick
            name: name
            open: open
            close: close
            low: low
            high: high
            title: "Candlestick: min/max by category"
            ```

            ### Funnel

            ```sql gallery_funnel_data
            SELECT
              category AS stage,
              SUM(value) AS total_value
            FROM demo
            GROUP BY category
            ORDER BY total_value DESC;
            ```

            ```echart gallery_funnel_chart
            data_query: gallery_funnel_data
            type: funnel
            name: stage
            value: total_value
            title: "Funnel: totals by category"
            ```

            ### Gauge

            ```sql gallery_gauge_value
            SELECT
              AVG(value) AS avg_value
            FROM demo;
            ```

            ```echart gallery_gauge_chart
            data_query: gallery_gauge_value
            type: gauge
            value: avg_value
            name: "Average"
            title: "Gauge: average demo value"
            ```

            ### Sankey

            ```sql gallery_sankey_edges
            SELECT 'A' AS src, 'X' AS dst, 5 AS weight
            UNION ALL SELECT 'A', 'Y', 3
            UNION ALL SELECT 'B', 'X', 4
            UNION ALL SELECT 'B', 'Z', 2;
            ```

            ```echart gallery_sankey_chart
            data_query: gallery_sankey_edges
            type: sankey
            source: src
            target: dst
            value: weight
            title: "Sankey: simple source→target flows"
            ```

            ### Graph

            ```sql gallery_graph_edges
            SELECT 'A' AS src, 'B' AS dst, 1 AS weight
            UNION ALL SELECT 'B', 'C', 1
            UNION ALL SELECT 'C', 'A', 1
            UNION ALL SELECT 'A', 'D', 1;
            ```

            ```echart gallery_graph_force
            data_query: gallery_graph_edges
            type: graph
            source: src
            target: dst
            value: weight
            layout: force
            title: "Graph: force layout"
            ```

            ```echart gallery_graph_circle
            data_query: gallery_graph_edges
            type: graph
            source: src
            target: dst
            value: weight
            layout: circular
            title: "Graph: circular layout"
            ```

            ```grid cols=12 gap=md
            | gallery_scatter:4 | gallery_effect_scatter:4 | gallery_pie_simple:4 |
            | gallery_pie_donut:4 | gallery_pictorial_bar:4 | gallery_heatmap_chart:4 |
            | gallery_radar_chart:4 | gallery_boxplot_chart:4 | gallery_candlestick_chart:4 |
            | gallery_funnel_chart:4 | gallery_gauge_chart:4 | gallery_sankey_chart:4 |
            | gallery_graph_force:6 | gallery_graph_circle:6 | .:0 |
            ```

            ---

            ## Advanced: full ECharts options via `option_column`

            For exotic series types (tree, treemap, sunburst, parallel, themeRiver,
            lines, custom, map, etc.), you can supply a full ECharts option JSON
            from SQL using a VARCHAR/TEXT column and `option_column` in the DSL.

            **The DSL stays JSON-free.** Only the SQL query builds JSON.

            ```sql gallery_treemap_option
            SELECT
              '{ "series": [ { "type": "treemap", "data": [
                  { "name": "Group A", "value": 6 },
                  { "name": "Group B", "value": 4 },
                  { "name": "Group C", "value": 2 }
                ] } ] }'::VARCHAR AS option_json;
            ```

            ```echart gallery_treemap_advanced
            data_query: gallery_treemap_option
            type: treemap
            option_column: option_json
            title: "Treemap (option_column demo)"
            ```

            ```grid cols=12 gap=md
            | gallery_treemap_advanced:6 |
            ```
            """
        ).strip()
        + "\n"
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
    _init_gallery_page(root)
