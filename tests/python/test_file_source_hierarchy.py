import duckdb

from ducksite.config import FileSourceConfig, FileSourceHierarchy, ProjectConfig
from ducksite.queries import build_file_source_queries


def test_hierarchy_base_query_uses_all_levels(tmp_path):
    site_root = tmp_path / "static"
    day = site_root / "data" / "hier" / "day"
    month = site_root / "data" / "hier" / "month"
    year = site_root / "data" / "hier" / "year"
    for p in [day, month, year]:
        p.mkdir(parents=True)
    (day / "d.parquet").write_text("", encoding="utf-8")
    (month / "m.parquet").write_text("", encoding="utf-8")
    (year / "y.parquet").write_text("", encoding="utf-8")

    cfg = ProjectConfig(
        root=tmp_path,
        dirs={},
        file_sources=[
            FileSourceConfig(
                name="hier",
                hierarchy=[
                    FileSourceHierarchy(
                        pattern="data/hier/day/*.parquet", row_filter="period = 'day'"
                    ),
                    FileSourceHierarchy(
                        pattern="data/hier/month/*.parquet", row_filter="period = 'month'"
                    ),
                    FileSourceHierarchy(
                        pattern="data/hier/year/*.parquet", row_filter="period = 'year'"
                    ),
                ],
            )
        ],
    )
    cfg.site_root = site_root

    queries = build_file_source_queries(cfg)

    sql = queries["hier"].sql
    assert "data/hier/day/d.parquet" in sql
    assert "period = 'day'" in sql
    assert "data/hier/month/m.parquet" in sql
    assert "period = 'month'" in sql
    assert "data/hier/year/y.parquet" in sql
    assert "period = 'year'" in sql


def test_hierarchy_defaults_to_pattern_when_missing_hierarchy(tmp_path):
    site_root = tmp_path / "static"
    data_dir = site_root / "data" / "plain"
    data_dir.mkdir(parents=True)
    (data_dir / "one.parquet").write_text("", encoding="utf-8")

    cfg = ProjectConfig(
        root=tmp_path,
        dirs={},
        file_sources=[
            FileSourceConfig(
                name="plain",
                pattern="data/plain/*.parquet",
                row_filter="flag = true",
            )
        ],
    )
    cfg.site_root = site_root

    queries = build_file_source_queries(cfg)

    sql = queries["plain"].sql
    assert "data/plain/one.parquet" in sql
    assert "flag = true" in sql


def test_hierarchy_templated_query_combines_level_filters(tmp_path):
    site_root = tmp_path / "static"
    day = site_root / "data" / "hier" / "day"
    month = site_root / "data" / "hier" / "month"
    for p in [day, month]:
        p.mkdir(parents=True)

    con = duckdb.connect()
    try:
        con.execute(
            """
            COPY (SELECT 'recent'::VARCHAR AS category, 'day'::VARCHAR AS period)
            TO ? (FORMAT 'parquet');
            """,
            [str(day / "d.parquet")],
        )
        con.execute(
            """
            COPY (SELECT 'older'::VARCHAR AS category, 'month'::VARCHAR AS period)
            TO ? (FORMAT 'parquet');
            """,
            [str(month / "m.parquet")],
        )
    finally:
        con.close()

    cfg = ProjectConfig(
        root=tmp_path,
        dirs={},
        file_sources=[
            FileSourceConfig(
                name="hier",
                template_name="hier_[category]",
                hierarchy=[
                    FileSourceHierarchy(
                        pattern="data/hier/day/*.parquet", row_filter="period = 'day'"
                    ),
                    FileSourceHierarchy(
                        pattern="data/hier/month/*.parquet", row_filter="period = 'month'"
                    ),
                ],
            )
        ],
    )
    cfg.site_root = site_root

    queries = build_file_source_queries(cfg)

    assert "hier_recent" in queries
    assert "hier_older" in queries
    recent_sql = queries["hier_recent"].sql
    older_sql = queries["hier_older"].sql

    assert "category = 'recent'" in recent_sql
    assert "period = 'day'" in recent_sql
    assert "category = 'older'" in older_sql
    assert "period = 'month'" in older_sql


def test_hierarchy_templated_query_merges_base_and_template_filters(tmp_path):
    site_root = tmp_path / "static"
    day = site_root / "data" / "hier" / "day"
    day.mkdir(parents=True)

    con = duckdb.connect()
    try:
        con.execute(
            """
            COPY (
                SELECT 'vip'::VARCHAR AS category,
                       'day'::VARCHAR AS period,
                       TRUE AS active
            ) TO ? (FORMAT 'parquet');
            """,
            [str(day / "only.parquet")],
        )
    finally:
        con.close()

    cfg = ProjectConfig(
        root=tmp_path,
        dirs={},
        file_sources=[
            FileSourceConfig(
                name="hier",
                template_name="hier_[category]",
                row_filter="active = TRUE",
                row_filter_template="category = ? AND premium = TRUE",
                hierarchy=[
                    FileSourceHierarchy(
                        pattern="data/hier/day/*.parquet", row_filter="period = 'day'"
                    ),
                ],
            )
        ],
    )
    cfg.site_root = site_root

    queries = build_file_source_queries(cfg)

    vip_sql = queries["hier_vip"].sql
    assert "active = TRUE" in vip_sql
    assert "period = 'day'" in vip_sql
    assert "category = 'vip'" in vip_sql
    assert "premium = TRUE" in vip_sql


def test_template_values_materialise_views_without_sample_rows(tmp_path):
    site_root = tmp_path / "static"
    day = site_root / "data" / "orders" / "day"
    day.mkdir(parents=True)

    con = duckdb.connect()
    try:
        con.execute(
            """
            COPY (
                SELECT 'north-america'::VARCHAR AS region,
                       DATE '2024-01-01' AS order_date
                WHERE 1 = 0
            ) TO ? (FORMAT 'parquet');
            """,
            [str(day / "empty.parquet")],
        )
    finally:
        con.close()

    cfg = ProjectConfig(
        root=tmp_path,
        dirs={},
        file_sources=[
            FileSourceConfig(
                name="orders",
                template_name=(
                    "orders_[concat(region, '_', strftime(order_date, '%Y-%m-%d'))]"
                ),
                template_values=["north-america_2024-12-05"],
                hierarchy=[
                    FileSourceHierarchy(pattern="data/orders/day/*.parquet"),
                ],
            )
        ],
    )
    cfg.site_root = site_root

    queries = build_file_source_queries(cfg)

    assert "orders_north_america_2024_12_05" in queries
    templated_sql = queries["orders_north_america_2024_12_05"].sql
    assert "concat(region, '_', strftime(order_date, '%Y-%m-%d')) = 'north-america_2024-12-05'" in templated_sql
    assert "data/orders/day/empty.parquet" in templated_sql


def test_template_values_sql_allows_multi_column_seeds(tmp_path):
    site_root = tmp_path / "static"
    day = site_root / "data" / "orders" / "day"
    day.mkdir(parents=True)

    con = duckdb.connect()
    try:
        con.execute(
            """
            COPY (
                SELECT 'na'::VARCHAR AS region,
                       DATE '2024-12-01' AS order_date
                WHERE 1 = 0
            ) TO ? (FORMAT 'parquet');
            """,
            [str(day / "empty.parquet")],
        )
    finally:
        con.close()

    cfg = ProjectConfig(
        root=tmp_path,
        dirs={},
        file_sources=[
            FileSourceConfig(
                name="orders",
                template_name=(
                    "orders_[concat(region, '_', strftime(order_date, '%Y-%m-%d'))]"
                ),
                row_filter_template="region = ? AND order_date = ?",
                template_values_sql="SELECT 'na' AS region, DATE '2024-12-07' AS order_date",
                hierarchy=[
                    FileSourceHierarchy(pattern="data/orders/day/*.parquet"),
                ],
            )
        ],
    )
    cfg.site_root = site_root

    queries = build_file_source_queries(cfg)

    templated_queries = [
        q.sql
        for name, q in queries.items()
        if name.startswith("orders_") and "base" not in name
    ]
    assert any("region = 'na'" in sql and "order_date = '2024-12-07'" in sql for sql in templated_queries)


def test_hierarchy_endpoints_are_included_and_filtered(tmp_path):
    site_root = tmp_path / "static"
    early = site_root / "data" / "hier" / "early_day"
    mid = site_root / "data" / "hier" / "month"
    late = site_root / "data" / "hier" / "late_day"
    for p in [early, mid, late]:
        p.mkdir(parents=True)

    cfg = ProjectConfig(
        root=tmp_path,
        dirs={},
        file_sources=[
            FileSourceConfig(
                name="hier",
                row_filter="active = TRUE",
                hierarchy_before=[
                    FileSourceHierarchy(
                        pattern="data/hier/early_day/*.parquet", row_filter="period = 'early'"
                    )
                ],
                hierarchy=[
                    FileSourceHierarchy(
                        pattern="data/hier/month/*.parquet", row_filter="period = 'month'"
                    )
                ],
                hierarchy_after=[
                    FileSourceHierarchy(
                        pattern="data/hier/late_day/*.parquet", row_filter="period = 'late'"
                    )
                ],
            )
        ],
    )
    cfg.site_root = site_root

    (early / "a.parquet").write_text("", encoding="utf-8")
    (mid / "m.parquet").write_text("", encoding="utf-8")
    (late / "z.parquet").write_text("", encoding="utf-8")

    queries = build_file_source_queries(cfg)
    sql = queries["hier"].sql

    assert "data/hier/early_day/a.parquet" in sql
    assert "period = 'early'" in sql
    assert "data/hier/month/m.parquet" in sql
    assert "period = 'month'" in sql
    assert "data/hier/late_day/z.parquet" in sql
    assert "period = 'late'" in sql
    assert sql.count("active = TRUE") == 3


def test_hierarchy_endpoints_apply_to_templated_views(tmp_path):
    site_root = tmp_path / "static"
    early = site_root / "data" / "hier" / "early_day"
    mid = site_root / "data" / "hier" / "month"
    late = site_root / "data" / "hier" / "late_day"
    for p in [early, mid, late]:
        p.mkdir(parents=True)

    con = duckdb.connect()
    try:
        con.execute(
            """
            COPY (
                SELECT 'edge'::VARCHAR AS category,
                       'early'::VARCHAR AS period,
                       TRUE AS active
            )
            TO ? (FORMAT 'parquet');
            """,
            [str(early / "edge1.parquet")],
        )
        con.execute(
            """
            COPY (
                SELECT 'middle'::VARCHAR AS category,
                       'month'::VARCHAR AS period,
                       TRUE AS active
            )
            TO ? (FORMAT 'parquet');
            """,
            [str(mid / "middle.parquet")],
        )
        con.execute(
            """
            COPY (
                SELECT 'edge'::VARCHAR AS category,
                       'late'::VARCHAR AS period,
                       TRUE AS active
            )
            TO ? (FORMAT 'parquet');
            """,
            [str(late / "edge2.parquet")],
        )
    finally:
        con.close()

    cfg = ProjectConfig(
        root=tmp_path,
        dirs={},
        file_sources=[
            FileSourceConfig(
                name="hier",
                template_name="hier_[category]",
                row_filter="active = TRUE",
                hierarchy_before=[
                    FileSourceHierarchy(
                        pattern="data/hier/early_day/*.parquet", row_filter="period = 'early'"
                    )
                ],
                hierarchy=[
                    FileSourceHierarchy(
                        pattern="data/hier/month/*.parquet", row_filter="period = 'month'"
                    )
                ],
                hierarchy_after=[
                    FileSourceHierarchy(
                        pattern="data/hier/late_day/*.parquet", row_filter="period = 'late'"
                    )
                ],
            )
        ],
    )
    cfg.site_root = site_root

    queries = build_file_source_queries(cfg)
    assert "hier_edge" in queries
    assert "hier_middle" in queries

    edge_sql = queries["hier_edge"].sql
    assert "category = 'edge'" in edge_sql
    assert "period = 'early'" in edge_sql
    assert "period = 'late'" in edge_sql
    assert edge_sql.count("active = TRUE") >= 2

    middle_sql = queries["hier_middle"].sql
    assert "category = 'middle'" in middle_sql
    assert "period = 'month'" in middle_sql
    assert "active = TRUE" in middle_sql


def test_hierarchy_endpoints_template_values_seed_views(tmp_path):
    site_root = tmp_path / "static"
    early = site_root / "data" / "hier" / "early_day"
    mid = site_root / "data" / "hier" / "month"
    late = site_root / "data" / "hier" / "late_day"
    for p in [early, mid, late]:
        p.mkdir(parents=True)

    con = duckdb.connect()
    try:
        con.execute(
            """
            COPY (
              SELECT 'na'::VARCHAR AS region,
                     DATE '2024-12-05' AS max_day,
                     'edge-start'::VARCHAR AS period,
                     TRUE AS active
            ) TO ? (FORMAT 'parquet');
            """,
            [str(early / "edge.parquet")],
        )
        con.execute(
            """
            COPY (
              SELECT 'na'::VARCHAR AS region,
                     DATE '2024-12-05' AS max_day,
                     'month'::VARCHAR AS period,
                     TRUE AS active
            ) TO ? (FORMAT 'parquet');
            """,
            [str(mid / "mid.parquet")],
        )
        con.execute(
            """
            COPY (
              SELECT 'na'::VARCHAR AS region,
                     DATE '2024-12-05' AS max_day,
                     'edge-end'::VARCHAR AS period,
                     TRUE AS active
            ) TO ? (FORMAT 'parquet');
            """,
            [str(late / "late.parquet")],
        )
    finally:
        con.close()

    cfg = ProjectConfig(
        root=tmp_path,
        dirs={},
        file_sources=[
            FileSourceConfig(
                name="hier",
                template_name="hier_[concat(region, '_', strftime(max_day, '%Y-%m-%d'))]",
                row_filter="active = TRUE",
                row_filter_template="concat(region, '_', strftime(max_day, '%Y-%m-%d')) = ?",
                template_values=["na_2024-12-05"],
                hierarchy_before=[
                    FileSourceHierarchy(
                        pattern="data/hier/early_day/*.parquet", row_filter="period = 'edge-start'"
                    )
                ],
                hierarchy=[
                    FileSourceHierarchy(
                        pattern="data/hier/month/*.parquet", row_filter="period = 'month'"
                    )
                ],
                hierarchy_after=[
                    FileSourceHierarchy(
                        pattern="data/hier/late_day/*.parquet", row_filter="period = 'edge-end'"
                    )
                ],
            )
        ],
    )
    cfg.site_root = site_root

    queries = build_file_source_queries(cfg)
    seeded_key = "hier_na_2024_12_05"
    assert seeded_key in queries

    seeded_sql = queries[seeded_key].sql
    assert "period = 'edge-start'" in seeded_sql
    assert "period = 'edge-end'" in seeded_sql
    assert seeded_sql.count("active = TRUE") >= 3
    assert "concat(region, '_', strftime(max_day, '%Y-%m-%d')) = 'na_2024-12-05'" in seeded_sql
