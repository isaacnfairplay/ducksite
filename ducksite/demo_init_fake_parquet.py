from __future__ import annotations
from pathlib import Path
import ssl
from urllib.request import urlopen

import duckdb

from .utils import ensure_dir

# Public NYC TLC yellow taxi Parquet file.
# Example monthly file (about ~50 MB) – January 2023.
# Source: NYC TLC trip record data (PARQUET) via CloudFront. :contentReference[oaicite:0]{index=0}
NYTAXI_SOURCE_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
)


def _download_nytaxi_parquet(dest: Path) -> bool:
    """
    Try to download a real NYC yellow taxi Parquet file to `dest`.

    Returns True on success, False on any failure (network, SSL, etc).
    """
    try:
        print(f"[ducksite:init] downloading NYTaxi dataset from {NYTAXI_SOURCE_URL}")
        ctx = ssl._create_unverified_context()
        with urlopen(NYTAXI_SOURCE_URL, context=ctx) as resp, dest.open("wb") as f:
            # Stream in chunks to avoid large memory spikes.
            while True:
                chunk = resp.read(1024 * 1024)
                if not chunk:
                    break
                f.write(chunk)
        print(f"[ducksite:init] downloaded NYTaxi parquet to {dest}")
        return True
    except Exception as e:  # pragma: no cover - best effort network path
        print(f"[ducksite:init] WARNING: failed to download NYTaxi parquet: {e}")
        return False


def _create_small_nytaxi_sample(dest: Path) -> None:
    """
    Fallback: create a tiny NYTaxi-like parquet file locally using DuckDB.

    Columns align with the public TLC drop so gallery queries work the same
    with the real file or this seed sample. Distances are stored in miles to
    match the `trip_distance` units used by NYC TLC.
    """
    con = duckdb.connect()
    try:
        con.execute(
            """
            COPY (
              SELECT *
              FROM (
                VALUES
                  ('Manhattan',  8,  1.6, 15.0,  2.0, 1, 1, TIMESTAMP '2023-01-01 08:05', TIMESTAMP '2023-01-01 08:25'),
                  ('Manhattan',  9,  1.1,  9.5,  1.5, 1, 2, TIMESTAMP '2023-01-01 09:10', TIMESTAMP '2023-01-01 09:30'),
                  ('Manhattan', 18,  4.2, 22.0,  3.0, 2, 1, TIMESTAMP '2023-01-01 18:45', TIMESTAMP '2023-01-01 19:15'),
                  ('Brooklyn',  18,  5.0, 25.0,  4.0, 2, 1, TIMESTAMP '2023-01-02 18:10', TIMESTAMP '2023-01-02 18:40'),
                  ('Brooklyn',  19,  3.8, 19.0,  2.0, 1, 2, TIMESTAMP '2023-01-02 19:15', TIMESTAMP '2023-01-02 19:40'),
                  ('Queens',     7, 12.0, 45.0,  6.0, 1, 1, TIMESTAMP '2023-01-03 07:30', TIMESTAMP '2023-01-03 08:00'),
                  ('Queens',     8,  3.2, 18.0,  2.0, 3, 2, TIMESTAMP '2023-01-03 08:10', TIMESTAMP '2023-01-03 08:35'),
                  ('Bronx',     22,  6.7, 28.0,  3.0, 2, 3, TIMESTAMP '2023-01-04 22:05', TIMESTAMP '2023-01-04 22:45'),
                  ('Bronx',     23,  2.1, 11.0,  0.0, 1, 1, TIMESTAMP '2023-01-04 23:40', TIMESTAMP '2023-01-05 00:05'),
                  ('Staten Island', 14, 10.0, 40.0,  5.0, 1, 4, TIMESTAMP '2023-01-05 14:20', TIMESTAMP '2023-01-05 14:55'),
                  ('Queens',    10,  2.4, 17.0,  1.0, 2, 1, TIMESTAMP '2023-01-06 10:05', TIMESTAMP '2023-01-06 10:30'),
                  ('Manhattan', 21,  8.0, 36.0,  4.5, 3, 2, TIMESTAMP '2023-01-06 21:10', TIMESTAMP '2023-01-06 21:50')
                ) AS t(borough, hour, trip_distance, total_amount, tip_amount, passenger_count, payment_type, tpep_pickup_datetime, tpep_dropoff_datetime)
            )
            TO ?
            (FORMAT 'parquet');
            """,
            [str(dest)],
        )
    finally:
        try:
            con.close()
        except Exception:
            pass
    print(f"[ducksite:init] wrote tiny NYTaxi sample parquet to {dest}")


def init_demo_fake_parquet(root: Path) -> None:
    """
    Create demo parquet files under fake_upstream/ if they do not already exist.

    Files created:

      fake_upstream/demo-A.parquet
      fake_upstream/demo-B.parquet
      fake_upstream/demo-C.parquet

        -> small category/value demo used by basic pages and file-source
           templating examples.

      fake_upstream/nytaxi-2023-01.parquet   (real NYC TLC slice, if download ok)
        OR
      fake_upstream/nytaxi-sample.parquet    (tiny synthetic fallback)

        -> NYTaxi-like data used by the chart gallery and more complex dashboards.
    """
    fake_dir = root / "fake_upstream"
    ensure_dir(fake_dir)

    # --- Small category/value demo split into three files ---
    a_path = fake_dir / "demo-A.parquet"
    b_path = fake_dir / "demo-B.parquet"
    c_path = fake_dir / "demo-C.parquet"

    # --- NYTaxi paths (real + fallback) ---
    nytaxi_real_path = fake_dir / "nytaxi-2023-01.parquet"
    nytaxi_sample_path = fake_dir / "nytaxi-sample.parquet"

    con = duckdb.connect()
    try:
        # Create the small demo split if missing.
        need_demo_split = not (a_path.exists() and b_path.exists() and c_path.exists())
        if need_demo_split:
            # Category A: two rows
            con.execute(
                """
                COPY (
                  SELECT 'A'::VARCHAR AS category, 10::INT AS value
                  UNION ALL SELECT 'A', 15
                )
                TO ?
                (FORMAT 'parquet');
                """,
                [str(a_path)],
            )

            # Category B: two rows
            con.execute(
                """
                COPY (
                  SELECT 'B'::VARCHAR AS category, 20::INT AS value
                  UNION ALL SELECT 'B', 30
                )
                TO ?
                (FORMAT 'parquet');
                """,
                [str(b_path)],
            )

            # Category C: one row
            con.execute(
                """
                COPY (
                  SELECT 'C'::VARCHAR AS category, 5::INT AS value
                )
                TO ?
                (FORMAT 'parquet');
                """,
                [str(c_path)],
            )

            print(f"[ducksite:init] wrote {a_path}")
            print(f"[ducksite:init] wrote {b_path}")
            print(f"[ducksite:init] wrote {c_path}")
        else:
            print(f"[ducksite:init] demo-A/B/C.parquet already exist, skipping.")

    finally:
        try:
            con.close()
        except Exception:
            pass

    # --- Hierarchy demo: day/month/year rollups ---
    hier_root = fake_dir / "demo_hierarchy"
    day_path = hier_root / "day" / "hier-day.parquet"
    month_path = hier_root / "month" / "hier-month.parquet"
    year_path = hier_root / "year" / "hier-year.parquet"

    ensure_dir(day_path.parent)
    ensure_dir(month_path.parent)
    ensure_dir(year_path.parent)

    if day_path.exists() and month_path.exists() and year_path.exists():
        print("[ducksite:init] hierarchy demo parquet already exist, skipping.")
    else:
        hier_con = duckdb.connect()
        try:
            hier_con.execute(
                """
                COPY (
                  SELECT 'recent'::VARCHAR AS category,
                         'day'::VARCHAR    AS period,
                         1::INT            AS value
                ) TO ? (FORMAT 'parquet');
                """,
                [str(day_path)],
            )
            hier_con.execute(
                """
                COPY (
                  SELECT 'older'::VARCHAR AS category,
                         'month'::VARCHAR AS period,
                         2::INT           AS value
                ) TO ? (FORMAT 'parquet');
                """,
                [str(month_path)],
            )
            hier_con.execute(
                """
                COPY (
                  SELECT 'archive'::VARCHAR AS category,
                         'year'::VARCHAR    AS period,
                         3::INT             AS value
                ) TO ? (FORMAT 'parquet');
                """,
                [str(year_path)],
            )
            print("[ducksite:init] wrote hierarchy demo parquet split across day/month/year")
        finally:
            hier_con.close()

    # --- Hierarchy endpoints demo: day/month/year with edge windows ---
    edge_root = fake_dir / "demo_hierarchy_window"
    edge_before = edge_root / "day_start" / "hier-edge-start.parquet"
    edge_day = edge_root / "day" / "hier-edge-day.parquet"
    edge_month = edge_root / "month" / "hier-edge-month.parquet"
    edge_year = edge_root / "year" / "hier-edge-year.parquet"
    edge_after = edge_root / "day_end" / "hier-edge-end.parquet"

    for p in [edge_before, edge_day, edge_month, edge_year, edge_after]:
        ensure_dir(p.parent)

    if all(p.exists() for p in [edge_before, edge_day, edge_month, edge_year, edge_after]):
        print("[ducksite:init] hierarchy endpoints demo parquet already exist, skipping.")
    else:
        edge_con = duckdb.connect()
        try:
            edge_con.execute(
                """
                COPY (
                  SELECT 'na'::VARCHAR AS region,
                         DATE '2024-12-05' AS max_day,
                         'edge-start'::VARCHAR AS period,
                         TRUE AS active,
                         5::INT AS value
                ) TO ? (FORMAT 'parquet');
                """,
                [str(edge_before)],
            )
            edge_con.execute(
                """
                COPY (
                  SELECT 'na'::VARCHAR AS region,
                         DATE '2024-12-05' AS max_day,
                         'day'::VARCHAR AS period,
                         TRUE AS active,
                         7::INT AS value
                ) TO ? (FORMAT 'parquet');
                """,
                [str(edge_day)],
            )
            edge_con.execute(
                """
                COPY (
                  SELECT 'na'::VARCHAR AS region,
                         DATE '2024-11-30' AS max_day,
                         'month'::VARCHAR AS period,
                         TRUE AS active,
                         11::INT AS value
                ) TO ? (FORMAT 'parquet');
                """,
                [str(edge_month)],
            )
            edge_con.execute(
                """
                COPY (
                  SELECT 'na'::VARCHAR AS region,
                         DATE '2023-12-31' AS max_day,
                         'year'::VARCHAR AS period,
                         TRUE AS active,
                         19::INT AS value
                ) TO ? (FORMAT 'parquet');
                """,
                [str(edge_year)],
            )
            edge_con.execute(
                """
                COPY (
                  SELECT 'na'::VARCHAR AS region,
                         DATE '2024-12-05' AS max_day,
                         'edge-end'::VARCHAR AS period,
                         TRUE AS active,
                         23::INT AS value
                ) TO ? (FORMAT 'parquet');
                """,
                [str(edge_after)],
            )
            print(
                "[ducksite:init] wrote hierarchy endpoints demo parquet with before/after day windows"
            )
        finally:
            edge_con.close()

    # --- NYTaxi: prefer real download, otherwise fallback to tiny sample ---

    if nytaxi_real_path.exists() or nytaxi_sample_path.exists():
        print(
            f"[ducksite:init] NYTaxi parquet already present "
            f"({nytaxi_real_path if nytaxi_real_path.exists() else nytaxi_sample_path}), skipping download."
        )
        return

    # Try real NYC TLC parquet (Jan 2023).
    if _download_nytaxi_parquet(nytaxi_real_path):
        return

    # Fallback: create small synthetic sample if download failed.
    _create_small_nytaxi_sample(nytaxi_sample_path)
