/***CONFIG
DATA_ROOT: InjectedPathStr
DEFAULT_WINDOW: int
DEFAULT_REGION: str
***/
/***PARAMS
Region:
  type: Optional[Literal['north','south','west','east']]
  scope: data
  applies_to:
    cte: base
    mode: inline
DayWindow:
  type: Optional[int]
  scope: data
***/
/***LITERAL_SOURCES
- id: regions
  from_cte: seeded
  value_column: region
***/
WITH seeded AS MATERIALIZE_CLOSED (
  SELECT * FROM (VALUES
    ('north', 1, 12), ('north', 2, 9), ('north', 3, 8),
    ('south', 1, 22), ('south', 2, 18), ('south', 3, 11),
    ('west', 1, 17), ('west', 2, 15), ('west', 3, 14),
    ('east', 1, 7), ('east', 2, 9), ('east', 3, 6)
  ) AS t(region, day, latency_ms)
),
base AS MATERIALIZE (
  SELECT
    region,
    day,
    latency_ms,
    avg(latency_ms) OVER (
      PARTITION BY region
      ORDER BY day
      ROWS BETWEEN COALESCE({{param DayWindow}}, {{config DEFAULT_WINDOW}}) PRECEDING AND CURRENT ROW
    ) AS rolling_ms
  FROM seeded
  WHERE {{param Region}} IS NULL OR region = {{param Region}}
),
ranked AS (
  SELECT
    region,
    day,
    latency_ms,
    rolling_ms,
    row_number() OVER (PARTITION BY region ORDER BY latency_ms DESC) AS rank_in_region
  FROM base
)
SELECT
  region,
  day,
  latency_ms,
  rolling_ms,
  rank_in_region,
  '{{config DATA_ROOT}}' AS demo_root_hint
FROM ranked
ORDER BY region, day;
