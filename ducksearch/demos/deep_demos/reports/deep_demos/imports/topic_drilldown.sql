/***CONFIG
DEFAULT_REGION: str
***/
/***PARAMS
Topic:
  type: Optional[Literal['routing','rendering','ingest']]
  scope: data
FocusVariant:
  type: Optional[Literal['alpha','beta']]
  scope: data
***/
/***IMPORTS
- id: stories
  report: deep_demos/imports/shared_base.sql
  pass_params:
    - Topic
***/
WITH base AS MATERIALIZE_CLOSED (
  SELECT * FROM parquet_scan('{{import stories}}')
),
ranked AS MATERIALIZE (
  SELECT
    topic,
    variant,
    ms,
    cost,
    row_number() OVER (PARTITION BY topic ORDER BY ms DESC) AS rank_in_topic
  FROM base
  WHERE {{param FocusVariant}} IS NULL OR variant = {{param FocusVariant}}
),
fanout AS (
  SELECT
    topic,
    variant,
    ms,
    cost,
    rank_in_topic,
    '{{config DEFAULT_REGION}}' AS default_region
  FROM ranked
)
SELECT * FROM fanout ORDER BY topic, rank_in_topic;
