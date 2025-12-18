/***PARAMS
Topic:
  type: Optional[Literal['routing','rendering','ingest']]
  scope: data
***/
/***LITERAL_SOURCES
- id: topics
  from_cte: stories
  value_column: topic
***/
WITH stories AS MATERIALIZE_CLOSED (
  SELECT * FROM (VALUES
    ('routing', 'alpha', 120),
    ('routing', 'beta', 90),
    ('rendering', 'alpha', 150),
    ('ingest', 'alpha', 80)
  ) AS t(topic, variant, ms)
),
scored AS MATERIALIZE (
  SELECT
    topic,
    variant,
    ms,
    ms / 10 AS cost,
    ms - lag(ms) OVER (PARTITION BY topic ORDER BY ms DESC) AS drop_ms
  FROM stories
  WHERE {{param Topic}} IS NULL OR topic = {{param Topic}}
)
SELECT * FROM scored ORDER BY topic, ms DESC;
