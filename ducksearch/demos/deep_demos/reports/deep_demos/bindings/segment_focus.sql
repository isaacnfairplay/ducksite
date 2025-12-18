/***PARAMS
Segment:
  type: Literal['alpha','beta']
  scope: data
Shard:
  type: Optional[int]
  scope: hybrid
  applies_to:
    cte: ranked
    mode: inline
***/
/***BINDINGS
- id: segment_label
  source: segment_lookup
  key_param: Segment
  key_column: segment
  value_column: friendly
  kind: demo
***/
/***LITERAL_SOURCES
- id: segments
  from_cte: segment_lookup
  value_column: friendly
***/
WITH segment_lookup AS MATERIALIZE_CLOSED (
  SELECT * FROM (VALUES
    ('alpha', 'Fast lane'),
    ('beta', 'Wide scan')
  ) AS t(segment, friendly)
),
payload AS MATERIALIZE (
  SELECT
    segment_lookup.segment,
    segment_lookup.friendly,
    gs.shard,
    gs.shard * CASE WHEN segment_lookup.segment = 'alpha' THEN 5 ELSE 11 END AS docs
  FROM segment_lookup, range(1, 5) AS gs(shard)
),
ranked AS (
  SELECT
    segment,
    friendly,
    shard,
    docs,
    rank() OVER (PARTITION BY segment ORDER BY docs DESC) AS shard_rank
  FROM payload
  WHERE segment = {{param Segment}}
    AND ({{param Shard}} IS NULL OR shard = {{param Shard}})
)
SELECT
  segment,
  friendly,
  shard,
  docs,
  shard_rank,
  '{{bind segment_label}}' AS chosen_segment
FROM ranked
ORDER BY shard;
