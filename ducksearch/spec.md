Below is the **complete, standalone Ducksearch specification**, rewritten **without referencing Codex or this chat**, and including:

* clear **packaging recommendations** (separate from ducksite)
* **per-CTE (upstream) parameter application**
* explicit DuckDB planner constraints
* client vs server filtering
* Parquet-first frontend with DuckDB WASM
* nested data rendering
* bindings, materialization, imports
* detailed, end-to-end SQL examples
* lint and lifecycle requirements

This document is intended to be pasted into your repo (e.g. `ducksearch/spec.md`) and treated as **authoritative**.

---

# Ducksearch

**Search & Inspection Service — Full Specification (v1.3)**

---

## 0. Purpose

Ducksearch is a **search and inspection web service** driven by SQL files.

It is designed for:

* fast lookup and filtering
* deterministic inspection of structured data
* nested data exploration
* reproducible, shareable URLs

Ducksearch is **not**:

* a BI dashboard engine
* a general SQL execution service
* a write-capable database interface

### Core mental model

> SQL defines **what data exists**
> URLs define **how it is viewed**
> Parquet is the **contract** between server and browser

---

## 1. Absolute invariants (MUST)

These rules override everything else.

1. **Parquet-first**

   * Every dataset (base results, slices, facets, charts, materializations) is written to Parquet before use.
   * The server never streams raw SQL results directly.

2. **Read-only SQL**
   Report SQL may not:

   * write files
   * attach databases
   * install or load extensions
   * create or modify tables
   * change global settings

3. **No SQL rewriting**

   * Ducksearch does not rewrite SQL text.
   * All indirection is explicit via placeholders.

4. **DuckDB planner compatibility**

   * Any file path passed to `parquet_scan()` **must be a bind-time string literal**.
   * SQL string concatenation (`||`) inside scan paths is forbidden.

5. **Secrets are never serialized**

   * Secrets never appear in:

     * SQL text
     * compiled SQL
     * Parquet files
     * cache keys
     * logs
     * error payloads

6. **Client-only parameters never reach data endpoints**

   * They must be stripped before any Parquet request.

7. **URLs are deterministic and shareable**

   * Opening the same URL yields the same logical result.

8. **System theme by default**

   * UI defaults to OS/browser `prefers-color-scheme`.

---

## 2. Packaging & distribution

Ducksearch is recommended to be **a separate package from ducksite**.

### Rationale

* Ducksearch focuses on **search/lookup**.
* Ducksite focuses on **analytics/dashboards**.
* Back-end logic (DSL parsing, caching, report lifecycle) should not be shared to avoid coupling.

### Recommended layout (separate package)

```
repo/
  ducksite/
    ...
  ducksearch/
    __init__.py
    cli.py
    server/
    compiler/
    runtime/
    cache/
    secrets/
    frontend_static/
  pyproject.toml
```

### Shared code policy

* **Do not share backend code** between ducksearch and ducksite.
* You MAY share a **small frontend DuckDB-WASM runtime** (plain ES modules) for:

  * DuckDB WASM boot
  * httpfs setup
  * client-side SQL execution over Parquet
* Rendering logic must remain separate.

---

## 3. CLI

Ducksearch is invoked via a CLI.

### CLI name

```bash
ducksearch
```

### Required subcommands (v1)

#### `ducksearch serve`

```bash
ducksearch serve --root /srv --host 0.0.0.0 --port 8080
```

Flags:

* `--root PATH` **(required)**: directory containing `config.toml`, `reports/`, `cache/`
* `--host HOST` (default `127.0.0.1`)
* `--port PORT` (default `8080`)
* `--workers N` (default `1`; v1 assumes single process)
* `--dev` (optional; disables HTML caching, verbose logging)

#### `ducksearch lint`

```bash
ducksearch lint --root /srv
```

Behavior:

* Validates all reports and metadata
* Exits non-zero on **any** violation
* Produces actionable error messages

---

## 4. Runtime root layout

Ducksearch expects the following structure under `--root`:

```
<root>/
  config.toml
  reports/
    <folder>/
      <report>.sql
      <folder>.js|css|html
      <prefix>.js|css|html
      charts/
        <prefix>.json
      filestore.yaml
  composites/
  cache/
    artifacts/
    slices/
    materialize/
    literal_sources/
    bindings/
    facets/
    charts/
    manifests/
    tmp/
```

---

## 5. Reports

A **report** is a single SQL file at:

```
reports/<folder>/<report>.sql
```

Rules:

* exactly **one SQL statement**
* optional metadata blocks
* no side effects

---

## 6. Metadata blocks

Metadata blocks are YAML embedded in SQL comments:

```sql
/***BLOCK_NAME
key: value
***/
```

Supported blocks:

* `PARAMS`
* `CONFIG`
* `SOURCES`
* `CACHE`
* `TABLE`
* `SEARCH`
* `FACETS`
* `CHARTS`
* `DERIVED_PARAMS`
* `LITERAL_SOURCES`
* `BINDINGS`
* `IMPORTS`
* `SECRETS`

Blocks may appear in any order.

---

## 7. Parameters

### 7.1 Case-insensitive keys

* URL keys match parameter names **case-insensitively**
* Canonical casing is defined in SQL
* Duplicate scalar params differing only by case → **400 error**

---

### 7.2 Parameter types

Supported:

* `Optional[T]`
* `List[T]`
* `Literal[...]`
* Primitive types: `int`, `float`, `bool`, `date`, `datetime`, `str`
* `InjectedStr`
  Escaped string literal only (never raw SQL)
* `InjectedIdentLiteral[...]`
  Allowlisted identifier token (build-time substitution)

---

### 7.3 Parameter scopes (CRITICAL)

Each parameter has a **scope**:

| Scope    | Meaning                     |
| -------- | --------------------------- |
| `data`   | Must be applied server-side |
| `view`   | Client-only                 |
| `hybrid` | Client or server            |

**Inference rules:**

* If referenced in base SQL → `data`
* Otherwise → `view`

Explicit `scope` overrides inference.

---

### 7.4 Per-CTE (upstream) parameter application

Parameters may declare **where** they apply using `applies_to`.

```yaml
applies_to:
  cte: <cte_name>
  mode: wrapper | inline
```

* `cte`: logical CTE name
* `mode: wrapper` means the SQL must define:

  * `<cte>_base`
  * `<cte>` as a filtered wrapper
* `mode: inline` means the filter is written directly in the CTE

This allows upstream filtering **without SQL rewriting**.

---

### Example: PARAMS with per-CTE application

```sql
/***PARAMS
LineName:
  type: Optional[Literal['SMT1','SMT2','SMT5']]
  scope: data
  applies_to:
    cte: acv_lines
    mode: wrapper

Sigma:
  type: Optional[Literal[1,2,3,4,5,6,7]]
  scope: data
  applies_to:
    cte: acv_lines
    mode: wrapper

ProgramSubstring:
  type: Optional[InjectedStr]
  scope: data
  applies_to:
    cte: part_agnostic
    mode: wrapper
***/
```

---

## 8. Client vs server filtering

### 8.1 URL namespaces

| Prefix                | Meaning        |
| --------------------- | -------------- |
| `Param=...`           | Server-applied |
| `__client__Param=...` | Client-only    |
| `__server__Param=...` | Force server   |
| `__force_server=1`    | Force all      |

Client-only params MUST NOT be sent to Parquet endpoints.

---

### 8.2 Client-side eligibility

A parameter may be applied client-side only if **all** are true:

1. Column exists in base schema
2. Filter does not affect:

   * scans
   * imports
   * bindings
   * materializations
3. No server `LIMIT` / top-N semantics are violated
4. Predicate is simple and bounded (equality, IN, range)

Otherwise → server applies.

---

## 9. SQL placeholders (ONLY these)

| Placeholder       | Meaning               |
| ----------------- | --------------------- |
| `{{param Name}}`  | Escaped value literal |
| `{{ident Name}}`  | Identifier token      |
| `{{path Name}}`   | Trusted path          |
| `{{bind Name}}`   | Binding output        |
| `{{mat Name}}`    | Materialized Parquet  |
| `{{import Name}}` | Imported report       |
| `{{config Name}}` | Config constant       |

No other interpolation is allowed.

---

## 10. DuckDB scan path rule (MANDATORY)

DuckDB requires scan paths to be **bind-time string literals**.

### VALID

```sql
SELECT *
FROM parquet_scan('{{config DATA_ROOT}}/orders/{{bind order_partition}}/*.parquet');
```

### INVALID

```sql
SELECT *
FROM parquet_scan('{{config DATA_ROOT}}/orders/' || {{bind order_partition}} || '/*.parquet');
```

Ducksearch MUST detect and reject invalid patterns during linting.

---

## 11. Materialization

Materializations are **explicit build targets**.

* Declared with `MATERIALIZE` or `MATERIALIZE_CLOSED`
* Stored as Parquet
* Referenced **only** via `{{mat name}}`
* No SQL rewriting

---

### Example: Materialized CTE with upstream filtering

```sql
-- reports/acv/last_ran.sql

/***CONFIG
DATA_ROOT: InjectedPathStr
***/

/***PARAMS
LineName:
  type: Optional[Literal['SMT1','SMT2','SMT5']]
  scope: data
  applies_to:
    cte: acv_lines
    mode: wrapper

Sigma:
  type: Optional[Literal[1,2,3,4,5,6,7]]
  scope: data
  applies_to:
    cte: acv_lines
    mode: wrapper
***/

/***TABLE
excel_controls: true
***/
WITH
part_agnostic_base AS (
  SELECT
    machine_id,
    program_name,
    MAX(timestamp)::DATE AS last_ran
  FROM parquet_scan('{{config DATA_ROOT}}/events/*.parquet')
  GROUP BY ALL
),

part_agnostic AS (
  SELECT *
  FROM part_agnostic_base
),

acv_lines_base AS (
  SELECT
    machine_id,
    line_name,
    TRY_CAST(split_part(machine_name, '-', -1) AS UTINYINT) AS sigma
  FROM parquet_scan('{{config DATA_ROOT}}/machines/*.parquet')
),

acv_lines AS (
  SELECT *
  FROM acv_lines_base
  WHERE TRUE
    AND ({{param LineName}} IS NULL OR line_name = {{param LineName}})
    AND ({{param Sigma}} IS NULL OR sigma = {{param Sigma}})
)

SELECT
  program_name,
  line_name,
  sigma,
  last_ran
FROM part_agnostic
JOIN acv_lines USING (machine_id)
ORDER BY last_ran DESC;
```

---

## 12. Dynamic literals & bindings

### 12.1 Dynamic literal source example

```sql
-- reports/orders/order_lookup.sql

/***CONFIG
DATA_ROOT: InjectedPathStr
***/

/***LITERAL_SOURCES
- id: order_ids
  from_cte: distinct_orders
  value_column: order_id
***/

WITH distinct_orders AS MATERIALIZE_CLOSED (
  SELECT DISTINCT order_id
  FROM parquet_scan('{{config DATA_ROOT}}/orders/*.parquet')
)

SELECT
  order_id,
  customer_name,
  order_date,
  total_amount
FROM parquet_scan('{{config DATA_ROOT}}/orders/*.parquet');
```

---

### 12.2 Binding example (partition identifiers)

```sql
-- reports/orders/order_partition_lookup.sql

/***CONFIG
DATA_ROOT: InjectedPathStr
***/

/***PARAMS
OrderID:
  type: Optional[int]
  scope: hybrid
***/

/***BINDINGS
- id: order_partition
  source: order_partitions
  key_param: OrderID
  key_column: order_id
  value_column: partition_id
  kind: partition
***/

WITH order_partitions AS MATERIALIZE_CLOSED (
  SELECT
    order_id,
    'order_id=' || CAST(order_id AS VARCHAR) AS partition_id
  FROM parquet_scan('{{config DATA_ROOT}}/orders/*.parquet')
  GROUP BY order_id
)

SELECT *
FROM parquet_scan(
  '{{config DATA_ROOT}}/orders/{{bind order_partition}}/*.parquet'
);
```

---

## 13. Imports

Reports may import other reports.

* Imports consume **base Parquet only**
* Import graph must be acyclic

---

### Example: Import

```sql
-- reports/orders/order_with_inventory.sql

/***IMPORTS
- id: inventory
  report: inventory/inventory_items
  pass_params: [Category]
***/

SELECT
  o.order_id,
  o.item_id,
  o.quantity,
  i.name AS item_name,
  i.category
FROM parquet_scan('{{config DATA_ROOT}}/orders/*.parquet') AS o
JOIN {{import inventory}} AS i
  USING (item_id);
```

---

## 14. TABLE UI & nested data

Enable with:

```sql
/***TABLE
excel_controls: true
nested:
  auto_expand: [line_items]
  max_expand_rows: 100
  max_depth: 3
***/
```

Rendering rules:

* `LIST<STRUCT>` → nested table with headers
* `LIST<scalar>` → nested table without header
* `STRUCT` → key/value mini-table

---

## 15. Assets & charts

Prefix-based matching:

* split report name on `_` and `-`
* cumulative prefixes only

Load order:

1. `<folder>.js|css|html`
2. `<prefix>.js|css|html` (short → long)

Same rule applies to charts under `<folder>/charts/`.

---

## 16. Filestore

Optional `filestore.yaml` mounts a jailed static route.

```yaml
id: assets
mount: /fs/orders
root: "{DATA_ROOT}/assets/orders"
read_only: true
allow_extensions: [".png",".jpg",".svg",".pdf"]
deny_extensions: [".js",".html",".sql",".parquet"]
max_file_bytes: 10485760
cache_control: "public, max-age=86400"
```

---

## 17. Frontend runtime (MANDATORY)

* Browser loads **Parquet only**
* DuckDB WASM renders:

  * tables
  * nested data
  * client-only filters
* Server renders filter bar
* URL controls all state

---

## 18. Error model

Errors MUST include:

* stable `error_code`
* SQL file + block
* normalized params
* reproduction URL
* **no secrets**

---

## 19. Lint requirements

`ducksearch lint` MUST detect:

* invalid placeholders
* illegal scan paths
* missing `{{mat ...}}` references
* invalid bindings
* cyclic imports
* invalid metadata schemas
* illegal SQL constructs

---

## 20. Cache & lifecycle

* Base artifacts → slice artifacts
* Stale-while-revalidate allowed
* v1 assumes **single process**

---

## 21. Launch defaults

* Server builds base + slice Parquet
* Client renders slice Parquet only
* Hybrid params default to client when safe
* Nested tables enabled
* Charts optional

---

## End of Specification

This document is **complete**.
A developer implementing exactly what is written here can build Ducksearch **correctly, safely, and deterministically** in one pass.
