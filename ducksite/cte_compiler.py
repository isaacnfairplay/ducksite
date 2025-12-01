from __future__ import annotations
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import re
import json
import duckdb

from .queries import NamedQuery, NetworkMetrics
from .utils import ensure_dir

MISSING_TABLE_RE = re.compile(r"Table with name ([^ ]+) does not exist")


def _parse_missing_table(e: duckdb.Error) -> Optional[str]:
    msg = str(e)
    m = MISSING_TABLE_RE.search(msg)
    if not m:
        return None
    return m.group(1).strip('"')


def _build_with_clause(ctes: Dict[str, str], main_sql: str) -> str:
    """
    Build a WITH clause that prepends our injected CTEs to main_sql.
    """
    if not ctes:
        return main_sql

    parts = [f"{name} AS ({sql})" for name, sql in ctes.items()]
    injected = ",\n     ".join(parts)

    stripped = main_sql.lstrip()
    leading_ws_len = len(main_sql) - len(stripped)
    leading_ws = main_sql[:leading_ws_len]

    if stripped.upper().startswith("WITH "):
        after_with = stripped[5:]
        return f"{leading_ws}WITH {injected},\n     {after_with}"

    return "WITH " + injected + "\n" + main_sql


def _find_read_parquet_paths(compiled_sql: str) -> List[str]:
    """
    Extract site-root-relative paths from read_parquet([...]) calls in the compiled SQL.
    """
    paths: List[str] = []
    for m in re.finditer(r"read_parquet\(\[(.*?)\]\)", compiled_sql, flags=re.DOTALL):
        inner = m.group(1)
        for s in inner.split(","):
            s = s.strip().strip("'\"")
            if s:
                paths.append(s)
    seen: Dict[str, None] = {}
    for p in paths:
        seen.setdefault(p, None)
    return list(seen.keys())


def _load_data_map_for_explain(site_root: Path) -> Dict[str, str]:
    """
    Load data_map.json when present, for EXPLAIN-time path rewriting.

    Keys: HTTP-visible paths like 'data/demo/demo-data.parquet'
    Values: filesystem paths for DuckDB to see during EXPLAIN.
    """
    path = site_root / "data_map.json"
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}
    try:
        raw = json.loads(text)
    except json.JSONDecodeError as e:
        print(f"[ducksite] WARNING: failed to parse {path}: {e}")
        return {}
    if not isinstance(raw, dict):
        return {}
    return {str(k): str(v) for k, v in raw.items()}


def _rewrite_virtual_paths_for_explain(site_root: Path, sql: str) -> str:
    """
    For EXPLAIN only:

    - Replace any read_parquet('data/...') paths that appear in data_map.json
      with their corresponding filesystem paths so DuckDB can locate files.

    This does NOT affect the compiled SQL written to static/sql/*.sql, which
    still uses HTTP-visible 'data/...' paths for DuckDB-Wasm.
    """
    data_map = _load_data_map_for_explain(site_root)
    if not data_map:
        return sql

    pattern = re.compile(r"read_parquet\(\s*\[(.*?)\]\s*\)", flags=re.DOTALL | re.IGNORECASE)

    def repl(match: re.Match) -> str:
        inner = match.group(1)
        parts = inner.split(",")
        out_parts: List[str] = []

        for part in parts:
            raw = part.strip()
            if not raw:
                continue
            # Extract the literal without quotes.
            m = re.match(r"^'([^']*)'$", raw)
            if not m:
                out_parts.append(raw)
                continue
            key = m.group(1)
            physical = data_map.get(key)
            if not physical:
                out_parts.append(raw)
                continue
            # Use the physical path in the EXPLAIN SQL.
            safe = physical.replace("'", "''")
            out_parts.append(f"'{safe}'")

        return f"read_parquet([{', '.join(out_parts)}])"

    rewritten = pattern.sub(repl, sql)
    return rewritten


def _compute_network_metrics(site_root: Path, compiled_sql: str) -> NetworkMetrics:
    """
    Compute simple network/bytes metrics based on the read_parquet(...) calls
    found in compiled_sql.

    NOTE: When using virtual data maps, compiled_sql still contains HTTP-visible
    'data/...' paths, so file sizes may not be available under site_root. We
    simply treat missing files as size 0.
    """
    paths = _find_read_parquet_paths(compiled_sql)
    sizes: List[int] = []
    for rel in paths:
        full = site_root / rel
        try:
            sizes.append(full.stat().st_size)
        except FileNotFoundError:
            sizes.append(0)
    if not sizes:
        return NetworkMetrics(
            num_files=0,
            total_bytes_cold=0,
            largest_file_bytes=0,
            two_largest_bytes=0,
            avg_file_bytes=0.0,
            sql_bytes=len(compiled_sql.encode("utf-8")),
        )
    total = sum(sizes)
    n = len(sizes)
    sorted_sizes = sorted(sizes, reverse=True)
    largest = sorted_sizes[0]
    second = sorted_sizes[1] if len(sorted_sizes) > 1 else 0
    avg = total / n
    return NetworkMetrics(
        num_files=n,
        total_bytes_cold=total,
        largest_file_bytes=largest,
        two_largest_bytes=largest + second,
        avg_file_bytes=avg,
        sql_bytes=len(compiled_sql.encode("utf-8")),
    )


_TEMPLATE_RE = re.compile(r"\$\{[^}]+\}")


def _strip_templates(sql: str) -> str:
    """
    Replace any ${...} template expressions with a dummy literal so DuckDB can
    parse and EXPLAIN the SQL for dependency resolution.

    We use NULL rather than TRUE so that both boolean contexts:

        WHERE ${params.category_filter}  -> WHERE NULL

    and scalar contexts:

        category = ${params.barcode_prefix} -> category = NULL
        SELECT ${params.barcode_prefix}     -> SELECT NULL

    remain type-correct for EXPLAIN.
    """
    return _TEMPLATE_RE.sub("NULL", sql)


def compile_query(
    site_root: Path,
    con: duckdb.DuckDBPyConnection,
    queries: Dict[str, NamedQuery],
    top_name: str,
) -> Tuple[str, NetworkMetrics, List[str]]:
    """
    Resolve dependencies for named_queries[top_name] into a single CTE query.
    """
    if top_name not in queries:
        raise KeyError(f"Unknown query {top_name}")

    base_sql = queries[top_name].sql
    ctes: Dict[str, str] = {}
    seen: set[str] = set()
    max_steps = 128

    # Ensure DuckDB resolves relative file paths against the site root.
    safe_root = site_root.as_posix().replace("'", "''")
    con.execute(f"SET file_search_path='{safe_root}'")

    steps = 0
    while True:
        steps += 1
        if steps > max_steps:
            raise RuntimeError(
                f"Too many dependency resolution steps for '{top_name}'. "
                "There is likely a bad or cyclic query definition."
            )

        # Build the full CTE envelope with original SQL (including templates).
        sql_with_ctes = _build_with_clause(ctes, base_sql)
        # For EXPLAIN, replace any ${...} templates with a neutral literal
        # (NULL) to keep the SQL type-correct, and rewrite any virtual
        # 'data/...' paths to physical filesystem paths using data_map.json.
        sql_for_explain = _strip_templates(sql_with_ctes)
        sql_for_explain = _rewrite_virtual_paths_for_explain(site_root, sql_for_explain)

        try:
            con.execute(f"EXPLAIN {sql_for_explain}")
            metrics = _compute_network_metrics(site_root, sql_for_explain)
            dep_names = list(ctes.keys())
            # Return the ORIGINAL SQL (with templates + virtual paths) so the
            # browser runtime can still parameterise it and use httpfs.
            return sql_with_ctes, metrics, dep_names
        except duckdb.Error as e:
            missing = _parse_missing_table(e)
            if not missing:
                # Not a missing-table error; re-raise with stripped SQL context.
                raise

            if missing == top_name:
                raise RuntimeError(
                    f"Query '{top_name}' appears to reference itself as a table "
                    "in its FROM clause. This is not supported; please fix the SQL."
                ) from e

            if missing not in queries:
                raise KeyError(
                    f"Query '{top_name}' depends on '{missing}', but no model or "
                    f"file_source named '{missing}' is defined."
                ) from e

            missing_q = queries[missing]

            if missing_q.kind == "page_query":
                raise RuntimeError(
                    f"Query '{top_name}' refers to page query '{missing}' as a table. "
                    "Page-level queries cannot be used as models; define a model in "
                    "sources_sql and reference that instead."
                ) from e

            if missing in seen:
                raise RuntimeError(
                    f"Cyclic or unresolved dependency involving '{missing}' while "
                    f"compiling '{top_name}'. Check the model definitions in sources_sql."
                ) from e

            seen.add(missing)

            # Inject the missing dependency as a CTE, prepending it so base
            # models appear before dependents.
            ctes = {missing: missing_q.sql, **ctes}


def write_compiled_sql(
    site_root: Path,
    page_rel_path: Path,
    query_id: str,
    sql_text: str,
    metrics: NetworkMetrics,
) -> Path:
    out_dir = site_root / "sql" / page_rel_path
    ensure_dir(out_dir)
    out_path = out_dir / f"{query_id}.sql"
    header = (
        f"-- METRICS: num_files={metrics.num_files} "
        f"total_bytes_cold={metrics.total_bytes_cold} "
        f"largest_file_bytes={metrics.largest_file_bytes} "
        f"two_largest_bytes={metrics.two_largest_bytes} "
        f"avg_file_bytes={metrics.avg_file_bytes:.2f} "
        f"sql_bytes={metrics.sql_bytes}\n"
    )
    out_path.write_text(header + sql_text + "\n", encoding="utf-8")
    return out_path


if __name__ == "__main__":
    import duckdb as dd
    from .queries import NamedQuery

    con = dd.connect()
    dummy = {"q": NamedQuery(name="q", sql="SELECT 1 AS x WHERE ${inputs.foo}", kind="test")}
    sql, metrics, deps = compile_query(Path(".").resolve(), con, dummy, "q")
    print(sql)
    print(metrics)
    print("deps:", deps)
