from __future__ import annotations
from typing import Dict, List, Match, Optional, Pattern, Tuple
from pathlib import Path
import re
import duckdb

from .data_map_cache import load_data_map
from .data_map_paths import data_map_shard
from .queries import NamedQuery, NetworkMetrics
from .utils import ensure_dir

__all__ = ["compile_query", "write_compiled_sql", "NetworkMetrics", "_find_read_parquet_paths"]

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


def _load_data_map_for_explain(site_root: Path, shards: list[str]) -> Dict[str, str]:
    merged: Dict[str, str] = {}
    targets = shards or [""]
    for shard in targets:
        merged.update(load_data_map(site_root, shard_hint=shard))
    return merged


def _rewrite_virtual_paths_for_explain(site_root: Path, sql: str) -> str:
    """
    For EXPLAIN only:

    - Replace any read_parquet('data/...') paths that appear in data_map.json
      with their corresponding filesystem paths so DuckDB can locate files.

    This does NOT affect the compiled SQL written to static/sql/*.sql, which
    still uses HTTP-visible 'data/...' paths for DuckDB-Wasm.
    """
    parquet_paths = _find_read_parquet_paths(sql)
    if not parquet_paths:
        _load_data_map_for_explain(site_root, [])
        return sql

    shard_hints = [data_map_shard(p) for p in parquet_paths]
    data_map = _load_data_map_for_explain(site_root, shard_hints)
    if not data_map:
        return sql

    pattern: Pattern[str] = re.compile(
        r"read_parquet\(\s*\[(.*?)\]\s*\)", flags=re.DOTALL | re.IGNORECASE
    )

    def repl(match: Match[str]) -> str:
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

    This intentionally avoids file stat calls; we only count the referenced
    files and measure the compiled SQL size.
    """
    paths = _find_read_parquet_paths(compiled_sql)
    return NetworkMetrics(
        num_files=len(paths),
        total_bytes_cold=0,
        largest_file_bytes=0,
        two_largest_bytes=0,
        avg_file_bytes=0.0,
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


def _find_join_clauses_without_condition(sql: str) -> List[str]:
    """
    Identify JOIN clauses that omit ON/USING, which DuckDB rejects.
    """
    warnings: List[str] = []
    pattern = re.compile(r"\b(?:left|right|full|inner|cross)?\s*join\s+[^\s;()]+", re.IGNORECASE)
    matches = list(pattern.finditer(sql))

    for idx, match in enumerate(matches):
        clause = match.group(0)
        clause_lower = clause.lower()
        if "cross join" in clause_lower or "natural" in clause_lower:
            continue

        start = match.end()
        end = len(sql)
        if idx + 1 < len(matches):
            end = matches[idx + 1].start()
        semicolon = sql.find(";", start)
        if semicolon != -1 and semicolon < end:
            end = semicolon

        segment = sql[start:end].lower()
        if " on " not in segment and " using " not in segment and " natural " not in segment:
            warnings.append(
                f"JOIN '{clause.strip()}' is missing an ON/USING clause; "
                "DuckDB raises a parser error for that shorthand."
            )

    return warnings


def _apply_parser_hints(sql: str) -> tuple[str, List[str]]:
    """
    Catch common parser-unsafe typos and optionally auto-fix them.

    The catalog intentionally focuses on short edits that DuckDB will always
    reject so that we can surface clearer feedback (and sometimes correct the
    query) before the user hits a parser exception.
    """

    warnings: List[str] = []
    fixed_sql = sql

    replacements: List[tuple[Pattern[str], str, str]] = [
        (
            re.compile(
                r"\b(?:left|right|full|inner)\s*(?:outer\s+)?asof\s+join\b", re.IGNORECASE
            ),
            "ASOF JOINs cannot be prefixed with LEFT/RIGHT/FULL/INNER; using ASOF JOIN.",
            "ASOF JOIN",
        ),
        (
            re.compile(r"\b(?:left|right|full|inner)?\s*atni\s+join\b", re.IGNORECASE),
            "Detected typo 'ATNI JOIN'; assuming ANTI JOIN.",
            "ANTI JOIN",
        ),
    ]

    for pattern, message, replacement in replacements:
        if pattern.search(fixed_sql):
            updated = pattern.sub(replacement, fixed_sql)
            if updated != fixed_sql:
                fixed_sql = updated
            warnings.append(f"{message} Auto-corrected safely.")

    using_no_parens = re.compile(
        r"\busing\s+([A-Za-z_][A-Za-z0-9_]*)\b(?!\s*\()", re.IGNORECASE
    )

    def _wrap_using(match: Match[str]) -> str:
        column = match.group(1)
        warnings.append(
            f"USING clause should wrap column names in parentheses; rewrote to USING ({column})."
        )
        return f"USING ({column})"

    fixed_sql = using_no_parens.sub(_wrap_using, fixed_sql)

    warnings.extend(_find_join_clauses_without_condition(fixed_sql))
    return fixed_sql, warnings


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

    print(f"[ducksite] compiling query '{top_name}'")
    base_sql, parser_warnings = _apply_parser_hints(queries[top_name].sql)
    for warning in parser_warnings:
        print(f"[ducksite] ParserWarning: {warning}")
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
            print(
                "[ducksite] compiled "
                f"'{top_name}' with {len(dep_names)} deps "
                f"(files={metrics.num_files}, bytes={metrics.total_bytes_cold}, sql_bytes={metrics.sql_bytes})"
            )
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
            print(
                f"[ducksite] resolving dependency '{missing}' for '{top_name}'"
            )
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
