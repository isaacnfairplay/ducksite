from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Any
import glob
import re
import json

import duckdb

from .config import ProjectConfig
from .utils import sha256_text, sha256_list


@dataclass
class QuerySignature:
    source_sql_hash: str
    dependency_hash: str
    file_set_hash: str


@dataclass
class NetworkMetrics:
    num_files: int
    total_bytes_cold: int
    largest_file_bytes: int
    two_largest_bytes: int
    avg_file_bytes: float
    sql_bytes: int


@dataclass
class NamedQuery:
    name: str
    sql: str
    kind: str
    signature: Optional[QuerySignature] = None
    metrics: Optional[NetworkMetrics] = None


def _expand_file_pattern(site_root: Path, pattern: str) -> List[str]:
    """
    Expand a site-root-relative glob like 'data/*.parquet' into a list of
    POSIX paths relative to site_root.

        site_root = demo/static
        pattern   = "data/demo-*.parquet"

    => returns ["data/demo-data.parquet", ...]
    """
    full = str(site_root / pattern)
    res: List[str] = []
    for p in glob.glob(full):
        p_path = Path(p)
        if p_path.is_file():
            rel = p_path.relative_to(site_root).as_posix()
            res.append(rel)
    return res


_TEMPLATE_NAME_RE = re.compile(r"\[(.+?)\]")


def _extract_template_expr(template_name: str) -> tuple[str, str]:
    """
    Extract the first [ ... ] segment from template_name.

    Returns:
        (base_name, expr)

    where:
      - expr is the SQL expression inside the brackets
      - base_name is template_name with that [ ... ] removed

    Example:
        template_name = "demo_[category]"
        -> base_name = "demo_"
           expr      = "category"
    """
    m = _TEMPLATE_NAME_RE.search(template_name)
    if not m:
        raise ValueError(
            f"template_name '{template_name}' does not contain a [expr] segment"
        )
    expr = m.group(1).strip()
    base = template_name[: m.start()] + template_name[m.end() :]
    base = base.strip()
    return base, expr


def _sql_literal(value: Any) -> str:
    """
    Render a Python value as a safe SQL literal.
    """
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    s = str(value)
    s = s.replace("'", "''")
    return f"'{s}'"


def _slug_value(value: Any) -> str:
    """
    Build a conservative identifier suffix from a value.

    - Keep letters, digits and underscore.
    - Replace other characters with underscore.
    - Collapse multiple underscores.
    - Trim leading/trailing underscores.
    - Fallback to a hash if the result is empty.
    """
    raw = str(value)
    out_chars: List[str] = []
    prev_us = False
    for ch in raw:
        if ch.isalnum() or ch == "_":
            out_chars.append(ch)
            prev_us = False
        else:
            if not prev_us:
                out_chars.append("_")
                prev_us = True
    slug = "".join(out_chars).strip("_")
    if not slug:
        slug = sha256_text(raw)[:8]
    return slug


def _build_read_parquet_expr(paths: List[str]) -> str:
    """
    Given a list of paths, build a DuckDB read_parquet([...]) expression.

    For the final compiled SQL we pass HTTP-visible paths like:

        'data/demo/demo-A.parquet'
    """
    files_expr = ",".join(f"'{p}'" for p in paths)
    return f"read_parquet([{files_expr}])"


def _load_data_map(site_root: Path) -> Dict[str, str]:
    """
    Load the virtual data map produced by symlinks.build_symlinks().

    Keys:
      HTTP-visible paths like 'data/demo/demo-A.parquet'
    Values:
      Absolute (or project-relative) filesystem paths.
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


def _logical_prefix_token(http_path: str) -> str:
    """
    Heuristic to extract the "logical prefix" token used for prefix-based
    file-list partitioning.

    We assume filenames look like one of:

        demo-A.parquet
        demo-B.parquet
        A-demo.parquet
        A_anything_else.parquet

    and we want the category prefix ('A', 'B', ...). Rules:

      - Take the basename without extension: stem = Path(p).stem
      - If there is a '-' in the stem:
            stem = "demo-A"   -> token = part after first '-' = "A"
            stem = "A-demo"   -> token = part before first '-' = "A"
      - Else:
            token = stem

    This keeps the demo flexible while still making “category is a prefix”
    meaningful with typical naming schemes.
    """
    name = Path(http_path).name
    stem = Path(http_path).stem  # e.g. "demo-A" or "A-demo"
    if "-" in stem:
        left, right = stem.split("-", 1)
        # Prefer the single-letter side as the "category-ish" token.
        if len(left) == 1:
            return left
        if len(right) == 1:
            return right
        # Fallback: use the right side as the varying token.
        return right
    return stem


def build_file_source_queries(
    cfg: ProjectConfig,
    con: Optional[duckdb.DuckDBPyConnection] = None,
) -> Dict[str, NamedQuery]:
    """
    Build NamedQuery entries for all file_sources.

    With virtual data maps enabled:

      - We prefer to pull file lists from static/data_map.json, grouping
        by HTTP prefix:

            data/<file_source_name>/...

      - These HTTP-relative paths are what DuckDB-Wasm will see via httpfs.

    Fallback:

      - If no data_map.json is present (or fs.name is missing), we fall back
        to globbing under site_root using fs.pattern.

    For templated file_sources (template_name is not None), we use DuckDB
    at build time to discover DISTINCT values of the template expression.

    **Extension for prefix-based file lists**

    When we expand templated file_sources, we also parameterise the **file
    list** by value under a simple convention:

      - Let `http_paths` be all HTTP-visible paths, e.g.

            data/demo/demo-A.parquet
            data/demo/demo-B.parquet
            data/demo/demo-C.parquet

      - For each distinct value `v` (e.g. 'A', 'B', 'C') we compute:

            value_paths = [p for p in http_paths
                           if logical_prefix_token(p).startswith(str(v))]

        i.e. we assume the category is a *prefix* of some logical token
        derived from the filename (for the demo, the 'A' or 'B' segment).

      - If value_paths is non-empty, the templated view uses:

            read_parquet(value_paths)

        instead of all files; otherwise we fall back to the full list.

    This matches the demo convention where category is effectively a prefix
    of each Parquet filename's logical token (demo-A.parquet, demo-B.parquet, ...).
    """
    result: Dict[str, NamedQuery] = {}
    if not cfg.file_sources:
        return result

    site_root = cfg.site_root
    data_map = _load_data_map(site_root)

    local_con: Optional[duckdb.DuckDBPyConnection] = None
    try:
        for fs in cfg.file_sources:
            rel_paths: List[str] = []

            # Prefer virtual map when we know the logical root name.
            if data_map and fs.name:
                prefix = f"data/{fs.name}/"
                for key in data_map.keys():
                    if key.startswith(prefix):
                        rel_paths.append(key)

            # Legacy fallback: pattern-based glob under site_root.
            if not rel_paths:
                rel_paths = _expand_file_pattern(site_root, fs.pattern)

            if not rel_paths:
                # No files for this file_source; skip it.
                continue

            # HTTP-visible paths for final SQL.
            http_paths = list(rel_paths)
            http_read_expr = _build_read_parquet_expr(http_paths)
            base_where = fs.row_filter or "TRUE"

            # Base (non-templated) file-source view.
            if fs.name:
                sql = f"SELECT * FROM {http_read_expr} WHERE {base_where}"
                result[fs.name] = NamedQuery(
                    name=fs.name,
                    sql=sql,
                    kind="file_source",
                )

            # No templating configured: skip per-value expansion.
            if not fs.template_name:
                continue

            # We have a template_name: ensure we have a DuckDB connection.
            if con is None and local_con is None:
                local_con = duckdb.connect()
            active_con = con or local_con
            if active_con is None:
                raise RuntimeError(
                    "Templated file_sources require a DuckDB connection for expansion."
                )

            # For DISTINCT sampling, prefer physical paths if we have
            # a virtual data map; otherwise use the HTTP paths directly.
            if data_map and fs.name:
                physical_paths = [
                    data_map[key] for key in http_paths if key in data_map
                ]
            else:
                # Translate HTTP paths into filesystem paths under site_root.
                physical_paths = [str(site_root / p) for p in http_paths]

            if not physical_paths:
                print(
                    f"[ducksite] WARNING: no physical paths available for templated "
                    f"file_source '{fs.name or fs.pattern}'; skipping template expansion."
                )
                continue

            explain_read_expr = _build_read_parquet_expr(physical_paths)

            # Ensure relative file paths resolve correctly for EXPLAIN.
            safe_root = site_root.as_posix().replace("'", "''")
            active_con.execute(f"SET file_search_path='{safe_root}'")

            try:
                base_name, expr = _extract_template_expr(fs.template_name)
            except ValueError as e:
                raise RuntimeError(
                    f"Invalid template_name for file source '{fs.name or fs.pattern}': {e}"
                ) from e

            distinct_sql = (
                f"SELECT DISTINCT {expr} AS v FROM {explain_read_expr} "
                f"WHERE {base_where} ORDER BY v"
            )

            try:
                rows = active_con.execute(distinct_sql).fetchall()
            except duckdb.IOException as err:
                # Do NOT crash the build just because upstream data is not
                # reachable at build time. The base file_source still works
                # at runtime via httpfs; we only skip the per-value views.
                print(
                    f"[ducksite] WARNING: IO error while sampling templated "
                    f"file_source '{fs.name or fs.pattern}': {err}; "
                    f"skipping template expansion."
                )
                continue

            if not rows:
                continue

            predicate_template = fs.row_filter_template
            if predicate_template is None:
                predicate_template = f"{expr} = ?"

            for (v,) in rows:
                if v is None:
                    continue

                # 1) Build the per-value predicate (row filter)
                lit = _sql_literal(v)
                predicate = predicate_template.replace("?", lit)
                if fs.row_filter:
                    predicate = f"({base_where}) AND ({predicate})"

                # 2) Parameterise the file list by **prefix** when possible.
                #
                #    We assume category is a prefix of a logical token derived
                #    from the file name (see _logical_prefix_token).
                #
                #    So for v = 'A' we prefer files whose token startswith 'A'.
                v_str = str(v)
                value_paths: List[str] = []
                for p in http_paths:
                    token = _logical_prefix_token(p)
                    if token.startswith(v_str):
                        value_paths.append(p)

                if not value_paths:
                    # Fallback: if we can't infer a prefix-based subset, just
                    # use the full list so the feature degrades gracefully.
                    value_paths = http_paths

                http_read_expr_v = _build_read_parquet_expr(value_paths)

                # IMPORTANT: final SQL uses HTTP-visible paths so DuckDB-Wasm
                # can fetch via httpfs. Only EXPLAIN/sampling used physical paths.
                view_sql = f"SELECT * FROM {http_read_expr_v} WHERE {predicate}"
                suffix = _slug_value(v)
                if not base_name:
                    view_name = suffix
                else:
                    view_name = f"{base_name}{suffix}"

                if view_name in result:
                    raise RuntimeError(
                        f"Templated file-source view name collision: '{view_name}' "
                        f"already defined while expanding template_name '{fs.template_name}'."
                    )

                result[view_name] = NamedQuery(
                    name=view_name,
                    sql=view_sql,
                    kind="file_source_template",
                )

    finally:
        if local_con is not None:
            try:
                local_con.close()
            except Exception:
                pass

    return result


def load_model_queries(cfg: ProjectConfig) -> Dict[str, NamedQuery]:
    """
    Load model queries from sources_sql/*.sql with blocks marked by `-- name: id`.
    """
    result: Dict[str, NamedQuery] = {}
    if not cfg.sources_sql_dir.exists():
        return result

    for path in cfg.sources_sql_dir.glob("*.sql"):
        text = path.read_text(encoding="utf-8")
        current_name: Optional[str] = None
        buf: List[str] = []
        for line in text.splitlines():
            if line.strip().startswith("-- name:"):
                if current_name and buf:
                    sql = "\n".join(buf).strip().rstrip(";")
                    result[current_name] = NamedQuery(
                        name=current_name,
                        sql=sql,
                        kind="model",
                    )
                    buf = []
                current_name = line.split(":", 1)[1].strip()
            else:
                buf.append(line)
        if current_name and buf:
            sql = "\n".join(buf).strip().rstrip(";")
            result[current_name] = NamedQuery(name=current_name, sql=sql, kind="model")
    return result


def compute_signature(sql: str, dep_names: List[str], file_paths: List[str]) -> QuerySignature:
    """
    Simple signatures for future incremental rebuild logic (not wired yet).
    """
    return QuerySignature(
        source_sql_hash=sha256_text(sql),
        dependency_hash=sha256_list(dep_names),
        file_set_hash=sha256_list(file_paths),
    )


if __name__ == "__main__":
    from .config import load_project_config

    root = Path(".").resolve()
    cfg = load_project_config(root)
    fq = build_file_source_queries(cfg)
    print("File-source queries:")
    for name, q in fq.items():
        print(f"  {name}: {q.sql}")
