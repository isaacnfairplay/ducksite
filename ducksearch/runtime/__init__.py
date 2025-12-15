"""Lightweight execution pipeline for ducksearch reports."""
from __future__ import annotations

import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Mapping

import duckdb

from ducksearch.loader import RootLayout, validate_root
from ducksearch.report_parser import MATERIALIZE_RE, PLACEHOLDER_RE, _extract_parenthetical, _materialized_ctes, parse_report_sql

CACHE_TTL_SECONDS = 300


class ExecutionError(RuntimeError):
    """User-facing execution error that avoids leaking secrets."""

    def __init__(self, message: str):
        super().__init__(message)


@dataclass(frozen=True)
class ExecutionResult:
    base: Path
    materialized: Dict[str, Path]
    literal_sources: Dict[str, Path]
    bindings: Dict[str, Path]

    def as_payload(self, root: Path) -> dict:
        return {
            "base_parquet": str(self.base.relative_to(root)),
            "materialize": {k: str(v.relative_to(root)) for k, v in self.materialized.items()},
            "literal_sources": {k: str(v.relative_to(root)) for k, v in self.literal_sources.items()},
            "bindings": {k: str(v.relative_to(root)) for k, v in self.bindings.items()},
        }


def execute_report(root: Path, report: Path, *, now: float | None = None, _seen: set[Path] | None = None) -> ExecutionResult:
    """Execute ``report`` under ``root`` and return cached Parquet artifacts."""

    layout = validate_root(root)
    report = report.resolve()
    now = now or time.time()
    seen = _seen or set()
    if report in seen:
        raise ExecutionError(f"Import cycle detected for {report}")
    seen.add(report)

    parsed = parse_report_sql(report)
    report_key = _cache_key(layout, report)

    import_paths: Dict[str, Path] = {}
    for entry in parsed.metadata.get("IMPORTS") or []:
        target_rel = str(entry.get("report"))
        target_path = layout.reports / target_rel
        imported = execute_report(layout.root, target_path, now=now, _seen=seen)
        import_paths[str(entry.get("id"))] = imported.base

    cache = _Cache(layout, report_key)
    replacements = _build_placeholder_replacements(parsed.sql, cache, import_paths)
    prepared_sql = _rewrite_materialize(parsed.sql)
    prepared_sql = _substitute_placeholders(prepared_sql, replacements)

    conn = duckdb.connect(database=":memory:")
    try:
        mats = _prepare_materializations(conn, prepared_sql, cache, now)
        literal_sources = _materialize_literal_sources(conn, parsed.metadata.get("LITERAL_SOURCES") or [], cache, now)
        bindings = _materialize_bindings(conn, parsed.metadata.get("BINDINGS") or [], cache, now)
        base_path = cache.base
        if _should_refresh(base_path, now):
            conn.execute(f"copy ({prepared_sql}) to '{base_path.as_posix()}' (format 'parquet')")
        return ExecutionResult(base=base_path, materialized=mats, literal_sources=literal_sources, bindings=bindings)
    except duckdb.Error as exc:  # pragma: no cover - defensive
        raise ExecutionError("DuckDB execution failed") from exc


def _prepare_materializations(conn: duckdb.DuckDBPyConnection, sql: str, cache: "_Cache", now: float) -> Dict[str, Path]:
    mats: Dict[str, Path] = {}
    for name, body in _extract_materialization_bodies(sql).items():
        path = cache.materialize_path(name)
        if _should_refresh(path, now):
            conn.execute(f"create or replace temp table {name} as {body}")
            conn.execute(f"copy (select * from {name}) to '{path.as_posix()}' (format 'parquet')")
        mats[name] = path
    return mats


def _materialize_literal_sources(
    conn: duckdb.DuckDBPyConnection, entries: Iterable[Mapping[str, object]], cache: "_Cache", now: float
) -> Dict[str, Path]:
    outputs: Dict[str, Path] = {}
    for entry in entries:
        source = str(entry.get("from_cte"))
        value_col = str(entry.get("value_column"))
        lit_id = str(entry.get("id"))
        path = cache.literal_source_path(lit_id)
        if _should_refresh(path, now):
            conn.execute(f"copy (select {value_col} from {source}) to '{path.as_posix()}' (format 'parquet')")
        outputs[lit_id] = path
    return outputs


def _materialize_bindings(conn: duckdb.DuckDBPyConnection, entries: Iterable[Mapping[str, object]], cache: "_Cache", now: float) -> Dict[str, Path]:
    outputs: Dict[str, Path] = {}
    for entry in entries:
        bind_id = str(entry.get("id"))
        source = str(entry.get("source"))
        key_col = str(entry.get("key_column"))
        value_col = str(entry.get("value_column"))
        path = cache.binding_path(bind_id)
        if _should_refresh(path, now):
            conn.execute(
                f"copy (select {key_col} as key, {value_col} as value from {source}) to '{path.as_posix()}' (format 'parquet')"
            )
        outputs[bind_id] = path
    return outputs


def _rewrite_materialize(sql: str) -> str:
    return MATERIALIZE_RE.sub(lambda m: f"{m.group(1)} AS (", sql)


def _build_placeholder_replacements(sql: str, cache: "_Cache", imports: Mapping[str, Path]) -> Dict[str, str]:
    replacements: Dict[str, str] = {}
    for name in _materialized_ctes(sql):
        replacements[f"mat {name}"] = f"'{cache.materialize_path(name).as_posix()}'"
    for imp_id, path in imports.items():
        replacements[f"import {imp_id}"] = f"'{path.as_posix()}'"
    return replacements


def _substitute_placeholders(sql: str, replacements: Mapping[str, str]) -> str:
    def _replace(match: "re.Match[str]") -> str:
        key = f"{match.group(1).lower()} {match.group(2).strip()}"
        return replacements.get(key, match.group(0))

    return PLACEHOLDER_RE.sub(_replace, sql)


def _extract_materialization_bodies(sql: str) -> Dict[str, str]:
    bodies: Dict[str, str] = {}
    for match in MATERIALIZE_RE.finditer(sql):
        name = match.group(1)
        body, _ = _extract_parenthetical(sql, match.end())
        if body:
            bodies[name] = body
    return bodies


def _should_refresh(path: Path, now: float) -> bool:
    if not path.exists():
        return True
    return (now - path.stat().st_mtime) > CACHE_TTL_SECONDS


class _Cache:
    def __init__(self, layout: RootLayout, report_key: str):
        self.layout = layout
        self.report_key = report_key
        self.base = layout.cache / "artifacts" / f"{report_key}.parquet"
        self._ensure_dirs()

    def _ensure_dirs(self) -> None:
        for child in self.layout.cache_children:
            child.mkdir(parents=True, exist_ok=True)

    def materialize_path(self, name: str) -> Path:
        return self.layout.cache / "materialize" / f"{self.report_key}__{name}.parquet"

    def literal_source_path(self, name: str) -> Path:
        return self.layout.cache / "literal_sources" / f"{self.report_key}__{name}.parquet"

    def binding_path(self, name: str) -> Path:
        return self.layout.cache / "bindings" / f"{self.report_key}__{name}.parquet"


def _cache_key(layout: RootLayout, report: Path) -> str:
    rel = report.relative_to(layout.reports)
    return rel.with_suffix("").as_posix().replace("/", "__")
