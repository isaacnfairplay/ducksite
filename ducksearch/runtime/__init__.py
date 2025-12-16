"""Lightweight execution pipeline for ducksearch reports."""
from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Mapping, MutableMapping, Sequence

import duckdb

from ducksearch.loader import RootLayout, validate_root
from ducksearch.report_parser import (
    MATERIALIZE_RE,
    PLACEHOLDER_RE,
    Parameter,
    ParameterType,
    _extract_parenthetical,
    _materialized_ctes,
    parse_report_sql,
)

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


@dataclass(frozen=True)
class _ValidatedParam:
    value: object | None
    apply_server: bool
    type: ParameterType


def execute_report(
    root: Path,
    report: Path,
    *,
    payload: Mapping[str, object] | None = None,
    now: float | None = None,
    _seen: set[Path] | None = None,
) -> ExecutionResult:
    """Execute ``report`` under ``root`` and return cached Parquet artifacts."""

    layout = validate_root(root)
    report = report.resolve()
    now = now or time.time()
    seen = _seen or set()
    if report in seen:
        raise ExecutionError(f"Import cycle detected for {report}")
    seen.add(report)

    parsed = parse_report_sql(report)
    validated_params = _validate_parameter_payload(payload or {}, parsed.parameters)

    import_paths: Dict[str, Path] = {}
    import_cache_keys: Dict[str, str] = {}
    for entry in parsed.metadata.get("IMPORTS") or []:
        target_rel = str(entry.get("report"))
        target_path = layout.reports / target_rel
        import_payload = _select_import_payload(payload or {}, entry.get("pass_params"))
        imported = execute_report(layout.root, target_path, payload=import_payload, now=now, _seen=seen)
        import_paths[str(entry.get("id"))] = imported.base
        import_cache_keys[str(entry.get("id"))] = imported.base.stem

    report_key = _cache_key(layout, report, validated_params, import_cache_keys)
    cache = _Cache(layout, report_key)
    replacements = _build_placeholder_replacements(parsed.sql, cache, import_paths, validated_params)
    materialization_sql = _substitute_placeholders(parsed.sql, replacements).rstrip(";\n\t ")
    prepared_sql = _rewrite_materialize(materialization_sql)

    conn = duckdb.connect(database=":memory:")
    try:
        mats = _prepare_materializations(conn, materialization_sql, cache, now)
        literal_sources = _materialize_literal_sources(conn, parsed.metadata.get("LITERAL_SOURCES") or [], cache, now)
        bindings = _materialize_bindings(conn, parsed.metadata.get("BINDINGS") or [], cache, now)
        base_path = cache.base
        if _should_refresh(base_path, now):
            conn.execute(f"copy ({prepared_sql}) to '{base_path.as_posix()}' (format 'parquet')")
        return ExecutionResult(base=base_path, materialized=mats, literal_sources=literal_sources, bindings=bindings)
    except duckdb.Error as exc:  # pragma: no cover - defensive
        raise ExecutionError("DuckDB execution failed") from exc


def _select_import_payload(payload: Mapping[str, object], pass_params: object) -> Dict[str, object]:
    if not pass_params:
        return {}
    allowed = {str(name).lower() for name in pass_params}
    selected: Dict[str, object] = {}
    for key, value in payload.items():
        base = _strip_param_prefix(str(key).lower())
        if base in allowed or key.lower() == "__force_server":
            selected[key] = value
    return selected


def _validate_parameter_payload(payload: Mapping[str, object], parameters: Sequence[Parameter]) -> Dict[str, _ValidatedParam]:
    normalized: Dict[str, Sequence[object]] = {}
    for key, value in payload.items():
        normalized[key.lower()] = _coerce_to_sequence(value)

    force_all_server = _is_truthy(normalized.get("__force_server"))
    validated: Dict[str, _ValidatedParam] = {}

    for param in parameters:
        lower_name = param.name.lower()
        client_key = f"__client__{lower_name}"
        server_key = f"__server__{lower_name}"

        source_key = None
        apply_server = param.scope == "data" or force_all_server

        if client_key in normalized:
            source_key = client_key
        if lower_name in normalized:
            source_key = lower_name
            if param.scope != "view":
                apply_server = True
        if server_key in normalized:
            source_key = server_key
            apply_server = True

        raw_values = normalized.get(source_key or "", ())
        coerced = _coerce_param_value(raw_values, param.type, param.name)
        if param.scope == "hybrid" and source_key == client_key and not force_all_server:
            apply_server = False
        if param.scope == "view" and source_key != server_key and not force_all_server:
            apply_server = False
        validated[param.name] = _ValidatedParam(value=coerced, apply_server=apply_server, type=param.type)
    return validated


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


def _build_placeholder_replacements(
    sql: str, cache: "_Cache", imports: Mapping[str, Path], params: Mapping[str, _ValidatedParam]
) -> Dict[str, str]:
    replacements: Dict[str, str] = {}
    for name in _materialized_ctes(sql):
        replacements[f"mat {name.lower()}"] = f"'{cache.materialize_path(name).as_posix()}'"
    for imp_id, path in imports.items():
        replacements[f"import {imp_id.lower()}"] = f"'{path.as_posix()}'"
    for name, param in params.items():
        key = name.lower()
        replacements[f"param {key}"] = _param_sql_literal(param.value if param.apply_server else None, param.type)
        replacements[f"ident {key}"] = _ident_sql_literal(param.value if param.apply_server else None, param.type)
        replacements[f"path {key}"] = _path_sql_literal(param.value if param.apply_server else None)
    return replacements


def _substitute_placeholders(sql: str, replacements: Mapping[str, str]) -> str:
    def _replace(match: "re.Match[str]") -> str:
        key = f"{match.group(1).lower()} {match.group(2).strip().lower()}"
        return replacements.get(key, match.group(0))

    return PLACEHOLDER_RE.sub(_replace, sql)


def _coerce_to_sequence(value: object) -> Sequence[object]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, Sequence):
        return value
    return [value]


def _is_truthy(values: Sequence[object] | None) -> bool:
    if not values:
        return False
    truthy = {"1", "true", "t", "yes", "on"}
    for val in values:
        if str(val).strip().lower() in truthy:
            return True
    return False


def _coerce_param_value(values: Sequence[object], param_type: ParameterType, name: str) -> object | None:
    if param_type.kind == "optional":
        if not values:
            return None
        if param_type.inner is None:
            raise ExecutionError(f"Invalid parameter type for {name}")
        return _coerce_param_value(values, param_type.inner, name)

    if param_type.kind == "list":
        if param_type.inner is None:
            raise ExecutionError(f"Invalid parameter type for {name}")
        return [_coerce_scalar(v, param_type.inner, name) for v in values]

    if not values:
        return None
    return _coerce_scalar(values[0], param_type, name)


def _coerce_scalar(value: object, param_type: ParameterType, name: str) -> object:
    kind = param_type.kind

    if kind == "literal":
        return _coerce_literal(value, param_type.literals, name)
    if kind == "injected_ident_literal":
        candidate = str(value)
        if candidate not in {str(lit) for lit in param_type.literals}:
            raise ExecutionError(f"Invalid value for parameter {name}")
        return candidate
    if kind == "int":
        try:
            return int(value)
        except Exception as exc:  # noqa: BLE001
            raise ExecutionError(f"Invalid value for parameter {name}") from exc
    if kind == "float":
        try:
            return float(value)
        except Exception as exc:  # noqa: BLE001
            raise ExecutionError(f"Invalid value for parameter {name}") from exc
    if kind == "bool":
        lowered = str(value).strip().lower()
        if lowered in {"1", "true", "t", "yes", "on"}:
            return True
        if lowered in {"0", "false", "f", "no", "off"}:
            return False
        raise ExecutionError(f"Invalid value for parameter {name}")
    if kind in {"date", "datetime", "str", "InjectedStr"}:
        return str(value)

    raise ExecutionError(f"Unsupported parameter type for {name}")


def _coerce_literal(value: object, allowed: Sequence[object], name: str) -> object:
    for literal in allowed:
        if _matches_literal(value, literal):
            return literal
    raise ExecutionError(f"Invalid value for parameter {name}")


def _matches_literal(value: object, literal: object) -> bool:
    if value == literal:
        return True
    if isinstance(value, str):
        try:
            if isinstance(literal, bool):
                lowered = value.strip().lower()
                return (lowered in {"1", "true", "t", "yes"} and literal is True) or (
                    lowered in {"0", "false", "f", "no"} and literal is False
                )
            cast_value = type(literal)(value)
            return cast_value == literal
        except Exception:  # noqa: BLE001
            return False
    return False


def _param_sql_literal(value: object | None, param_type: ParameterType) -> str:
    if param_type.kind == "optional":
        if value is None:
            return "NULL"
        if param_type.inner is None:
            return "NULL"
        return _param_sql_literal(value, param_type.inner)

    if param_type.kind == "list":
        inner = param_type.inner
        if inner is None:
            return "NULL"
        items = value or []
        if not isinstance(items, Sequence):
            return "NULL"
        rendered = [_scalar_sql_literal(item, inner) for item in items]
        return f"({', '.join(rendered)})" if rendered else "(NULL)"

    if value is None:
        return "NULL"

    return _scalar_sql_literal(value, param_type)


def _scalar_sql_literal(value: object, param_type: ParameterType) -> str:
    kind = param_type.kind
    if kind in {"int", "float"}:
        return str(value)
    if kind == "bool":
        return "TRUE" if bool(value) else "FALSE"
    if kind == "literal":
        return f"'{_escape_sql_string(str(value))}'"
    if kind == "injected_ident_literal":
        return _escape_identifier(str(value))
    if kind in {"date", "datetime", "str", "InjectedStr"}:
        return f"'{_escape_sql_string(str(value))}'"
    return f"'{_escape_sql_string(str(value))}'"


def _ident_sql_literal(value: object | None, param_type: ParameterType) -> str:
    if param_type.kind == "optional":
        if value is None:
            return "NULL"
        if param_type.inner is None:
            return "NULL"
        return _ident_sql_literal(value, param_type.inner)

    if value is None:
        return "NULL"

    if param_type.kind not in {"injected_ident_literal", "str", "InjectedStr"}:
        return "NULL"

    try:
        return _escape_identifier(str(value))
    except ExecutionError:
        return "NULL"


def _path_sql_literal(value: object | None) -> str:
    if value is None:
        return "NULL"
    return f"'{_escape_sql_string(str(value))}'"


def _escape_sql_string(value: str) -> str:
    return value.replace("'", "''")


def _escape_identifier(value: str) -> str:
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", value):
        raise ExecutionError("Invalid identifier")
    return value


def _strip_param_prefix(name: str) -> str:
    for prefix in ("__client__", "__server__"):
        if name.startswith(prefix):
            return name[len(prefix) :]
    return name


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


def _cache_key(
    layout: RootLayout,
    report: Path,
    params: Mapping[str, _ValidatedParam] | None = None,
    import_cache_keys: Mapping[str, str] | None = None,
) -> str:
    rel = report.relative_to(layout.reports)
    base_key = rel.with_suffix("").as_posix().replace("/", "__")
    if not params and not import_cache_keys:
        return base_key

    payload: MutableMapping[str, object] = {}
    for name, param in params.items():
        if param.apply_server:
            payload[name] = param.value

    if import_cache_keys:
        payload["__imports__"] = {k: import_cache_keys[k] for k in sorted(import_cache_keys)}

    if not payload:
        return base_key

    digest_source = json.dumps(payload, sort_keys=True, default=str)
    digest = hashlib.sha256(digest_source.encode("utf-8")).hexdigest()[:12]
    return f"{base_key}__{digest}"
