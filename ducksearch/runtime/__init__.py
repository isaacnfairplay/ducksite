"""Lightweight execution pipeline for ducksearch reports."""
from __future__ import annotations

import hashlib
import json
import re
import time
import tomllib
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

DEFAULT_CACHE_TTL_SECONDS = 300


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


@dataclass(frozen=True)
class _BindingArtifacts:
    paths: Dict[str, Path]
    values: Dict[str, str]


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
    config_values = _select_config_values(parsed.metadata.get("CONFIG") or {}, _load_config(layout.config))
    cache_ttl = _select_cache_ttl(parsed.metadata.get("CACHE"))

    import_paths: Dict[str, Path] = {}
    import_cache_keys: Dict[str, str] = {}
    for entry in parsed.metadata.get("IMPORTS") or []:
        target_rel = str(entry.get("report"))
        target_path = layout.reports / target_rel
        import_payload = _select_import_payload(payload or {}, entry.get("pass_params"))
        imported = execute_report(layout.root, target_path, payload=import_payload, now=now, _seen=seen)
        import_paths[str(entry.get("id"))] = imported.base
        import_cache_keys[str(entry.get("id"))] = imported.base.stem

    report_key = _cache_key(layout, report, validated_params, import_cache_keys, config_values)
    cache = _Cache(layout, report_key)
    replacements = _build_placeholder_replacements(
        parsed.sql, cache, import_paths, validated_params, config_values
    )
    materialization_sql = _substitute_placeholders(parsed.sql, replacements).rstrip(";\n\t ")

    conn = duckdb.connect(database=":memory:")
    try:
        binding_placeholder_fallbacks = {
            f"bind {str(entry.get('id')).lower()}": "NULL"
            for entry in parsed.metadata.get("BINDINGS") or []
        }
        fallback_materialization_sql = _substitute_placeholders(
            materialization_sql, binding_placeholder_fallbacks
        )
        _prepare_materializations(conn, fallback_materialization_sql, cache, now, cache_ttl)

        bindings = _materialize_bindings(
            conn, parsed.metadata.get("BINDINGS") or [], validated_params, cache, now, cache_ttl
        )
        binding_replacements = _build_binding_replacements(parsed.metadata.get("BINDINGS") or [], bindings)
        materialization_sql = _substitute_placeholders(materialization_sql, binding_replacements)
        prepared_sql = _rewrite_materialize(materialization_sql)
        mats = _prepare_materializations(conn, materialization_sql, cache, now, cache_ttl, force=True)
        literal_sources = _materialize_literal_sources(
            conn, parsed.metadata.get("LITERAL_SOURCES") or [], cache, now, cache_ttl
        )
        final_sql = _substitute_placeholders(prepared_sql, binding_replacements)
        base_path = cache.base
        if _should_refresh(base_path, now, cache_ttl):
            conn.execute(f"copy ({final_sql}) to '{base_path.as_posix()}' (format 'parquet')")
        return ExecutionResult(
            base=base_path,
            materialized=mats,
            literal_sources=literal_sources,
            bindings=bindings.paths,
        )
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
    seen_keys: Dict[str, str] = {}
    for key, value in payload.items():
        lowered = key.lower()
        if lowered in seen_keys and key != seen_keys[lowered]:
            raise ExecutionError("Duplicate parameter key")
        seen_keys[lowered] = key
        normalized[lowered] = _coerce_to_sequence(value)

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


def _load_config(config_path: Path) -> Dict[str, object]:
    with config_path.open("rb") as fh:
        return tomllib.load(fh)


def _select_cache_ttl(cache_meta: Mapping[str, object] | None) -> float:
    if not cache_meta:
        return DEFAULT_CACHE_TTL_SECONDS
    raw_ttl = cache_meta.get("ttl_seconds")
    if raw_ttl is None:
        return DEFAULT_CACHE_TTL_SECONDS
    try:
        ttl_value = float(raw_ttl)
    except (TypeError, ValueError):
        raise ExecutionError("CACHE ttl_seconds must be a positive number")
    if ttl_value <= 0:
        raise ExecutionError("CACHE ttl_seconds must be positive")
    return ttl_value


def _select_config_values(config_meta: Mapping[str, object], config_values: Mapping[str, object]) -> Dict[str, object]:
    selected: Dict[str, object] = {}
    for name in config_meta.keys():
        if name not in config_values:
            raise ExecutionError(f"Missing config value for {name}")
        selected[name] = config_values[name]
    return selected


def _prepare_materializations(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    cache: "_Cache",
    now: float,
    ttl_seconds: float,
    *,
    force: bool = False,
) -> Dict[str, Path]:
    mats: Dict[str, Path] = {}
    for name, body in _extract_materialization_bodies(sql).items():
        path = cache.materialize_path(name)
        if force or _should_refresh(path, now, ttl_seconds):
            conn.execute(f"create or replace temp table {name} as {body}")
            conn.execute(f"copy (select * from {name}) to '{path.as_posix()}' (format 'parquet')")
        mats[name] = path
    return mats


def _materialize_literal_sources(
    conn: duckdb.DuckDBPyConnection,
    entries: Iterable[Mapping[str, object]],
    cache: "_Cache",
    now: float,
    ttl_seconds: float,
) -> Dict[str, Path]:
    outputs: Dict[str, Path] = {}
    for entry in entries:
        source = str(entry.get("from_cte"))
        value_col = str(entry.get("value_column"))
        lit_id = str(entry.get("id"))
        path = cache.literal_source_path(lit_id)
        if _should_refresh(path, now, ttl_seconds):
            conn.execute(f"copy (select {value_col} from {source}) to '{path.as_posix()}' (format 'parquet')")
        outputs[lit_id] = path
    return outputs


def _materialize_bindings(
    conn: duckdb.DuckDBPyConnection,
    entries: Iterable[Mapping[str, object]],
    params: Mapping[str, _ValidatedParam],
    cache: "_Cache",
    now: float,
    ttl_seconds: float,
) -> _BindingArtifacts:
    paths: Dict[str, Path] = {}
    values: Dict[str, str] = {}
    for entry in entries:
        bind_id = str(entry.get("id"))
        source = str(entry.get("source"))
        key_param = entry.get("key_param")
        key_sql = entry.get("key_sql")
        key_col = str(entry.get("key_column"))
        value_col = str(entry.get("value_column"))
        value_mode = str(entry.get("value_mode") or "single")

        if key_param and key_sql:
            raise ExecutionError(f"Binding {bind_id} cannot specify both key_param and key_sql")
        if not key_param and not key_sql:
            raise ExecutionError(f"Binding {bind_id} requires key_param or key_sql")
        if value_mode not in {"single", "list", "path_list_literal"}:
            raise ExecutionError(f"Binding {bind_id} has invalid value_mode: {value_mode}")

        binding_param = params.get(str(key_param)) if key_param else None
        if key_param and (not binding_param or not binding_param.apply_server):
            raise ExecutionError(f"Binding {bind_id} requires server parameter {key_param}")
        key_value = _binding_key_value(binding_param.value, str(key_param)) if key_param else None

        path = cache.binding_path(bind_id)
        if _should_refresh(path, now, ttl_seconds):
            conn.execute(
                f"copy (select {key_col} as key, {value_col} as value from {source}) to '{path.as_posix()}' (format 'parquet')"
            )
        paths[bind_id] = path

        if key_sql:
            rendered_key_sql = _render_binding_key_sql(str(key_sql), params, bind_id)
            key_view = _binding_key_view_name(bind_id)
            try:
                conn.execute(
                    f"create or replace temp view {key_view} as select * from ({rendered_key_sql}) as key_sub(key)"
                )
            except duckdb.Error as exc:  # pragma: no cover - defensive
                raise ExecutionError(f"Binding {bind_id} key_sql must return exactly one column") from exc

            candidate_keys = conn.execute(f"select * from {key_view}").fetchall()
            if not candidate_keys:
                raise ExecutionError(f"No binding keys produced for {bind_id}")

            rows = conn.execute(
                f"select distinct b.value from parquet_scan(?) as b join {key_view} as k on b.key = k.key",
                [path.as_posix()],
            ).fetchall()
        else:
            rows = conn.execute("select value from parquet_scan(?) where key = ?", [path.as_posix(), key_value]).fetchall()

        drop_missing_paths = bool(entry.get("drop_missing_paths")) if value_mode == "path_list_literal" else False

        row_values = [str(row[0]) for row in rows]
        if value_mode == "path_list_literal":
            _validate_literal_paths(row_values, bind_id)
            if drop_missing_paths:
                row_values = [val for val in row_values if _path_literal_exists(val)]

        if not row_values:
            if drop_missing_paths and value_mode == "path_list_literal":
                values[bind_id] = _sql_list_literal([])
                continue
            raise ExecutionError(f"No binding value for {bind_id}")

        if len(row_values) > 1 and value_mode == "single":
            sample = ", ".join(row_values[:3])
            raise ExecutionError(f"Multiple binding values for {bind_id}: {sample}. Set value_mode: list to allow lists")
        if value_mode in {"list", "path_list_literal"}:
            values[bind_id] = _sql_list_literal(row_values)
        else:
            values[bind_id] = row_values[0]
    return _BindingArtifacts(paths=paths, values=values)


def _render_binding_key_sql(key_sql: str, params: Mapping[str, _ValidatedParam], bind_id: str) -> str:
    for match in PLACEHOLDER_RE.finditer(key_sql):
        if match.group(1).lower() == "param":
            name = match.group(2).strip()
            param = params.get(name) or params.get(name.lower()) or params.get(name.upper())
            if not param:
                raise ExecutionError(f"Binding {bind_id} key_sql refers to missing param {name}")
            if not param.apply_server:
                raise ExecutionError(f"Binding {bind_id} key_sql requires server parameter {name}")
    replacements = _binding_param_replacements(params)
    return _substitute_placeholders(key_sql, replacements)


def _binding_key_view_name(bind_id: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9_]", "_", bind_id)
    if not re.match(r"^[A-Za-z_]", sanitized):
        sanitized = f"b_{sanitized}"
    return f"__binding_keys_{sanitized}"


def _binding_param_replacements(params: Mapping[str, _ValidatedParam]) -> Dict[str, str]:
    replacements: Dict[str, str] = {}
    for name, param in params.items():
        if not param.apply_server:
            continue
        key = name.lower()
        replacements[f"param {key}"] = _param_sql_literal(param.value, param.type)
        replacements[f"ident {key}"] = _ident_sql_literal(param.value, param.type)
        replacements[f"path {key}"] = _path_sql_literal(param.value)
    return replacements


def _binding_key_value(raw_value: object | None, name: str) -> object:
    if raw_value is None:
        raise ExecutionError(f"Binding {name} requires a value")
    if isinstance(raw_value, Sequence) and not isinstance(raw_value, (str, bytes)):
        if len(raw_value) != 1:
            raise ExecutionError(f"Binding {name} requires a single value")
        return raw_value[0]
    return raw_value


def _rewrite_materialize(sql: str) -> str:
    return MATERIALIZE_RE.sub(lambda m: f"{m.group(1)} AS (", sql)


def _build_placeholder_replacements(
    sql: str,
    cache: "_Cache",
    imports: Mapping[str, Path],
    params: Mapping[str, _ValidatedParam],
    config: Mapping[str, object],
) -> Dict[str, str]:
    replacements: Dict[str, str] = {}
    for name, value in config.items():
        replacements[f"config {name.lower()}"] = _escape_sql_string(str(value))
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


def _build_binding_replacements(entries: Iterable[Mapping[str, object]], bindings: _BindingArtifacts) -> Dict[str, str]:
    replacements: Dict[str, str] = {}
    for entry in entries:
        bind_id = str(entry.get("id"))
        value = bindings.values.get(bind_id)
        if value is not None:
            value_mode = str(entry.get("value_mode") or "single")
            if value_mode in {"list", "path_list_literal"}:
                replacements[f"bind {bind_id.lower()}"] = value
            else:
                replacements[f"bind {bind_id.lower()}"] = _escape_sql_string(value)
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


def _validate_literal_paths(paths: Sequence[str], bind_id: str) -> None:
    for path in paths:
        is_url = re.match(r"^[a-zA-Z0-9.+-]+://", path)

        if path.startswith("~"):
            raise ExecutionError(f"Binding {bind_id} cannot expand user home in paths")

        if is_url:
            if "*" in path:
                raise ExecutionError(f"Binding {bind_id} contains unsupported wildcard path")
            continue

        if re.search(r"[\\*\?\[]", path):
            raise ExecutionError(f"Binding {bind_id} contains unsupported wildcard path")


def _path_literal_exists(path: str) -> bool:
    if re.match(r"^[a-zA-Z0-9]+://", path):
        return True
    return Path(path).exists()


def _sql_list_literal(values: Sequence[str]) -> str:
    escaped = [f"'{_escape_sql_string(val)}'" for val in values]
    return f"[{', '.join(escaped)}]"


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


def _should_refresh(path: Path, now: float, ttl_seconds: float) -> bool:
    if not path.exists():
        return True
    return (now - path.stat().st_mtime) > ttl_seconds


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
    config_values: Mapping[str, object] | None = None,
) -> str:
    rel = report.relative_to(layout.reports)
    base_key = rel.with_suffix("").as_posix().replace("/", "__")
    if not params and not import_cache_keys and not config_values:
        return base_key

    payload: MutableMapping[str, object] = {}
    for name, param in (params or {}).items():
        if param.apply_server:
            payload[name] = param.value

    if import_cache_keys:
        payload["__imports__"] = {k: import_cache_keys[k] for k in sorted(import_cache_keys)}

    if config_values:
        payload["__config__"] = {k: config_values[k] for k in sorted(config_values)}

    if not payload:
        return base_key

    digest_source = json.dumps(payload, sort_keys=True, default=str)
    digest = hashlib.sha256(digest_source.encode("utf-8")).hexdigest()[:12]
    return f"{base_key}__{digest}"
