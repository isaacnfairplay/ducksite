"""Parser for ducksearch report SQL files and metadata."""
from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import yaml


SUPPORTED_BLOCKS: tuple[str, ...] = (
    "PARAMS",
    "CONFIG",
    "SOURCES",
    "CACHE",
    "TABLE",
    "SEARCH",
    "FACETS",
    "CHARTS",
    "DERIVED_PARAMS",
    "LITERAL_SOURCES",
    "BINDINGS",
    "IMPORTS",
    "SECRETS",
)

METADATA_RE = re.compile(r"/\*{3}([A-Z_]+)\s*(.*?)\*{3}/", re.DOTALL)
CTE_DEF_RE = re.compile(r"\b([A-Za-z0-9_]+)\b\s+AS\s*\(", re.IGNORECASE)


@dataclass(frozen=True)
class AppliesTo:
    cte: str
    mode: str


@dataclass(frozen=True)
class ParameterType:
    kind: str
    inner: "ParameterType | None" = None
    literals: Tuple[Any, ...] = ()


@dataclass(frozen=True)
class Parameter:
    name: str
    type: ParameterType
    scope: str
    applies_to: AppliesTo | None = None


@dataclass(frozen=True)
class Report:
    sql: str
    metadata: Dict[str, Any]
    parameters: List[Parameter]


def parse_report_sql(path: Path) -> Report:
    text = path.read_text()
    metadata, stripped_sql = _extract_metadata(text)
    _ensure_single_statement(stripped_sql)
    params = _parse_params(metadata.get("PARAMS", {}) or {}, stripped_sql)
    return Report(sql=stripped_sql.strip(), metadata=metadata, parameters=params)


def _extract_metadata(sql_text: str) -> tuple[Dict[str, Any], str]:
    metadata: Dict[str, Any] = {}
    stripped = sql_text
    for match in METADATA_RE.finditer(sql_text):
        block = match.group(1)
        if block not in SUPPORTED_BLOCKS:
            raise ValueError(f"Unsupported metadata block: {block}")
        yaml_text = match.group(2).strip()
        metadata[block] = yaml.safe_load(yaml_text) or {}
        stripped = stripped.replace(match.group(0), "")
    return metadata, stripped


def _ensure_single_statement(sql_text: str) -> None:
    statements = _split_top_level_statements(sql_text)
    if len(statements) != 1:
        raise ValueError("Report SQL must contain exactly one statement")


def _split_top_level_statements(sql_text: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    in_string: str | None = None
    in_line_comment = False
    in_block_comment = False

    i = 0
    length = len(sql_text)
    while i < length:
        ch = sql_text[i]
        next_ch = sql_text[i + 1] if i + 1 < length else ""

        if in_string:
            current.append(ch)
            if ch == in_string:
                if next_ch == in_string:
                    current.append(next_ch)
                    i += 1
                else:
                    in_string = None
        elif in_line_comment:
            if ch == "\n":
                in_line_comment = False
        elif in_block_comment:
            if ch == "*" and next_ch == "/":
                i += 1
                in_block_comment = False
        else:
            if ch in {"'", '"'}:
                in_string = ch
                current.append(ch)
            elif ch == "-" and next_ch == "-":
                in_line_comment = True
                i += 1
            elif ch == "/" and next_ch == "*":
                in_block_comment = True
                i += 1
            elif ch == ";":
                segment = "".join(current).strip()
                if segment:
                    statements.append(segment)
                current = []
            else:
                current.append(ch)

        i += 1

    tail = "".join(current).strip()
    if tail:
        statements.append(tail)

    return statements


def _parse_params(raw: Dict[str, Any], sql: str) -> List[Parameter]:
    params: List[Parameter] = []
    seen_lower: set[str] = set()
    for name, cfg in raw.items():
        if not isinstance(cfg, dict):
            raise ValueError("PARAMS entries must be YAML mappings")
        lowered = name.lower()
        if lowered in seen_lower:
            raise ValueError("Duplicate parameter names differ only by case")
        seen_lower.add(lowered)

        type_spec = cfg.get("type")
        if not isinstance(type_spec, str):
            raise ValueError(f"Parameter {name} is missing a type")
        param_type = parse_param_type(type_spec)

        scope = cfg.get("scope")
        inferred_scope = scope or infer_scope(name, sql)
        if inferred_scope not in {"data", "view", "hybrid"}:
            raise ValueError(f"Invalid scope for {name}: {inferred_scope}")

        applies_to_cfg = cfg.get("applies_to")
        applies_to = _parse_applies_to(applies_to_cfg) if applies_to_cfg else None
        if applies_to:
            _enforce_applies_to(sql, applies_to)

        params.append(
            Parameter(
                name=name,
                type=param_type,
                scope=inferred_scope,
                applies_to=applies_to,
            )
        )
    return params


def _parse_applies_to(raw: Dict[str, Any]) -> AppliesTo:
    if not isinstance(raw, dict):
        raise ValueError("applies_to must be a mapping")
    cte = raw.get("cte")
    mode = raw.get("mode")
    if not cte or not mode:
        raise ValueError("applies_to requires cte and mode")
    if mode not in {"wrapper", "inline"}:
        raise ValueError("applies_to mode must be wrapper or inline")
    return AppliesTo(cte=str(cte), mode=str(mode))


def _enforce_applies_to(sql: str, applies_to: AppliesTo) -> None:
    cte_names = _cte_names(sql)
    if applies_to.cte not in cte_names:
        raise ValueError(f"CTE {applies_to.cte} not defined in SQL")
    if applies_to.mode == "wrapper":
        base_name = f"{applies_to.cte}_base"
        if base_name not in cte_names:
            raise ValueError(f"Wrapper applies_to expects {base_name} CTE")


def _cte_names(sql: str) -> set[str]:
    return {match.group(1) for match in CTE_DEF_RE.finditer(sql)}


def parse_param_type(spec: str) -> ParameterType:
    text = spec.strip()
    optional_prefix = "Optional["
    list_prefix = "List["
    literal_prefix = "Literal["
    injected_ident_prefix = "InjectedIdentLiteral["

    if text.startswith(optional_prefix) and text.endswith("]"):
        inner_text = text[len(optional_prefix) : -1]
        return ParameterType(kind="optional", inner=parse_param_type(inner_text))
    if text.startswith(list_prefix) and text.endswith("]"):
        inner_text = text[len(list_prefix) : -1]
        return ParameterType(kind="list", inner=parse_param_type(inner_text))
    if text.startswith(literal_prefix) and text.endswith("]"):
        literals = _parse_literal_values(text[len(literal_prefix) : -1])
        return ParameterType(kind="literal", literals=literals)
    if text.startswith(injected_ident_prefix) and text.endswith("]"):
        literals = _parse_literal_values(text[len(injected_ident_prefix) : -1])
        return ParameterType(kind="injected_ident_literal", literals=literals)

    primitive_kinds = {
        "int",
        "float",
        "bool",
        "date",
        "datetime",
        "str",
        "InjectedStr",
    }
    if text in primitive_kinds:
        return ParameterType(kind=text)

    raise ValueError(f"Unsupported parameter type: {spec}")


def _parse_literal_values(body: str) -> Tuple[Any, ...]:
    parsed = ast.literal_eval(f"[{body}]")
    if not isinstance(parsed, list):
        raise ValueError("Literal must parse to a list of values")
    return tuple(parsed)


def infer_scope(name: str, sql: str) -> str:
    pattern = re.compile(r"\{\{\s*(?:param|ident)\s+" + re.escape(name) + r"\s*\}\}", re.IGNORECASE)
    return "data" if pattern.search(sql) else "view"
