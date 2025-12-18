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
MATERIALIZE_RE = re.compile(
    r"\b([A-Za-z0-9_]+)\b\s+AS\s+MATERIALIZE(?:_CLOSED)?\s*\(", re.IGNORECASE
)
PLACEHOLDER_RE = re.compile(r"\{\{\s*([A-Za-z_]+)\s+([^}]+?)\s*\}\}")


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


class LintError(ValueError):
    """Structured lint error with a stable error code."""

    def __init__(self, code: str, message: str):
        super().__init__(f"[{code}] {message}")
        self.code = code


def parse_report_sql(path: Path) -> Report:
    text = path.read_text()
    metadata, stripped_sql = _extract_metadata(text)
    _validate_metadata_schema(metadata)
    _ensure_single_statement(stripped_sql)
    params = _parse_params(metadata.get("PARAMS", {}) or {}, stripped_sql)
    _validate_cross_references(metadata, params)
    _validate_sql(stripped_sql, metadata, params)
    return Report(sql=stripped_sql.strip(), metadata=metadata, parameters=params)


def _extract_metadata(sql_text: str) -> tuple[Dict[str, Any], str]:
    metadata: Dict[str, Any] = {}
    stripped = sql_text
    for match in METADATA_RE.finditer(sql_text):
        block = match.group(1)
        if block not in SUPPORTED_BLOCKS:
            raise LintError("DS001", f"Unsupported metadata block: {block}")
        yaml_text = match.group(2).strip()
        metadata[block] = yaml.safe_load(yaml_text) or {}
        stripped = stripped.replace(match.group(0), "")
    return metadata, stripped


def _ensure_single_statement(sql_text: str) -> None:
    statements = _split_top_level_statements(sql_text)
    if len(statements) != 1:
        raise LintError("DS008", "Report SQL must contain exactly one statement")


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
            raise LintError("DS002", "PARAMS entries must be YAML mappings")
        lowered = name.lower()
        if lowered in seen_lower:
            raise LintError("DS003", "Duplicate parameter names differ only by case")
        seen_lower.add(lowered)

        type_spec = cfg.get("type")
        if not isinstance(type_spec, str):
            raise LintError("DS004", f"Parameter {name} is missing a type")
        param_type = parse_param_type(type_spec)

        scope = cfg.get("scope")
        inferred_scope = scope or infer_scope(name, sql)
        if inferred_scope not in {"data", "view", "hybrid"}:
            raise LintError("DS005", f"Invalid scope for {name}: {inferred_scope}")

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
        raise LintError("DS002", "applies_to must be a mapping")
    cte = raw.get("cte")
    mode = raw.get("mode")
    if not cte or not mode:
        raise LintError("DS002", "applies_to requires cte and mode")
    if mode not in {"wrapper", "inline"}:
        raise LintError("DS002", "applies_to mode must be wrapper or inline")
    return AppliesTo(cte=str(cte), mode=str(mode))


def _enforce_applies_to(sql: str, applies_to: AppliesTo) -> None:
    cte_names = _cte_names(sql)
    if applies_to.cte not in cte_names:
        raise LintError("DS007", f"CTE {applies_to.cte} not defined in SQL")
    if applies_to.mode == "wrapper":
        base_name = f"{applies_to.cte}_base"
        if base_name not in cte_names:
            raise LintError("DS007", f"Wrapper applies_to expects {base_name} CTE")


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

    raise LintError("DS004", f"Unsupported parameter type: {spec}")


def _parse_literal_values(body: str) -> Tuple[Any, ...]:
    parsed = ast.literal_eval(f"[{body}]")
    if not isinstance(parsed, list):
        raise LintError("DS004", "Literal must parse to a list of values")
    return tuple(parsed)


def infer_scope(name: str, sql: str) -> str:
    pattern = re.compile(r"\{\{\s*(?:param|ident)\s+" + re.escape(name) + r"\s*\}\}", re.IGNORECASE)
    return "data" if pattern.search(sql) else "view"


def _validate_metadata_schema(metadata: Dict[str, Any]) -> None:
    _ensure_mapping(metadata, "DS002", "Metadata payload must be a mapping")

    for block, value in metadata.items():
        if block == "PARAMS":
            _ensure_mapping(value, "DS002", "PARAMS block must be a mapping")
        elif block == "CONFIG":
            _ensure_mapping(value, "DS002", "CONFIG block must be a mapping")
            for key, val in value.items():
                if not isinstance(val, str):
                    raise LintError("DS002", f"CONFIG {key} must be a string type hint")
        elif block == "CACHE":
            _ensure_mapping(value, "DS002", "CACHE block must be a mapping")
            ttl = value.get("ttl_seconds") if isinstance(value, dict) else None
            if ttl is not None and (isinstance(ttl, bool) or not isinstance(ttl, (int, float))):
                raise LintError("DS002", "CACHE ttl_seconds must be a number")
            if isinstance(ttl, (int, float)) and ttl <= 0:
                raise LintError("DS002", "CACHE ttl_seconds must be positive")
        elif block in {"SOURCES", "TABLE", "SEARCH", "FACETS", "CHARTS", "DERIVED_PARAMS", "SECRETS"}:
            _ensure_mapping(value, "DS002", f"{block} block must be a mapping")
        elif block == "LITERAL_SOURCES":
            _ensure_list_of_dicts(value, "DS002", "LITERAL_SOURCES must be a list of mappings")
            for entry in value:
                for key in ("id", "from_cte", "value_column"):
                    if key not in entry:
                        raise LintError("DS002", f"LITERAL_SOURCES entries require {key}")
        elif block == "BINDINGS":
            _ensure_list_of_dicts(value, "DS002", "BINDINGS must be a list of mappings")
            for entry in value:
                for key in ("id", "source", "key_column", "value_column", "kind"):
                    if key not in entry:
                        raise LintError("DS002", f"BINDINGS entries require {key}")
                if not entry.get("key_param") and not entry.get("key_sql"):
                    raise LintError("DS002", "BINDINGS entries require key_param or key_sql")
                if entry.get("key_param") and entry.get("key_sql"):
                    raise LintError("DS002", "BINDINGS entries cannot set both key_param and key_sql")
                if entry.get("value_mode") and entry.get("value_mode") not in {"single", "list", "path_list_literal"}:
                    raise LintError("DS002", "BINDINGS value_mode must be single, list, or path_list_literal")
        elif block == "IMPORTS":
            _ensure_list_of_dicts(value, "DS002", "IMPORTS must be a list of mappings")
            for entry in value:
                if "id" not in entry or "report" not in entry:
                    raise LintError("DS002", "IMPORTS entries require id and report")
                if "pass_params" in entry and not isinstance(entry["pass_params"], list):
                    raise LintError("DS002", "IMPORTS pass_params must be a list")


def _ensure_mapping(value: Any, code: str, message: str) -> None:
    if value is None:
        return
    if not isinstance(value, dict):
        raise LintError(code, message)


def _ensure_list_of_dicts(value: Any, code: str, message: str) -> None:
    if value is None:
        return
    if not isinstance(value, list):
        raise LintError(code, message)
    for entry in value:
        if not isinstance(entry, dict):
            raise LintError(code, message)


def _validate_cross_references(metadata: Dict[str, Any], params: List[Parameter]) -> None:
    param_names = {p.name for p in params}

    bindings = metadata.get("BINDINGS") or []
    seen_bind_ids: set[str] = set()
    for binding in bindings:
        bind_id = str(binding.get("id"))
        if bind_id in seen_bind_ids:
            raise LintError("DS003", f"Duplicate binding id: {bind_id}")
        seen_bind_ids.add(bind_id)
        key_param = binding.get("key_param")
        if key_param and key_param not in param_names:
            raise LintError("DS010", f"Binding {bind_id} refers to missing param {key_param}")

    imports = metadata.get("IMPORTS") or []
    seen_import_ids: set[str] = set()
    for imp in imports:
        imp_id = str(imp.get("id"))
        if imp_id in seen_import_ids:
            raise LintError("DS003", f"Duplicate import id: {imp_id}")
        seen_import_ids.add(imp_id)

    _detect_dependency_cycles(metadata)


def _detect_dependency_cycles(metadata: Dict[str, Any]) -> None:
    nodes: set[str] = set()
    edges: dict[str, set[str]] = {}

    bindings = metadata.get("BINDINGS") or []
    for binding in bindings:
        bind_id = str(binding.get("id"))
        nodes.add(bind_id)
        target = str(binding.get("source"))
        if target:
            edges.setdefault(bind_id, set()).add(target)

    imports = metadata.get("IMPORTS") or []
    for imp in imports:
        imp_id = str(imp.get("id"))
        nodes.add(imp_id)
        target = str(imp.get("report"))
        if target:
            edges.setdefault(imp_id, set()).add(target)

    visiting: set[str] = set()
    visited: set[str] = set()

    def _dfs(node: str) -> None:
        if node in visited:
            return
        if node in visiting:
            raise LintError("DS013", f"Cycle detected involving {node}")
        visiting.add(node)
        for dest in edges.get(node, set()):
            if dest in nodes:
                _dfs(dest)
        visiting.remove(node)
        visited.add(node)

    for node in nodes:
        _dfs(node)


def _validate_sql(sql: str, metadata: Dict[str, Any], params: List[Parameter]) -> None:
    sanitized = _strip_comments(sql)
    _detect_illegal_constructs(sanitized)
    _validate_parquet_paths(sanitized)
    _validate_placeholders(sql, metadata, params)


def _strip_comments(sql_text: str) -> str:
    result: list[str] = []
    in_line_comment = False
    in_block_comment = False
    in_string: str | None = None

    i = 0
    length = len(sql_text)
    while i < length:
        ch = sql_text[i]
        nxt = sql_text[i + 1] if i + 1 < length else ""

        if in_string:
            result.append(ch)
            if ch == in_string:
                if nxt == in_string:
                    result.append(nxt)
                    i += 1
                else:
                    in_string = None
        elif in_line_comment:
            if ch == "\n":
                in_line_comment = False
                result.append(ch)
        elif in_block_comment:
            if ch == "*" and nxt == "/":
                i += 1
                in_block_comment = False
        else:
            if ch in {"'", '"'}:
                in_string = ch
                result.append(ch)
            elif ch == "-" and nxt == "-":
                in_line_comment = True
                i += 1
            elif ch == "/" and nxt == "*":
                in_block_comment = True
                i += 1
            else:
                result.append(ch)
        i += 1
    return "".join(result)


def _detect_illegal_constructs(sql: str) -> None:
    patterns = [
        r"\battach\b",
        r"\binstall\b",
        r"\bload\b",
        r"\bpragma\b",
        r"\bset\b",
        r"\bcreate\b",
        r"\balter\b",
        r"\bdrop\b",
        r"\binsert\b",
        r"\bupdate\b",
        r"\bdelete\b",
    ]
    for pattern in patterns:
        if re.search(pattern, sql, flags=re.IGNORECASE):
            keyword = pattern.strip("\\b")
            raise LintError("DS012", f"Illegal SQL construct detected: {keyword}")

    for match in re.finditer(r"\bcopy\b", sql, flags=re.IGNORECASE):
        statement = sql[match.start() :]
        copy_clause = statement.split(";", 1)[0]
        has_to = re.search(r"\bto\b", copy_clause, flags=re.IGNORECASE)
        options_segment = copy_clause[has_to.end() :] if has_to else ""
        parquet_format = re.search(
            r"\bformat\b[^;]*\bparquet\b", options_segment, flags=re.IGNORECASE
        )
        if not (has_to and parquet_format):
            raise LintError("DS012", "Illegal SQL construct detected: copy")


def _validate_parquet_paths(sql: str) -> None:
    for match in re.finditer(r"parquet_scan\s*\(", sql, flags=re.IGNORECASE):
        start = match.end()
        body, _ = _extract_parenthetical(sql, start)
        if body is None:
            continue
        arg = _first_argument(body)
        if not arg:
            continue
        stripped_arg = arg.strip()
        if "||" in _strip_comments(stripped_arg):
            raise LintError("DS011", "parquet_scan path must not use string concatenation")
        if stripped_arg[0:1] not in {"'", '"'}:
            placeholder = PLACEHOLDER_RE.match(stripped_arg)
            if placeholder and placeholder.group(1).lower() == "bind":
                continue
            raise LintError("DS011", "parquet_scan path must be a string literal")


def _extract_parenthetical(sql: str, start_index: int) -> tuple[str | None, int]:
    depth = 1
    current: list[str] = []
    in_string: str | None = None
    i = start_index
    length = len(sql)
    while i < length:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < length else ""
        if in_string:
            current.append(ch)
            if ch == in_string:
                if nxt == in_string:
                    current.append(nxt)
                    i += 1
                else:
                    in_string = None
        else:
            if ch in {"'", '"'}:
                in_string = ch
                current.append(ch)
            elif ch == "(":
                depth += 1
                current.append(ch)
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    return "".join(current), i
                current.append(ch)
            else:
                current.append(ch)
        i += 1
    return None, length


def _first_argument(body: str) -> str:
    depth = 0
    in_string: str | None = None
    for idx, ch in enumerate(body):
        nxt = body[idx + 1] if idx + 1 < len(body) else ""
        if in_string:
            if ch == in_string:
                if nxt == in_string:
                    continue
                in_string = None
        else:
            if ch in {"'", '"'}:
                in_string = ch
            elif ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif ch == "," and depth == 0:
                return body[:idx]
    return body


def _validate_placeholders(sql: str, metadata: Dict[str, Any], params: List[Parameter]) -> None:
    config_names = set((metadata.get("CONFIG") or {}).keys())
    param_names = {p.name for p in params}
    binding_ids = {str(entry.get("id")) for entry in (metadata.get("BINDINGS") or [])}
    import_ids = {str(entry.get("id")) for entry in (metadata.get("IMPORTS") or [])}
    mat_names = _materialized_ctes(_strip_comments(sql))

    allowed_types = {"config", "param", "bind", "mat", "import", "ident", "path"}
    for match in PLACEHOLDER_RE.finditer(sql):
        placeholder_type = match.group(1).lower()
        name = match.group(2).strip()
        if placeholder_type not in allowed_types:
            raise LintError("DS009", f"Invalid placeholder type: {placeholder_type}")
        if placeholder_type == "config" and name not in config_names:
            raise LintError("DS010", f"Unknown config placeholder: {name}")
        if placeholder_type == "param" and name not in param_names:
            raise LintError("DS010", f"Unknown param placeholder: {name}")
        if placeholder_type == "bind" and name not in binding_ids:
            raise LintError("DS010", f"Unknown binding placeholder: {name}")
        if placeholder_type == "mat" and name not in mat_names:
            raise LintError("DS010", f"Unknown materialization placeholder: {name}")
        if placeholder_type == "import" and name not in import_ids:
            raise LintError("DS010", f"Unknown import placeholder: {name}")


def _materialized_ctes(sql: str) -> set[str]:
    return {match.group(1) for match in MATERIALIZE_RE.finditer(sql)}
