from __future__ import annotations

from pathlib import Path
import tempfile
import tomllib
from typing import Any, Dict, List, Optional

from .config import load_project_config


def _render_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, dict):
        inner = ", ".join(f"{k} = {_render_value(v)}" for k, v in value.items())
        return "{" + inner + "}"
    if isinstance(value, list):
        return "[" + ", ".join(_render_value(v) for v in value) + "]"
    escaped = str(value).replace("\"", "\\\"")
    return f'"{escaped}"'


def _render_config(data: Dict[str, Any]) -> str:
    parts: List[str] = []
    dirs = data.get("dirs") or {}
    if dirs:
        parts.append("[dirs]")
        for key, value in dirs.items():
            parts.append(f"{key} = {_render_value(value)}")
        parts.append("")

    for fs in data.get("file_sources", []) or []:
        parts.append("[[file_sources]]")
        for key, value in fs.items():
            parts.append(f"{key} = {_render_value(value)}")
        parts.append("")

    return "\n".join(parts).strip() + "\n"


def _parse_file_source_block(block_text: str) -> Dict[str, Any]:
    data = tomllib.loads(block_text)
    file_sources = data.get("file_sources")
    if not file_sources:
        raise ValueError("Block must define [[file_sources]]")
    entry = file_sources[0]
    if "name" not in entry:
        raise ValueError("File source block must include a name")
    return entry


def _validate_config_text(root: Path, text: str) -> None:
    with tempfile.TemporaryDirectory() as td:
        tmp_root = Path(td)
        cfg_path = tmp_root / "ducksite.toml"
        cfg_path.write_text(text, encoding="utf-8")
        load_project_config(tmp_root)


def render_config_text(
    root: Path,
    *,
    dirs: Optional[Dict[str, Any]] = None,
    file_sources: Optional[List[Dict[str, Any]]] = None,
    comments: Optional[List[str]] = None,
) -> str:
    """Render and validate a ducksite.toml string using structured data.

    The helper keeps the TUY tooling in charge of formatting and validation so
    callers don't need to hand-write TOML snippets.
    """

    parts: List[str] = []
    if comments:
        parts.extend([f"# {line}".rstrip() for line in comments])
        parts.append("")

    rendered = _render_config({"dirs": dirs or {}, "file_sources": file_sources or []})
    parts.append(rendered.rstrip())

    final_text = "\n".join(parts).rstrip() + "\n"
    _validate_config_text(root, final_text)
    return final_text


def add_file_source_block(config_text: str, block_text: str, root: Path) -> str:
    entry = _parse_file_source_block(block_text)
    data = tomllib.loads(config_text) if config_text.strip() else {}
    sources = list(data.get("file_sources", []) or [])
    if any(fs.get("name") == entry.get("name") for fs in sources):
        raise ValueError(f"File source '{entry.get('name')}' already exists")
    sources.append(entry)
    data["file_sources"] = sources
    rendered = _render_config(data)
    _validate_config_text(root, rendered)
    return rendered


def add_file_source_entry(
    config_text: str, entry: Dict[str, Any], root: Path, comments: Optional[List[str]] = None
) -> str:
    """Add a file-source entry using structured data instead of raw TOML blocks."""

    data = tomllib.loads(config_text) if config_text.strip() else {}
    sources = list(data.get("file_sources", []) or [])
    if any(fs.get("name") == entry.get("name") for fs in sources):
        raise ValueError(f"File source '{entry.get('name')}' already exists")
    sources.append(entry)
    rendered = render_config_text(root, dirs=data.get("dirs", {}), file_sources=sources, comments=comments)
    _validate_config_text(root, rendered)
    return rendered


def modify_file_source_block(config_text: str, block_text: str, root: Path) -> str:
    entry = _parse_file_source_block(block_text)
    data = tomllib.loads(config_text) if config_text.strip() else {}
    sources = list(data.get("file_sources", []) or [])
    name = entry.get("name")
    for idx, fs in enumerate(sources):
        if fs.get("name") == name:
            sources[idx] = entry
            break
    else:
        raise ValueError(f"File source '{name}' not found")
    data["file_sources"] = sources
    rendered = _render_config(data)
    _validate_config_text(root, rendered)
    return rendered


def remove_file_source_block(config_text: str, name: str, root: Path) -> str:
    data = tomllib.loads(config_text) if config_text.strip() else {}
    sources = list(data.get("file_sources", []) or [])
    filtered = [fs for fs in sources if fs.get("name") != name]
    if len(filtered) == len(sources):
        raise ValueError(f"File source '{name}' not found")
    data["file_sources"] = filtered
    rendered = _render_config(data)
    _validate_config_text(root, rendered)
    return rendered


def _prompt_file_source_block() -> str:
    name = input("File source name: ").strip()
    pattern = input("Pattern [data/*.parquet]: ").strip() or "data/*.parquet"
    template = input("template_name (optional): ").strip()
    upstream = input("upstream_glob (optional): ").strip()
    parts = ["[[file_sources]]", f'name = "{name}"', f'pattern = "{pattern}"']
    if template:
        parts.append(f'template_name = "{template}"')
    if upstream:
        parts.append(f'upstream_glob = "{upstream}"')
    return "\n".join(parts) + "\n"


def handle(command: str, root: Path) -> None:
    cfg_path = root / "ducksite.toml"
    if not cfg_path.exists():
        print(f"Config file not found at {cfg_path}")
        return

    text = cfg_path.read_text(encoding="utf-8")

    try:
        if command == "add":
            updated = add_file_source_block(text, _prompt_file_source_block(), root)
            cfg_path.write_text(updated, encoding="utf-8")
            print(f"Added file source to {cfg_path}")
        elif command == "modify":
            updated = modify_file_source_block(text, _prompt_file_source_block(), root)
            cfg_path.write_text(updated, encoding="utf-8")
            print(f"Modified file source in {cfg_path}")
        elif command == "remove":
            target = input("File source name to remove: ").strip()
            updated = remove_file_source_block(text, target, root)
            cfg_path.write_text(updated, encoding="utf-8")
            print(f"Removed file source from {cfg_path}")
        else:
            print(f"Unknown command '{command}' for TOML handler")
    except Exception as exc:  # pragma: no cover - user facing
        print(f"Error: {exc}")
