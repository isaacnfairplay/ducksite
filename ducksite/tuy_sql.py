from __future__ import annotations

from pathlib import Path
import tempfile
from typing import Dict, List

from .config import ProjectConfig
from .queries import load_model_queries


def _parse_model_blocks(text: str) -> Dict[str, List[str]]:
    blocks: Dict[str, List[str]] = {}
    current: List[str] = []
    current_name: str | None = None
    for line in text.splitlines():
        if line.strip().startswith("-- name:"):
            if current_name is not None:
                blocks[current_name] = current
            current_name = line.split(":", 1)[1].strip()
            current = []
        else:
            current.append(line)
    if current_name is not None:
        blocks[current_name] = current
    return blocks


def _render_model_blocks(blocks: Dict[str, List[str]]) -> str:
    parts: List[str] = []
    for name, lines in blocks.items():
        parts.append(f"-- name: {name}")
        parts.extend(lines)
        parts.append("")
    return "\n".join(parts).rstrip() + "\n"


def _validate_model_text(text: str) -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        sql_dir = root / "sources_sql"
        sql_dir.mkdir(parents=True)
        (sql_dir / "models.sql").write_text(text, encoding="utf-8")
        cfg = ProjectConfig(root=root, dirs={}, file_sources=[])
        load_model_queries(cfg)


def add_model_block(text: str, name: str, sql_body: str) -> str:
    blocks = _parse_model_blocks(text)
    if name in blocks:
        raise ValueError(f"Model '{name}' already exists")
    blocks[name] = sql_body.strip().splitlines()
    rendered = _render_model_blocks(blocks)
    _validate_model_text(rendered)
    return rendered


def modify_model_block(text: str, name: str, sql_body: str) -> str:
    blocks = _parse_model_blocks(text)
    if name not in blocks:
        raise ValueError(f"Model '{name}' not found")
    blocks[name] = sql_body.strip().splitlines()
    rendered = _render_model_blocks(blocks)
    _validate_model_text(rendered)
    return rendered


def remove_model_block(text: str, name: str) -> str:
    blocks = _parse_model_blocks(text)
    if name not in blocks:
        raise ValueError(f"Model '{name}' not found")
    blocks.pop(name)
    rendered = _render_model_blocks(blocks)
    _validate_model_text(rendered)
    return rendered


def handle(command: str, root: Path) -> None:
    sql_dir = root / "sources_sql"
    sql_dir.mkdir(exist_ok=True)
    target = sql_dir / "models.sql"
    text = target.read_text(encoding="utf-8") if target.exists() else ""

    try:
        if command == "add":
            name = input("Model name: ").strip()
            body = input("SQL body: ")
            updated = add_model_block(text, name, body)
        elif command == "modify":
            name = input("Model name to modify: ").strip()
            body = input("New SQL body: ")
            updated = modify_model_block(text, name, body)
        elif command == "remove":
            name = input("Model name to remove: ").strip()
            updated = remove_model_block(text, name)
        else:
            print(f"Unknown command '{command}' for SQL handler")
            return
        target.write_text(updated, encoding="utf-8")
        print(f"Updated SQL models in {target}")
    except Exception as exc:  # pragma: no cover - user facing
        print(f"Error: {exc}")
