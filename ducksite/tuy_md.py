from __future__ import annotations

from pathlib import Path
import re
import tempfile

from .markdown_parser import parse_markdown_page


def _validate_markdown_text(text: str) -> None:
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td) / "page.md"
        tmp.write_text(text, encoding="utf-8")
        parse_markdown_page(tmp, tmp.name)


def _block_regex(kind: str, block_id: str) -> re.Pattern[str]:
    return re.compile(rf"```{re.escape(kind)}\s+{re.escape(block_id)}\s*\n(.*?)```", re.DOTALL)


def add_markdown_block(text: str, kind: str, block_id: str, body: str) -> str:
    pattern = _block_regex(kind, block_id)
    if pattern.search(text):
        raise ValueError(f"Block '{block_id}' already exists")
    addition = f"\n```{kind} {block_id}\n{body.strip()}\n```\n"
    updated = text.rstrip() + addition
    _validate_markdown_text(updated)
    return updated


def modify_markdown_block(text: str, kind: str, block_id: str, body: str) -> str:
    pattern = _block_regex(kind, block_id)
    if not pattern.search(text):
        raise ValueError(f"Block '{block_id}' not found")
    updated = pattern.sub(lambda _: f"```{kind} {block_id}\n{body.strip()}\n```", text)
    _validate_markdown_text(updated)
    return updated


def remove_markdown_block(text: str, kind: str, block_id: str) -> str:
    pattern = _block_regex(kind, block_id)
    if not pattern.search(text):
        raise ValueError(f"Block '{block_id}' not found")
    updated = pattern.sub("", text).strip() + "\n"
    _validate_markdown_text(updated)
    return updated


def rename_sql_block(text: str, old: str, new: str) -> str:
    pattern = _block_regex("sql", old)
    match = pattern.search(text)
    if not match:
        raise ValueError(f"SQL block '{old}' not found")
    body = match.group(1).strip()
    updated = pattern.sub(f"```sql {new}\n{body}\n```", text)
    updated = re.sub(rf"(query\s*:\s*){re.escape(old)}", rf"\g<1>{new}", updated)
    updated = re.sub(rf"(sql_id\s*:\s*){re.escape(old)}", rf"\g<1>{new}", updated)
    _validate_markdown_text(updated)
    return updated


def handle(command: str, root: Path) -> None:
    content_dir = root / "content"
    content_dir.mkdir(exist_ok=True)
    target = content_dir / "page.md"
    text = target.read_text(encoding="utf-8") if target.exists() else ""

    try:
        if command == "add":
            kind = input("Block type (sql/echart/table): ").strip()
            block_id = input("Block id: ").strip()
            body = input("Block body: ")
            updated = add_markdown_block(text, kind, block_id, body)
        elif command == "modify":
            kind = input("Block type: ").strip()
            block_id = input("Block id to modify: ").strip()
            body = input("New body: ")
            updated = modify_markdown_block(text, kind, block_id, body)
        elif command == "remove":
            kind = input("Block type: ").strip()
            block_id = input("Block id to remove: ").strip()
            updated = remove_markdown_block(text, kind, block_id)
        else:
            print(f"Unknown command '{command}' for Markdown handler")
            return
        target.write_text(updated, encoding="utf-8")
        print(f"Updated markdown at {target}")
    except Exception as exc:  # pragma: no cover - user facing
        print(f"Error: {exc}")
