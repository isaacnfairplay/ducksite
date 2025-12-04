from __future__ import annotations

from pathlib import Path
import re
import tempfile

from .markdown_parser import parse_markdown_page
from .tuy_ui import FieldSpec, prompt_form


def _validate_markdown_text(text: str) -> None:
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td) / "page.md"
        tmp.write_text(text, encoding="utf-8")
        parse_markdown_page(tmp, tmp)


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
    updated = re.sub(rf"(data_query\s*:\s*){re.escape(old)}", rf"\g<1>{new}", updated)
    _validate_markdown_text(updated)
    return updated


def handle(command: str, root: Path) -> None:
    content_dir = root / "content"
    content_dir.mkdir(exist_ok=True)
    target = content_dir / "page.md"
    text = target.read_text(encoding="utf-8") if target.exists() else ""

    try:
        if command == "add":
            while True:
                values = prompt_form(
                    "Add dashboard block",
                    "Add SQL, table, or echart blocks. Supply the identifier dashboards should reference.",
                    [
                        FieldSpec(
                            name="heading",
                            label="Optional heading or notes before the block",
                            placeholder="# My dashboard section",
                            optional=True,
                            multiline=True,
                            help_text="Use this to add markdown headers or helper text before the DSL block.",
                        ),
                        FieldSpec(
                            name="kind",
                            label="Block type",
                            choices=[("sql", "SQL"), ("echart", "EChart"), ("table", "Table")],
                            default="sql",
                        ),
                        FieldSpec(name="block_id", label="Block id", placeholder="orders_by_day"),
                        FieldSpec(
                            name="body",
                            label="Block body",
                            multiline=True,
                            placeholder="query: my_query\noption: value",
                            help_text="For charts, note the data_query you want to bind; for SQL, paste the query body.",
                        ),
                    ],
                )
                heading = values.get("heading", "").strip()
                base_text = text.rstrip()
                if heading:
                    base_text = base_text + "\n\n" + heading
                try:
                    updated = add_markdown_block(base_text + "\n", values["kind"], values["block_id"], values["body"])
                    break
                except Exception as exc:
                    print(f"Validation failed: {exc}")
                    continue
        elif command == "modify":
            kind_choice = prompt_form(
                "Pick block to modify",
                "Choose the block whose SQL or visualization options should change.",
                [
                    FieldSpec(
                        name="kind",
                        label="Block type",
                        choices=[("sql", "SQL"), ("echart", "EChart"), ("table", "Table")],
                        default="sql",
                    ),
                    FieldSpec(name="block_id", label="Block id", placeholder="orders_by_day"),
                ],
            )
            while True:
                body_choice = prompt_form(
                    "Updated block body",
                    "Paste the replacement content. Ensure referenced SQL ids already exist or add them first.",
                    [FieldSpec(name="body", label="Block body", multiline=True)],
                )
                try:
                    updated = modify_markdown_block(text, kind_choice["kind"], kind_choice["block_id"], body_choice["body"])
                    break
                except Exception as exc:
                    print(f"Validation failed: {exc}")
                    continue
        elif command == "remove":
            values = prompt_form(
                "Remove block",
                "Pick the block you want to drop. Update charts or queries that referenced it.",
                [
                    FieldSpec(
                        name="kind",
                        label="Block type",
                        choices=[("sql", "SQL"), ("echart", "EChart"), ("table", "Table")],
                        default="sql",
                    ),
                    FieldSpec(name="block_id", label="Block id", placeholder="orders_by_day"),
                ],
            )
            updated = remove_markdown_block(text, values["kind"], values["block_id"])
        else:
            print(f"Unknown command '{command}' for Markdown handler")
            return
        target.write_text(updated, encoding="utf-8")
        print(f"Updated markdown at {target}")
    except KeyboardInterrupt:
        print("Cancelled")
    except Exception as exc:  # pragma: no cover - user facing
        print(f"Error: {exc}")
