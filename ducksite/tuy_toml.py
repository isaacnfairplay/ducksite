from __future__ import annotations

from pathlib import Path
import tempfile
import tomllib
from typing import Any, Dict, List, Optional

from .config import load_project_config
from .tuy_ui import FieldSpec, prompt_form


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


def add_dir_entry(
    config_text: str, name: str, path: str, root: Path, comments: Optional[List[str]] = None
) -> str:
    data = tomllib.loads(config_text) if config_text.strip() else {}
    dirs = dict(data.get("dirs") or {})
    if name in dirs:
        raise ValueError(f"Directory constant '{name}' already exists")
    dirs[name] = path
    return render_config_text(root, dirs=dirs, file_sources=data.get("file_sources", []), comments=comments)


def modify_dir_entry(
    config_text: str, name: str, path: str, root: Path, comments: Optional[List[str]] = None
) -> str:
    data = tomllib.loads(config_text) if config_text.strip() else {}
    dirs = dict(data.get("dirs") or {})
    if name not in dirs:
        raise ValueError(f"Directory constant '{name}' not found")
    dirs[name] = path
    return render_config_text(root, dirs=dirs, file_sources=data.get("file_sources", []), comments=comments)


def remove_dir_entry(
    config_text: str, name: str, root: Path, comments: Optional[List[str]] = None
) -> str:
    data = tomllib.loads(config_text) if config_text.strip() else {}
    dirs = dict(data.get("dirs") or {})
    if name not in dirs:
        raise ValueError(f"Directory constant '{name}' not found")
    dirs.pop(name)
    return render_config_text(root, dirs=dirs, file_sources=data.get("file_sources", []), comments=comments)


def _parse_file_source_block(block_text: str) -> Dict[str, Any]:
    data: Dict[str, Any] = tomllib.loads(block_text)
    file_sources = data.get("file_sources")
    if not isinstance(file_sources, list) or not file_sources:
        raise ValueError("Block must define [[file_sources]]")
    entry_raw = file_sources[0]
    if not isinstance(entry_raw, dict):
        raise ValueError("File source must be a mapping")
    entry: Dict[str, Any] = dict(entry_raw)
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
    return render_config_text(
        root,
        dirs=data.get("dirs", {}),
        file_sources=sources,
        comments=comments,
    )


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


def _prompt_file_source_block(existing: Dict[str, Any] | None = None) -> str:
    help_text = (
        "Add the Parquet or CSV source you want dashboards to rely on."
        " Patterns accept ${DIR_*} placeholders so you can reuse directory constants."
    )
    defaults = {
        "name": "",
        "pattern": "data/*.parquet",
        "template_name": "",
        "upstream_glob": "",
        "row_filter": "",
        "row_filter_template": "",
        "on_empty": "error",
    }
    if existing:
        for key in defaults:
            if key in existing and existing[key] is not None:
                defaults[key] = str(existing[key])
    values = prompt_form(
        "Add or edit file sources",
        help_text,
        [
            FieldSpec(
                name="name",
                label="Logical name",
                default=defaults["name"],
                placeholder="analytics",
                help_text="Used as the model/view name in dashboards.",
            ),
            FieldSpec(
                name="pattern",
                label="Static pattern under static/",
                default=defaults["pattern"],
                placeholder="data/*.parquet",
                help_text="Where built artifacts should live. Defaults to data/*.parquet.",
            ),
            FieldSpec(
                name="template_name",
                label="template_name (optional)",
                default=defaults["template_name"],
                placeholder="demo_[category]",
                optional=True,
                help_text="Fill when you want per-value views like demo_[category].",
            ),
            FieldSpec(
                name="upstream_glob",
                label="upstream_glob (optional)",
                default=defaults["upstream_glob"],
                placeholder="${DIR_FAKE}/demo-*.parquet",
                optional=True,
                help_text="Actual disk glob. Supports ${DIR_*} placeholders.",
            ),
            FieldSpec(
                name="row_filter",
                label="row_filter (optional)",
                default=defaults["row_filter"],
                placeholder="status = 'active'",
                optional=True,
                help_text="Global predicate applied to every read for this source.",
            ),
            FieldSpec(
                name="row_filter_template",
                label="row_filter_template (optional)",
                default=defaults["row_filter_template"],
                placeholder="category = ?",
                optional=True,
                help_text="Template predicate when template_name is used (use ? for value)",
            ),
            FieldSpec(
                name="on_empty",
                label="on_empty behaviour",
                choices=[("error", "error"), ("ignore", "ignore")],
                default=defaults["on_empty"],
                help_text="Decide whether missing upstream files stop the build.",
            ),
        ],
    )

    parts = [
        "[[file_sources]]",
        f"name = \"{values['name']}\"",
        f"pattern = \"{values['pattern']}\"",
    ]
    if values.get("template_name"):
        parts.append(f"template_name = \"{values['template_name']}\"")
    if values.get("upstream_glob"):
        parts.append(f"upstream_glob = \"{values['upstream_glob']}\"")
    if values.get("row_filter"):
        parts.append(f"row_filter = \"{values['row_filter']}\"")
    if values.get("row_filter_template"):
        parts.append(f"row_filter_template = \"{values['row_filter_template']}\"")
    if values.get("on_empty"):
        parts.append(f"on_empty = \"{values['on_empty']}\"")
    return "\n".join(parts) + "\n"


def _prompt_dir_fields(default_name: str = "", default_path: str = "") -> Dict[str, str]:
    return prompt_form(
        "Manage [dirs] constants",
        "Keep paths reusable by naming them DIR_* and pointing at project-relative locations.",
        [
            FieldSpec(
                name="name",
                label="Directory constant (DIR_*)",
                default=default_name,
                placeholder="DIR_FORMS",
                help_text="Names must begin with DIR_.",
            ),
            FieldSpec(
                name="path",
                label="Relative path or template",
                placeholder="static/forms",
                default=default_path,
                help_text="Supports ${DIR_*} placeholders to chain directories.",
            ),
        ],
    )


def handle(command: str, root: Path) -> None:
    cfg_path = root / "ducksite.toml"
    if not cfg_path.exists():
        print(f"Config file not found at {cfg_path}")
        return

    text = cfg_path.read_text(encoding="utf-8")

    try:
        updated: str = text
        data = tomllib.loads(text) if text.strip() else {}
        dirs = data.get("dirs", {}) or {}
        file_sources = data.get("file_sources", []) or []

        target_choice = prompt_form(
            "Pick what to manage",
            "You can edit directory constants for templating or file sources for upstream data.",
            [
                FieldSpec(
                    name="target",
                    label="Target",
                    choices=[("dir", "Directory constant [dirs]"), ("file", "File source [[file_sources]]")],
                    default="dir" if command != "remove" and not file_sources else "file",
                )
            ],
        )
        target = target_choice.get("target", "file")

        if target == "dir":
            if command == "add":
                while True:
                    values = _prompt_dir_fields(
                        "DIR_FORMS" if "DIR_FORMS" not in dirs else "",
                        "static/forms" if "DIR_FORMS" not in dirs else dirs.get("DIR_FORMS", ""),
                    )
                    try:
                        updated = add_dir_entry(text, values["name"], values["path"], root)
                        break
                    except Exception as exc:
                        print(f"Validation failed: {exc}")
                        continue
                cfg_path.write_text(updated, encoding="utf-8")
                print(f"Added directory constant {values['name']} to {cfg_path}")
            elif command == "modify":
                if not dirs:
                    raise ValueError("No directory constants available to modify")
                choices = [(name, name) for name in sorted(dirs)]
                picked = prompt_form(
                    "Choose DIR_* to update",
                    "Pick which reusable path to change before supplying its new value.",
                    [FieldSpec(name="name", label="Existing DIR_*", choices=choices, default=choices[0][0])],
                )
                while True:
                    values = _prompt_dir_fields(picked["name"], dirs.get(picked["name"], ""))
                    try:
                        updated = modify_dir_entry(text, values["name"], values["path"], root)
                        break
                    except Exception as exc:
                        print(f"Validation failed: {exc}")
                        continue
                cfg_path.write_text(updated, encoding="utf-8")
                print(f"Updated directory constant {values['name']} in {cfg_path}")
            elif command == "remove":
                if not dirs:
                    raise ValueError("No directory constants available to remove")
                choices = [(name, name) for name in sorted(dirs)]
                picked = prompt_form(
                    "Choose DIR_* to remove",
                    "Drop a directory constant that is no longer needed.",
                    [FieldSpec(name="name", label="Existing DIR_*", choices=choices, default=choices[0][0])],
                )
                updated = remove_dir_entry(text, picked["name"], root)
                cfg_path.write_text(updated, encoding="utf-8")
                print(f"Removed directory constant {picked['name']} from {cfg_path}")
            else:
                print(f"Unknown command '{command}' for TOML handler")
            return

        if target == "file":
            if command == "add":
                while True:
                    try:
                        updated = add_file_source_block(text, _prompt_file_source_block(), root)
                        break
                    except Exception as exc:
                        print(f"Validation failed: {exc}")
                        continue
                cfg_path.write_text(updated, encoding="utf-8")
                print(f"Added file source to {cfg_path}")
            elif command == "modify":
                if not file_sources:
                    raise ValueError("No file sources available to modify")
                names = [fs.get("name", "") for fs in file_sources if fs.get("name")]
                if not names:
                    raise ValueError("Existing file sources must have names")
                choices = [(name, name) for name in sorted(names)]
                picked = prompt_form(
                    "Choose file source",
                    "Select which source to edit, then update its pattern, upstream glob, or filters.",
                    [FieldSpec(name="name", label="Existing source", choices=choices, default=choices[0][0])],
                )
                selected = next((fs for fs in file_sources if fs.get("name") == picked["name"]), None)
                if selected is None:
                    raise ValueError("Could not find selected file source")
                while True:
                    try:
                        updated = modify_file_source_block(text, _prompt_file_source_block(selected), root)
                        break
                    except Exception as exc:
                        print(f"Validation failed: {exc}")
                        continue
                cfg_path.write_text(updated, encoding="utf-8")
                print(f"Modified file source in {cfg_path}")
            elif command == "remove":
                if not file_sources:
                    raise ValueError("No file sources available to remove")
                names = [fs.get("name", "") for fs in file_sources if fs.get("name")]
                if not names:
                    raise ValueError("Existing file sources must have names")
                choices = [(name, name) for name in sorted(names)]
                picked = prompt_form(
                    "Choose file source to remove",
                    "We will remove the matching [[file_sources]] block.",
                    [FieldSpec(name="name", label="Existing source", choices=choices, default=choices[0][0])],
                )
                updated = remove_file_source_block(text, picked["name"], root)
                cfg_path.write_text(updated, encoding="utf-8")
                print(f"Removed file source from {cfg_path}")
            else:
                print(f"Unknown command '{command}' for TOML handler")
    except KeyboardInterrupt:
        print("Cancelled")
    except Exception as exc:  # pragma: no cover - user facing
        print(f"Error: {exc}")
