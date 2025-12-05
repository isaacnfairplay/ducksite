from __future__ import annotations

from pathlib import Path

from .tuy_ui import FieldSpec, prompt_form
from .utils import ensure_dir
from .virtual_parquet import VirtualParquetManifest


def render_blank_plugin(name: str) -> str:
    safe_name = name.strip() or "plugin"
    return "\n".join(
        [
            "from ducksite.virtual_parquet import VirtualParquetManifest",
            "",
            "def build_manifest(cfg):",
            "    \"\"\"",
            f"    Skeleton virtual parquet plugin '{safe_name}'.",
            "",
            "    Fill this out to return VirtualParquetFile entries that map",
            "    HTTP paths to real parquet locations. Returning an empty",
            "    manifest keeps the build working until you add real files.",
            "    \"\"\"",
            "    return VirtualParquetManifest(files=[])",
            "",
            "",
        ]
    )


def write_blank_plugin(root: Path, name: str, directory: str = "plugins") -> Path:
    safe_name = name.strip() or "plugin"
    dest = (root / directory / f"{safe_name}.py").resolve()
    ensure_dir(dest.parent)
    if dest.exists():
        raise ValueError(f"Plugin file already exists: {dest}")
    dest.write_text(render_blank_plugin(safe_name), encoding="utf-8")
    return dest


def handle(command: str, root: Path) -> None:
    if command != "add":
        print(f"Unsupported command '{command}' for plugin handler; only 'add' is available.")
        return

    values = prompt_form(
        "Create a virtual parquet plugin",
        "Name the plugin file to generate under your project root.",
        [
            FieldSpec(
                name="name",
                label="Plugin name (no extension)",
                placeholder="example_plugin",
            ),
            FieldSpec(
                name="directory",
                label="Directory for plugin",
                default="plugins",
                placeholder="plugins",
                optional=True,
            ),
        ],
    )

    try:
        path = write_blank_plugin(root, values.get("name", "plugin"), values.get("directory") or "plugins")
    except Exception as exc:
        print(f"Error: {exc}")
        return

    rel = path.relative_to(root)
    print(f"Wrote blank plugin scaffold at {rel}")
