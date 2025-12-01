from __future__ import annotations
from pathlib import Path

from .utils import ensure_dir


def write_if_missing(path: Path, content: str) -> None:
    """
    Write `content` to `path` if the file does not already exist.

    This helper keeps demo initialisers idempotent so you can safely re-run
    `ducksite init --root demo` without clobbering manual edits.
    """
    if path.exists():
        print(f"[ducksite:init] {path} already exists, skipping.")
        return
    ensure_dir(path.parent)
    path.write_text(content, encoding="utf-8")
    print(f"[ducksite:init] wrote {path}")
