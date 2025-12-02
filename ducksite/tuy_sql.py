from __future__ import annotations

from pathlib import Path


def handle(command: str, root: Path) -> None:
    print(f"Handling {command} for SQL resources in {root}")
