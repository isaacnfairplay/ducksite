from __future__ import annotations
from pathlib import Path

from .builder import build_project
from .watcher import watch_and_build

__all__ = ["build_project", "watch_and_build"]


if __name__ == "__main__":
    build_project(Path(".").resolve())
