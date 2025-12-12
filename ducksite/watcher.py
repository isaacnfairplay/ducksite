from __future__ import annotations

from pathlib import Path
from typing import Dict
import time
import os

from .builder import build_project


def _snapshot_paths(root: Path) -> Dict[Path, float]:
    watched: Dict[Path, float] = {}
    content = root / "content"
    sources_sql = root / "sources_sql"
    if content.exists():
        for p in content.rglob("*.md"):
            watched[p] = os.path.getmtime(p)
    if sources_sql.exists():
        for p in sources_sql.rglob("*.sql"):
            watched[p] = os.path.getmtime(p)
    toml = root / "ducksite.toml"
    if toml.exists():
        watched[toml] = os.path.getmtime(toml)
    return watched


def watch_and_build(root: Path, interval: float = 2.0, clean: bool = False) -> None:
    print("[ducksite] initial build")
    try:
        build_project(root, clean=clean)
    except FileNotFoundError as exc:
        print(f"[ducksite] watcher exiting: {exc}")
        return
    prev = _snapshot_paths(root)
    print("[ducksite] watching for changes... (Ctrl+C to stop)")
    while True:
        time.sleep(interval)
        cur = _snapshot_paths(root)
        changed = False
        if len(cur) != len(prev):
            changed = True
        else:
            for p, m in cur.items():
                if p not in prev or prev[p] != m:
                    changed = True
                    break
        if changed:
            print("[ducksite] change detected, rebuilding...")
            try:
                build_project(root)
            except FileNotFoundError as exc:
                print(f"[ducksite] watcher exiting: {exc}")
                return
            prev = cur


if __name__ == "__main__":
    watch_and_build(Path(".").resolve(), interval=2.0)
