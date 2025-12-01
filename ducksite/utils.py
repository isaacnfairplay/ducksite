from __future__ import annotations
import hashlib
from pathlib import Path
from typing import Iterable


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_list(items: Iterable[str]) -> str:
    h = hashlib.sha256()
    for it in sorted(items):
        h.update(it.encode("utf-8"))
    return h.hexdigest()


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    print("sha256_text('test') =", sha256_text("test"))
