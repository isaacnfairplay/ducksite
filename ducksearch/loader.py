"""Utilities for validating and loading ducksearch project roots."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List


CACHE_SUBDIRS: tuple[str, ...] = (
    "artifacts",
    "slices",
    "materialize",
    "literal_sources",
    "bindings",
    "facets",
    "charts",
    "manifests",
    "tmp",
)


@dataclass(frozen=True)
class RootLayout:
    """Canonical layout for a ducksearch runtime root."""

    root: Path
    config: Path
    reports: Path
    composites: Path
    cache: Path

    @property
    def cache_children(self) -> List[Path]:
        return [self.cache / name for name in CACHE_SUBDIRS]


def validate_root(root: Path) -> RootLayout:
    """Validate the expected runtime root structure.

    A valid root contains ``config.toml``, ``reports/``, ``composites/``, and a
    ``cache/`` directory populated with the required subdirectories.
    """

    config = root / "config.toml"
    reports = root / "reports"
    composites = root / "composites"
    cache = root / "cache"

    missing: list[Path] = []
    if not config.exists() or not config.is_file():
        missing.append(config)

    for path in (reports, composites, cache):
        if not path.exists() or not path.is_dir():
            missing.append(path)

    if missing:
        missing_str = ", ".join(str(p) for p in missing)
        raise FileNotFoundError(f"Missing required paths: {missing_str}")

    child_missing: list[Path] = [p for p in _expected_cache_dirs(cache) if not p.exists() or not p.is_dir()]
    if child_missing:
        missing_str = ", ".join(str(p) for p in child_missing)
        raise FileNotFoundError(f"Missing required cache paths: {missing_str}")

    return RootLayout(root=root, config=config, reports=reports, composites=composites, cache=cache)


def _expected_cache_dirs(cache_root: Path) -> Iterable[Path]:
    for name in CACHE_SUBDIRS:
        yield cache_root / name
