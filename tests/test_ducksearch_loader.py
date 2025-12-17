from pathlib import Path

import pytest

from ducksearch.loader import CACHE_SUBDIRS, validate_root


def _base_root(tmp_path: Path) -> None:
    (tmp_path / "config.toml").write_text("name='demo'\n")
    (tmp_path / "reports").mkdir(parents=True, exist_ok=True)
    (tmp_path / "composites").mkdir(parents=True, exist_ok=True)
    for child in CACHE_SUBDIRS:
        (tmp_path / "cache" / child).mkdir(parents=True, exist_ok=True)


def test_validate_root_rejects_file_instead_of_reports(tmp_path: Path):
    _base_root(tmp_path)
    reports_path = tmp_path / "reports"
    reports_path.rmdir()
    reports_path.write_text("not a dir")

    with pytest.raises(FileNotFoundError):
        validate_root(tmp_path)


def test_validate_root_rejects_file_in_cache_child(tmp_path: Path):
    _base_root(tmp_path)
    cache_child = tmp_path / "cache" / CACHE_SUBDIRS[0]
    cache_child.rmdir()
    cache_child.write_text("not a dir")

    with pytest.raises(FileNotFoundError):
        validate_root(tmp_path)
