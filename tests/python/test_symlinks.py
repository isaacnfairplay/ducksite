import os

from ducksite import symlinks
from ducksite.config import FileSourceConfig, ProjectConfig


def test_file_source_fingerprint_uses_cached_matches(monkeypatch, tmp_path):
    upstream_file = tmp_path / "data.parquet"
    upstream_file.write_text("data")

    fs = FileSourceConfig(name="demo", upstream_glob=str(upstream_file))
    cfg = ProjectConfig(root=tmp_path, dirs={}, file_sources=[fs])

    with os.scandir(tmp_path) as it:
        entries = [entry for entry in it if entry.name == upstream_file.name]

    cached_matches = {
        id(fs): {"pattern": str(upstream_file), "matches": entries, "error": False}
    }

    def _fail_glob(_pattern):
        raise AssertionError("glob should not be called when cached matches are provided")

    monkeypatch.setattr(symlinks, "_scandir_glob", _fail_glob)

    fingerprint = symlinks._file_source_fingerprints(cfg, cached_matches)

    assert fingerprint


def test_scandir_glob_is_not_recursive_without_double_star(tmp_path):
    root = tmp_path / "data"
    (root / "child").mkdir(parents=True)

    top_file = root / "top.parquet"
    nested_file = root / "child" / "nested.parquet"
    top_file.write_text("top")
    nested_file.write_text("nested")

    matches = symlinks._scandir_glob(str(root / "*.parquet"))

    matched_paths = {entry.path for entry in matches}

    assert str(top_file) in matched_paths
    assert str(nested_file) not in matched_paths


def test_scandir_glob_respects_double_star(tmp_path):
    root = tmp_path / "data"
    (root / "child" / "grandchild").mkdir(parents=True)

    nested_file = root / "child" / "grandchild" / "nested.parquet"
    nested_file.write_text("nested")

    matches = symlinks._scandir_glob(str(root / "**" / "*.parquet"))

    matched_paths = [entry.path for entry in matches]

    assert str(nested_file) in matched_paths
