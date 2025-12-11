from ducksite import symlinks
from ducksite.config import FileSourceConfig, ProjectConfig


def test_file_source_fingerprint_uses_cached_matches(monkeypatch, tmp_path):
    upstream_file = tmp_path / "data.parquet"
    upstream_file.write_text("data")

    fs = FileSourceConfig(name="demo", upstream_glob=str(upstream_file))
    cfg = ProjectConfig(root=tmp_path, dirs={}, file_sources=[fs])

    cached_matches = {
        id(fs): {"pattern": str(upstream_file), "matches": [str(upstream_file)], "error": False}
    }

    def _fail_glob(_pattern):
        raise AssertionError("glob should not be called when cached matches are provided")

    monkeypatch.setattr(symlinks.glob, "glob", _fail_glob)

    fingerprint = symlinks._file_source_fingerprint(cfg, cached_matches)

    assert fingerprint
