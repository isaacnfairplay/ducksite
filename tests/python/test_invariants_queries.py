from ducksite.config import ProjectConfig, FileSourceConfig
from ducksite.queries import build_file_source_queries


def test_build_file_source_queries_basic(tmp_path):
    site_root = tmp_path / "static"
    data = site_root / "data" / "demo"
    data.mkdir(parents=True)
    (data / "demo-A.parquet").write_text("x", encoding="utf-8")

    cfg = ProjectConfig(
        root=tmp_path,
        dirs={},
        file_sources=[FileSourceConfig(name="demo", pattern="data/demo/*.parquet")],
    )
    cfg.site_root = site_root

    queries = build_file_source_queries(cfg)
    assert "demo" in queries
    sql = queries["demo"].sql
    assert "read_parquet" in sql
    assert "data/demo/demo-A.parquet" in sql
