from pathlib import Path

from ducksearch import cli


def _make_minimal_root(tmp_path: Path) -> Path:
    (tmp_path / "config.toml").write_text("name='demo'\n")
    for name in [
        "reports",
        "composites",
        "cache/artifacts",
        "cache/slices",
        "cache/materialize",
        "cache/literal_sources",
        "cache/bindings",
        "cache/facets",
        "cache/charts",
        "cache/manifests",
        "cache/tmp",
    ]:
        (tmp_path / name).mkdir(parents=True, exist_ok=True)

    report = tmp_path / "reports/demo/example.sql"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text("SELECT 1;\n")
    return tmp_path


def test_serve_validates_root(capsys, tmp_path: Path):
    root = _make_minimal_root(tmp_path)
    cli.main(["serve", "--root", str(root)])
    captured = capsys.readouterr()
    assert "ready on" in captured.out


def test_lint_validates_reports(capsys, tmp_path: Path):
    root = _make_minimal_root(tmp_path)
    cli.main(["lint", "--root", str(root)])
    captured = capsys.readouterr()
    assert "lint passed" in captured.out
