import runpy
from pathlib import Path

from ducksearch import cli
from ducksearch.loader import validate_root


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


def test_serve_validates_root(capsys, tmp_path: Path, monkeypatch):
    root = _make_minimal_root(tmp_path)
    monkeypatch.setattr(cli, "run_server", lambda *_, **__: None)
    cli.main(["serve", "--root", str(root)])
    captured = capsys.readouterr()
    assert "ready on" in captured.out


def test_lint_validates_reports(capsys, tmp_path: Path):
    root = _make_minimal_root(tmp_path)
    cli.main(["lint", "--root", str(root)])
    captured = capsys.readouterr()
    assert "lint passed" in captured.out


def test_serve_accepts_workers_and_dev(capsys, tmp_path: Path, monkeypatch):
    root = _make_minimal_root(tmp_path)
    called = {}

    def fake_run_server(layout, host, port, *, dev, workers):
        called.update(
            {
                "layout": layout,
                "host": host,
                "port": port,
                "dev": dev,
                "workers": workers,
            }
        )

    monkeypatch.setattr(cli, "run_server", fake_run_server)
    cli.main(
        [
            "serve",
            "--root",
            str(root),
            "--host",
            "0.0.0.0",
            "--port",
            "9090",
            "--workers",
            "2",
            "--dev",
        ]
    )
    captured = capsys.readouterr()
    assert "workers=2" in captured.out
    assert called == {
        "layout": validate_root(root),
        "host": "0.0.0.0",
        "port": 9090,
        "dev": True,
        "workers": 2,
    }


def test_module_entrypoint_invokes_cli(monkeypatch):
    called = {}

    def fake_main(argv=None):  # noqa: ARG001 - signature matches cli.main
        called["argv"] = argv

    monkeypatch.setattr(cli, "main", fake_main)

    runpy.run_module("ducksearch", run_name="__main__", alter_sys=True)

    assert called == {"argv": None}
