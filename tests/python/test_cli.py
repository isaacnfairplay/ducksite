from __future__ import annotations

import sys

import pytest

from ducksite import cli, tuy_toml


def test_dispatches_to_resource_handler(monkeypatch, tmp_path):
    called = {}

    def fake_handle(command: str, root):
        called["args"] = (command, root)

    monkeypatch.setattr(tuy_toml, "handle", fake_handle)
    monkeypatch.setattr(sys, "argv", ["ducksite", "add", "toml", "--root", str(tmp_path)])

    cli.main()

    assert called["args"][0] == "add"
    assert called["args"][1] == tmp_path.resolve()


def test_missing_resource_shows_usage(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["ducksite", "add"])

    with pytest.raises(SystemExit) as exc:
        cli.main()

    assert exc.value.code != 0
    err = capsys.readouterr().err
    assert "usage:" in err.lower()


def test_invalid_resource_type(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["ducksite", "modify", "yaml"])

    with pytest.raises(SystemExit) as exc:
        cli.main()

    assert exc.value.code != 0
    err = capsys.readouterr().err
    assert "usage:" in err.lower()
    assert "yaml" in err
