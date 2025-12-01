import builtins
from pathlib import Path

from ducksite.config import ProjectConfig
from ducksite.fast_server import create_app


def test_create_app_raises_when_fastapi_missing(monkeypatch) -> None:
    original_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name.startswith("fastapi") or name.startswith("uvicorn"):
            raise ImportError("fastapi not installed")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    cfg = ProjectConfig(root=Path("."), dirs={})
    try:
        create_app(cfg)
    except RuntimeError as exc:
        assert "fast server backend requires 'fastapi'" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError when fastapi missing")
