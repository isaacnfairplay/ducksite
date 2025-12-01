from __future__ import annotations
from pathlib import Path

from .config import ProjectConfig


def create_app(cfg: ProjectConfig):
    """
    Create a minimal ASGI app that serves cfg.site_root as static content.

    Requires:
      - fastapi
      - uvicorn
    """
    try:
        from fastapi import FastAPI
        from fastapi.staticfiles import StaticFiles
    except ImportError as e:
        raise RuntimeError(
            "fast server backend requires 'fastapi' and 'uvicorn' to be installed.\n"
            "Install with: pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org fastapi uvicorn"
        ) from e

    app = FastAPI()

    # Serve everything under /, with index.html resolution.
    app.mount(
        "/",
        StaticFiles(directory=str(cfg.site_root), html=True),
        name="static",
    )
    return app


def serve_fast(cfg: ProjectConfig, port: int = 8080) -> None:
    """
    Run uvicorn against the static ASGI app.
    """
    try:
        import uvicorn
    except ImportError as e:
        raise RuntimeError(
            "fast server backend requires 'uvicorn' to be installed.\n"
            "Install with: pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org uvicorn"
        ) from e

    app = create_app(cfg)
    host = "0.0.0.0"
    print(f"[ducksite] serving {cfg.site_root} at http://localhost:{port}/ (uvicorn)")
    uvicorn.run(app, host=host, port=port, reload=False, access_log=False)
