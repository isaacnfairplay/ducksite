from __future__ import annotations
import json
from pathlib import Path

from .config import ProjectConfig
from .forms import discover_forms, process_form_submission


def create_app(cfg: ProjectConfig):
    """
    Create a minimal ASGI app that serves cfg.site_root as static content.

    Requires:
      - fastapi
      - uvicorn
    """
    try:
        from fastapi import FastAPI, Request
        from fastapi.staticfiles import StaticFiles
    except ImportError as e:
        raise RuntimeError(
            "fast server backend requires 'fastapi' and 'uvicorn' to be installed.\n"
            "Install with: pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org fastapi uvicorn"
        ) from e

    app = FastAPI()
    forms = discover_forms(cfg)

    @app.post("/api/forms/submit")
    async def submit_form(request: Request):
        ctype = request.headers.get("content-type", "")
        payload: dict = {}
        files: dict = {}

        if ctype.startswith("multipart/form-data"):
            form = await request.form()
            for key, value in form.multi_items():
                if hasattr(value, "read"):
                    files[key] = await value.read()
                else:
                    try:
                        payload[key] = json.loads(value)
                    except Exception:
                        payload[key] = value
        else:
            try:
                payload = await request.json()
            except Exception:
                payload = {}

        form_id = payload.get("form_id")
        if not form_id or form_id not in forms:
            return {"error": "unknown form"}
        return process_form_submission(cfg, forms[form_id], payload, files or None)

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
