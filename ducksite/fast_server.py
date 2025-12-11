from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, TYPE_CHECKING

from .config import ProjectConfig
from .forms import discover_forms, process_form_submission
from .auth import update_password

if TYPE_CHECKING:
    from fastapi import FastAPI, Request  # type: ignore[import-not-found]


def create_app(cfg: ProjectConfig) -> Any:
    """
    Create a minimal ASGI app that serves cfg.site_root as static content.

    Requires:
      - fastapi
      - uvicorn
    """
    try:
        from fastapi import FastAPI, Request
        from fastapi.staticfiles import StaticFiles  # type: ignore[import-not-found]
    except ImportError as e:
        raise RuntimeError(
            "fast server backend requires 'fastapi' and 'uvicorn' to be installed.\n"
            "Install with: pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org fastapi uvicorn"
        ) from e

    app: "FastAPI" = FastAPI()
    forms = discover_forms(cfg)

    @app.post("/api/forms/submit")  # type: ignore[misc]
    async def submit_form(request: "Request") -> Dict[str, Any]:
        ctype = request.headers.get("content-type", "")
        payload: Dict[str, Any] = {}
        files: Dict[str, bytes] = {}

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
        if not isinstance(form_id, str) or form_id not in forms:
            return {"error": "unknown form"}
        return process_form_submission(cfg, forms[form_id], payload, files or None)

    @app.post("/api/auth/update_password")  # type: ignore[misc]
    async def update_password_endpoint(request: "Request") -> Dict[str, Any]:
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        email = payload.get("email")
        old_password_raw = payload.get("old_password")
        new_password_raw = payload.get("new_password")
        old_password = str(old_password_raw) if old_password_raw is not None else ""
        new_password = str(new_password_raw) if new_password_raw is not None else ""
        if not isinstance(email, str):
            return {"error": "email required"}
        try:
            update_password(cfg, email, old_password, new_password)
            return {"status": "ok"}
        except ValueError as e:
            return {"error": str(e)}

    # Serve everything under /, with index.html resolution.
    app.mount(
        "/",
        StaticFiles(directory=str(cfg.site_root), html=True),
        name="static",
    )
    return app


def serve_fast(cfg: ProjectConfig, port: int = 8080, host: str = "127.0.0.1") -> None:
    """
    Run uvicorn against the static ASGI app.
    """
    try:
        import uvicorn  # type: ignore[import-not-found]
    except ImportError as e:
        raise RuntimeError(
            "fast server backend requires 'uvicorn' to be installed.\n"
            "Install with: pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org uvicorn"
        ) from e

    app = create_app(cfg)
    display_host = "localhost" if host in {"127.0.0.1", "::1", "localhost"} else host
    print(
        f"[ducksite] serving {cfg.site_root} at http://{display_host}:{port}/ (uvicorn)"
    )
    uvicorn.run(app, host=host, port=port, reload=False, access_log=False)
