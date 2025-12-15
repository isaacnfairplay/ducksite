"""Server-facing helpers for ducksearch."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

from ducksearch.runtime import ExecutionError, ExecutionResult


@dataclass(frozen=True)
class ErrorPayload:
    code: str
    message: str

    def as_dict(self) -> Dict[str, str]:
        return {"code": self.code, "message": self.message}


def build_response(result: ExecutionResult | None = None, error: Exception | None = None, *, cache_root: str | None = None) -> Dict[str, Any]:
    """Construct a JSON-safe response consumed by the frontend."""

    if error:
        if isinstance(error, ExecutionError):
            payload = ErrorPayload(code="runtime_error", message=str(error))
        else:
            payload = ErrorPayload(code="unexpected", message="request failed")
        return {"ok": False, "error": payload.as_dict()}

    assert result is not None, "result is required when no error is provided"
    root = cache_root if cache_root is not None else "/"
    body: Dict[str, Any] = {"ok": True, "cache_root": root}
    body.update(result.as_payload(Path(root)))
    return body
