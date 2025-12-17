"""Minimal HTTP server for ducksearch reports and cache."""
from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from ducksearch.loader import RootLayout
from ducksearch.runtime import ExecutionError, execute_report


class _DucksearchHandler(BaseHTTPRequestHandler):
    server_version = "ducksearch"

    def __init__(self, *args, layout: RootLayout, **kwargs):
        self.layout = layout
        super().__init__(*args, **kwargs)

    def do_GET(self) -> None:  # noqa: N802 - required by BaseHTTPRequestHandler
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._json_response(200, {"status": "ok"})
            return
        if parsed.path == "/report":
            self._handle_report(parsed)
            return
        if parsed.path.startswith("/cache/"):
            self._handle_cache_request(parsed.path)
            return
        self.send_error(404, "Not Found")

    def log_message(self, format: str, *args) -> None:  # noqa: A003 - inherited name
        return

    def _handle_report(self, parsed) -> None:
        params = parse_qs(parsed.query)
        rel = params.get("report", [])
        if not rel:
            self._json_response(400, {"error": "missing report parameter"})
            return
        report_rel = Path(unquote(rel[0]))
        report_path = (self.layout.reports / report_rel).resolve()
        try:
            report_path.relative_to(self.layout.reports)
        except ValueError:
            self._json_response(400, {"error": "invalid report path"})
            return
        try:
            result = execute_report(self.layout.root, report_path, payload=params)
        except (FileNotFoundError, ExecutionError) as exc:
            self._json_response(400, {"error": str(exc)})
            return
        payload = result.as_payload(self.layout.root)
        self._json_response(200, payload)

    def _handle_cache_request(self, path: str) -> None:
        rel = Path(unquote(path.lstrip("/")))
        target = (self.layout.root / rel).resolve()
        try:
            target.relative_to(self.layout.cache)
        except ValueError:
            self.send_error(404, "Not Found")
            return
        if not target.exists():
            self.send_error(404, "Not Found")
            return
        data = target.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _json_response(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def run_server(layout: RootLayout, host: str, port: int, *, dev: bool = False, workers: int = 1) -> None:
    """Start an HTTP server bound to ``host``/``port`` for ducksearch."""

    _ = dev, workers  # currently unused but accepted for compatibility

    def handler(*args, **kwargs):  # type: ignore[override]
        return _DucksearchHandler(*args, layout=layout, **kwargs)

    with ThreadingHTTPServer((host, port), handler) as httpd:
        httpd.serve_forever()
