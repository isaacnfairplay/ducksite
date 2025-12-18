"""Minimal HTTP server for ducksearch reports and cache."""
from __future__ import annotations

import json
from html import escape
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
            if self._wants_html(params):
                self._html_response(400, _render_error_html("missing report parameter"))
            else:
                self._json_response(400, {"error": "missing report parameter"})
            return
        report_rel = Path(unquote(rel[0]))
        report_path = (self.layout.reports / report_rel).resolve()
        try:
            report_path.relative_to(self.layout.reports)
        except ValueError:
            if self._wants_html(params):
                self._html_response(400, _render_error_html("invalid report path"))
            else:
                self._json_response(400, {"error": "invalid report path"})
            return
        try:
            result = execute_report(self.layout.root, report_path, payload=params)
        except (FileNotFoundError, ExecutionError) as exc:
            if self._wants_html(params):
                self._html_response(400, _render_error_html(str(exc)))
            else:
                self._json_response(400, {"error": str(exc)})
            return
        payload = result.as_payload(self.layout.root)
        if self._wants_html(params):
            html = _render_report_html(report_rel, payload, params)
            self._html_response(200, html)
        else:
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

    def _html_response(self, status: int, html: str) -> None:
        body = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _wants_html(self, params: dict[str, list[str]]) -> bool:
        fmt = params.get("format", [])
        if fmt and str(fmt[0]).lower() == "html":
            return True
        accept = self.headers.get("Accept", "")
        return "text/html" in accept.lower()


def run_server(layout: RootLayout, host: str, port: int, *, dev: bool = False, workers: int = 1) -> None:
    """Start an HTTP server bound to ``host``/``port`` for ducksearch."""

    _ = dev, workers  # currently unused but accepted for compatibility

    def handler(*args, **kwargs):  # type: ignore[override]
        return _DucksearchHandler(*args, layout=layout, **kwargs)

    with ThreadingHTTPServer((host, port), handler) as httpd:
        httpd.serve_forever()


def _render_report_html(report_rel: Path, payload: dict, params: dict[str, list[str]]) -> str:
    base_rel = escape(str(payload.get("base_parquet", "")))
    params_list = [
        f"<li><code>{escape(k)}</code> = <code>{escape(','.join(v))}</code></li>" for k, v in sorted(params.items())
    ]
    mats_list = "".join(
        f"<li><a href='/{escape(path)}'>{escape(name)}</a></li>"
        for name, path in sorted((payload.get("materialize") or {}).items())
    )
    literal_list = "".join(
        f"<li><a href='/{escape(path)}'>{escape(name)}</a></li>"
        for name, path in sorted((payload.get("literal_sources") or {}).items())
    )
    bindings_list = "".join(
        f"<li><a href='/{escape(path)}'>{escape(name)}</a></li>"
        for name, path in sorted((payload.get("bindings") or {}).items())
    )
    payload_json = json.dumps(payload).replace("</", "<\\/")  # prevent script break-out
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>ducksearch report: {escape(str(report_rel))}</title>
    <style>
      body {{ font-family: sans-serif; margin: 2rem; color: #222; }}
      code {{ background: #f5f5f5; padding: 0.1rem 0.3rem; border-radius: 4px; }}
      .chips > li {{ display: inline-block; margin-right: 0.75rem; }}
      .section {{ margin-bottom: 1.75rem; }}
      .table-wrap {{ overflow: auto; border: 1px solid #ddd; border-radius: 6px; padding: 0.5rem; }}
      table {{ border-collapse: collapse; min-width: 400px; }}
      th, td {{ border: 1px solid #ccc; padding: 0.25rem 0.5rem; text-align: left; }}
      th {{ background: #f0f0f0; }}
      #preview-status {{ margin-bottom: 0.5rem; font-weight: 600; }}
    </style>
  </head>
  <body>
    <h1>ducksearch HTML preview</h1>
    <div class="section">
      <p><strong>Report:</strong> <code>{escape(str(report_rel))}</code></p>
      <p><strong>Base Parquet:</strong> <a id="base-link" href="/{base_rel}">/{base_rel}</a></p>
    </div>
    <div class="section">
      <h2>Parameters</h2>
      <ul class="chips">
        {''.join(params_list) or '<li><em>none provided</em></li>'}
      </ul>
    </div>
    <div class="section">
      <h2>Materializations</h2>
      <ul>{mats_list or '<li><em>none</em></li>'}</ul>
      <h3>Literal sources</h3>
      <ul>{literal_list or '<li><em>none</em></li>'}</ul>
      <h3>Bindings</h3>
      <ul>{bindings_list or '<li><em>none</em></li>'}</ul>
    </div>
    <div class="section">
      <h2>Preview (DuckDB-Wasm)</h2>
      <div id="preview-status">Loading DuckDB-Wasm…</div>
      <label for="preview-search"><strong>Search rows:</strong></label>
      <input id="preview-search" type="search" placeholder="Filter values (case-insensitive)" />
      <div class="table-wrap">
        <table id="preview-table"></table>
      </div>
    </div>

    <script type="application/json" id="report-payload">{payload_json}</script>
    <script type="module">
      import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.30.0/+esm";

      const payload = JSON.parse(document.getElementById("report-payload").textContent);
      const baseParquet = payload.base_parquet
        ? new URL(payload.base_parquet, window.location.origin).toString()
        : null;

      const statusEl = document.getElementById("preview-status");
      const tableEl = document.getElementById("preview-table");
      const searchEl = document.getElementById("preview-search");

      if (!baseParquet) {{
        statusEl.textContent = "No base_parquet path found in payload.";
      }} else {{
        statusEl.textContent = "Starting DuckDB-Wasm…";
        renderPreview().catch((err) => {{
          console.error("[ducksearch] preview error", err);
          statusEl.textContent = "Preview failed: " + err;
        }});
      }}

      async function renderPreview() {{
        const bundles = duckdb.getJsDelivrBundles();
        const bundle = await duckdb.selectBundle(bundles);
        const workerUrl = URL.createObjectURL(
          new Blob([`importScripts("${{bundle.mainWorker}}");`], {{ type: "text/javascript" }})
        );
        try {{
          const worker = new Worker(workerUrl);
          const logger = new duckdb.ConsoleLogger();
          const db = new duckdb.AsyncDuckDB(logger, worker);
          await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
          const conn = await db.connect();
          statusEl.textContent = "Querying preview slice…";
          const safePath = baseParquet.replaceAll("'", "''");
          const result = await conn.query(`select * from read_parquet('${{safePath}}') limit 200`);
          const rows = result.toArray();
          attachSearch(rows);
          renderTable(rows);
          statusEl.textContent = "Showing up to 200 rows from base_parquet";
        }} finally {{
          URL.revokeObjectURL(workerUrl);
        }}
      }}

      function attachSearch(rows) {{
        if (!searchEl) return;
        searchEl.addEventListener("input", () => {{
          const term = (searchEl.value || "").toLowerCase();
          if (!term) {{
            renderTable(rows);
            return;
          }}
          const filtered = rows.filter((row) =>
            Object.values(row).some((val) => String(val ?? "").toLowerCase().includes(term))
          );
          renderTable(filtered);
          statusEl.textContent = filtered.length
            ? `Filtered to ${{filtered.length}} rows (preview capped at 200)`
            : "No rows matched your search";
        }});
      }}

      function renderTable(rows) {{
        if (!rows.length) {{
          tableEl.innerHTML = "<tr><td><em>No rows returned</em></td></tr>";
          return;
        }}
        const headers = Object.keys(rows[0]);
        tableEl.innerHTML = "";
        const thead = document.createElement("thead");
        const headRow = document.createElement("tr");
        headers.forEach((h) => {{
          const th = document.createElement("th");
          th.textContent = h;
          headRow.appendChild(th);
        }});
        thead.appendChild(headRow);
        tableEl.appendChild(thead);

        const tbody = document.createElement("tbody");
        rows.forEach((row) => {{
          const tr = document.createElement("tr");
          headers.forEach((h) => {{
            const td = document.createElement("td");
            td.textContent = row[h];
            tr.appendChild(td);
          }});
          tbody.appendChild(tr);
        }});
        tableEl.appendChild(tbody);
      }}
    </script>
  </body>
</html>
"""


def _render_error_html(message: str) -> str:
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>ducksearch error</title>
  </head>
  <body>
    <h1>ducksearch error</h1>
    <p>{escape(message)}</p>
    <p>Append <code>format=html</code> to retry with HTML output.</p>
  </body>
</html>"""
