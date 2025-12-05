"""Ad-hoc probe to time demo chart loads with caching effects."""
from __future__ import annotations

import contextlib
import http.client
import json
import socket
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import duckdb

sys.path.append(str(Path(__file__).resolve().parent.parent))

from ducksite import demo_init_fake_parquet, js_assets
from ducksite.builder import build_project, serve_project
from ducksite.init_project import init_demo_project
from ducksite.sternum import AssetPath, Scheme


@dataclass
class RequestTiming:
    path: str
    status: int
    duration_ms: float
    size: int
    last_modified: Optional[str]


def _stub_echarts() -> None:
    def fake_download(url: str, dest: Path) -> None:  # noqa: ARG001
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text("// stub echarts", encoding="utf-8")

    js_assets._download_with_ssl_bypass = fake_download


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("", 0))
        return int(sock.getsockname()[1])


def _start_server(root: Path, port: int) -> None:
    thread = threading.Thread(target=serve_project, args=(root, port, "builtin"), daemon=True)
    thread.start()
    time.sleep(1.0)


def _time_request(path: str, port: int, headers: Optional[Dict[str, str]] = None) -> RequestTiming:
    conn = http.client.HTTPConnection("localhost", port, timeout=10)
    start = time.perf_counter()
    merged_headers = {"Accept-Encoding": "gzip"}
    if headers:
        merged_headers.update(headers)
    conn.request("GET", path, headers=merged_headers)
    resp = conn.getresponse()
    body = resp.read()
    duration_ms = (time.perf_counter() - start) * 1000
    timing = RequestTiming(
        path=path,
        status=resp.status,
        duration_ms=duration_ms,
        size=len(body),
        last_modified=resp.getheader("Last-Modified"),
    )
    conn.close()
    return timing


def _connect_httpfs_or_local(base_url: str):
    con = duckdb.connect()
    try:
        con.execute("LOAD httpfs")
        con.execute("SET enable_http_metadata_cache=true")
        use_httpfs = True
    except duckdb.Error:
        use_httpfs = False
    return con, use_httpfs


def _time_query(con: duckdb.DuckDBPyConnection, url: str, label: str) -> Dict[str, object]:
    start = time.perf_counter()
    con.execute(f"SELECT sum(value) FROM read_parquet('{url}')")
    con.fetchall()
    duration_ms = (time.perf_counter() - start) * 1000
    return {"label": label, "duration_ms": duration_ms}


def run_probe() -> Dict[str, object]:
    _stub_echarts()
    demo_init_fake_parquet._download_nytaxi_parquet = lambda dest: False

    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        init_demo_project(root)
        build_project(root)
        port = _find_free_port()
        _start_server(root, port)

        asset_paths = [
            AssetPath.INDEX.value,
            AssetPath.CONTRACT_JS.value,
            AssetPath.ECHARTS_JS.value,
            AssetPath.DUCKDB_BUNDLE_JS.value,
        ]
        first = [_time_request(path, port) for path in asset_paths]
        cached_conditional = [
            _time_request(
                path,
                port,
                {"If-Modified-Since": timing.last_modified} if timing.last_modified else None,
            )
            for timing, path in zip(first, asset_paths)
        ]
        cached_unconditional = [_time_request(path, port) for path in asset_paths]

        base_url = f"{Scheme.HTTP.value}://localhost:{port}{AssetPath.DEMO_DATA.value}"
        con, httpfs_loaded = _connect_httpfs_or_local(base_url)
        if not httpfs_loaded:
            base_url = str((root / "fake_upstream").resolve())

        query_labels = []
        for name in ("demo-A", "demo-B", "demo-C"):
            label = f"{name} query"
            query_labels.append(
                _time_query(con, f"{base_url}/{name}.parquet", label),
            )
        con.close()

        initial_ops: List[Dict[str, object]] = [
            {"label": t.path, "duration_ms": t.duration_ms} for t in first
        ] + query_labels
        cached_ops: List[Dict[str, object]] = [
            {"label": t.path, "duration_ms": t.duration_ms} for t in cached_conditional
        ] + query_labels

        unconditional_ops: List[Dict[str, object]] = [
            {"label": t.path, "duration_ms": t.duration_ms} for t in cached_unconditional
        ] + query_labels

        def pareto(entries: List[Dict[str, object]]) -> List[Dict[str, object]]:
            return sorted(entries, key=lambda item: item["duration_ms"], reverse=True)

        return {
            "httpfs_loaded": httpfs_loaded,
            "initial": pareto(initial_ops),
            "cached": pareto(cached_ops),
            "cached_unconditional": pareto(unconditional_ops),
        }


if __name__ == "__main__":
    report = run_probe()
    print(json.dumps(report, indent=2))
