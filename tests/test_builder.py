from __future__ import annotations

import email.utils
import gzip
import http.client
import http.server
import json
import socket
import sys
import threading
import time
import json
from http import HTTPStatus
from pathlib import Path

import duckdb
import pytest

sys.path.append(str(Path(__file__).resolve().parent.parent))

import ducksite.builder as builder
from ducksite import demo_init_fake_parquet, js_assets
from ducksite.builder import _clean_site, build_project, serve_project
from ducksite.config import FileSourceConfig, ProjectConfig
from ducksite.data_map_cache import load_data_map, load_fingerprints
from ducksite.data_map_paths import data_map_dir, data_map_sqlite_path
from ducksite.init_project import init_demo_project, init_project
from ducksite.sternum import AssetPath, Scheme
from ducksite.symlinks import build_symlinks


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return int(s.getsockname()[1])


def _start_server(root: Path, port: int, monkeypatch: pytest.MonkeyPatch, request_log: list) -> None:
    def log_message(self: http.server.SimpleHTTPRequestHandler, format: str, *args: object) -> None:  # type: ignore[override]
        request_log.append(
            {
                "method": self.command,
                "path": self.path,
                "range": self.headers.get("Range"),
                "if_modified_since": self.headers.get("If-Modified-Since"),
            }
        )

    monkeypatch.setattr(http.server.SimpleHTTPRequestHandler, "log_message", log_message)

    t = threading.Thread(target=serve_project, args=(root, port, "builtin"), daemon=True)
    t.start()
    time.sleep(1.0)


def _stub_echarts(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_download(url: str, dest: Path) -> None:  # noqa: ARG001
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text("// stub echarts", encoding="utf-8")

    monkeypatch.setattr(js_assets, "_download_with_ssl_bypass", fake_download)


def _connect_with_httpfs_or_skip():
    con = duckdb.connect()
    try:
        try:
            con.execute("LOAD httpfs")
        except duckdb.IOException:
            con.execute("INSTALL httpfs")
            con.execute("LOAD httpfs")
    except duckdb.IOException:
        con.close()
        pytest.skip("httpfs extension unavailable in offline test environment")
    return con


@pytest.fixture
def demo_root(tmp_path: Path) -> Path:
    """Create a temporary ducksite project root with minimal config."""
    cfg = """
[dirs]
"""
    (tmp_path / "ducksite.toml").write_text(cfg, encoding="utf-8")
    return tmp_path


def test_clean_preserves_data_maps(tmp_path: Path) -> None:
    site_root = tmp_path / "static"
    site_root.mkdir(parents=True, exist_ok=True)

    private_root = data_map_dir(site_root)
    private_root.mkdir(parents=True, exist_ok=True)

    preserved = {
        "data_map.json": "{}",
        "data_map.sqlite": "sqlite stub",
        "data_map_meta.json": "{}",
    }

    for name, content in preserved.items():
        (private_root / name).write_text(content, encoding="utf-8")

    stale_file = site_root / "old.txt"
    stale_file.write_text("old", encoding="utf-8")
    stale_dir = site_root / "nested"
    stale_dir.mkdir()
    (stale_dir / "file.txt").write_text("remove", encoding="utf-8")

    _clean_site(site_root)

    assert site_root.exists()
    for name, content in preserved.items():
        path = private_root / name
        assert path.exists()
        assert path.read_text(encoding="utf-8") == content

    assert not stale_file.exists()
    assert not stale_dir.exists()


def test_serve_project_respects_host(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _stub_echarts(monkeypatch)
    init_project(tmp_path)

    monkeypatch.setattr("ducksite.watcher.watch_and_build", lambda *_, **__: None)

    bound: dict[str, object] = {}

    class FakeServer:
        def __init__(self, address, handler):  # type: ignore[no-untyped-def]
            bound["address"] = address
            bound["handler"] = handler

        def __enter__(self):  # type: ignore[override]
            return self

        def __exit__(self, exc_type, exc, tb):  # type: ignore[override]
            return False

        def serve_forever(self):  # type: ignore[override]
            bound["served"] = True

    monkeypatch.setattr(builder.http.server, "ThreadingHTTPServer", FakeServer)

    serve_project(tmp_path, port=8123, backend="builtin", host="0.0.0.0")

    assert bound["address"] == ("0.0.0.0", 8123)
    assert bound.get("served") is True


def test_build_does_not_clean_by_default(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _stub_echarts(monkeypatch)
    init_project(tmp_path)

    content = tmp_path / "content"
    content.mkdir(parents=True, exist_ok=True)
    (content / "index.md").write_text("hello", encoding="utf-8")

    keep = tmp_path / "static" / "keep.txt"
    keep.parent.mkdir(parents=True, exist_ok=True)
    keep.write_text("keep me", encoding="utf-8")

    build_project(tmp_path)

    assert keep.exists()
    assert keep.read_text(encoding="utf-8") == "keep me"


def test_build_cleans_when_requested(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _stub_echarts(monkeypatch)
    init_project(tmp_path)

    content = tmp_path / "content"
    content.mkdir(parents=True, exist_ok=True)
    (content / "index.md").write_text("hello", encoding="utf-8")

    stale = tmp_path / "static" / "stale.txt"
    stale.parent.mkdir(parents=True, exist_ok=True)
    stale.write_text("remove me", encoding="utf-8")

    build_project(tmp_path, clean=True)

    assert not stale.exists()
    assert (tmp_path / "static" / "index.html").exists()


def test_build_removes_stale_pages(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _stub_echarts(monkeypatch)
    init_project(tmp_path)

    content = tmp_path / "content"
    content.mkdir(parents=True, exist_ok=True)

    page = content / "foo.md"
    page.write_text("# Hello", encoding="utf-8")

    build_project(tmp_path)

    html_out = tmp_path / "static" / "foo.html"
    assert html_out.exists()

    page.unlink()

    build_project(tmp_path)

    assert not html_out.exists()


def test_build_project_generates_site_and_configs(monkeypatch: pytest.MonkeyPatch, demo_root: Path) -> None:
    def fake_download(url: str, dest: Path) -> None:  # noqa: ARG001
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text("// stub echarts", encoding="utf-8")

    monkeypatch.setattr(js_assets, "_download_with_ssl_bypass", fake_download)

    content = demo_root / "content"
    (content / "section").mkdir(parents=True, exist_ok=True)

    (content / "index.md").write_text(
        """
# Welcome

Intro copy for the homepage.

```sql page_query
SELECT 1 AS value
```

```echart chart1
title: Demo Chart
```

```input country
label: Country
```

```grid cols=2 gap=lg
| chart1 | table1 |
| .      | table1:2 |
```
""",
        encoding="utf-8",
    )

    (content / "section" / "index.md").write_text("Section content", encoding="utf-8")

    build_project(demo_root)

    site_root = demo_root / "static"
    index_html = site_root / "index.html"
    section_html = site_root / "section" / "index.html"

    assert index_html.exists()
    assert section_html.exists()

    html_text = index_html.read_text(encoding="utf-8")
    assert "(current: /index.html)" in html_text
    assert "/section/index.html" in html_text
    assert "page-config-json" in html_text

    section_text = section_html.read_text(encoding="utf-8")
    assert "(current: /section/index.html)" in section_text

    sitemap = json.loads((site_root / "sitemap.json").read_text(encoding="utf-8"))
    assert sitemap["routes"] == [AssetPath.INDEX.value, f"/section{AssetPath.INDEX.value}"]

    data_map = load_data_map(site_root)
    assert data_map == {}

    sql_path = site_root / "sql" / "page_query.sql"
    assert sql_path.exists()
    sql_text = sql_path.read_text(encoding="utf-8")
    assert sql_text.startswith("-- METRICS:")
    assert "SELECT 1 AS value" in sql_text

    global_sql = site_root / "sql" / "_global" / "page_query.sql"
    assert global_sql.exists()
    global_sql_text = global_sql.read_text(encoding="utf-8")
    assert not global_sql_text.startswith("-- METRICS:")
    assert "SELECT 1 AS value" in global_sql_text

    manifest = json.loads((site_root / "sql" / "_manifest.json").read_text(encoding="utf-8"))
    views = manifest.get("views", {})
    assert views.get("page_query", {}).get("num_files") == 0
    assert views.get("page_query", {}).get("sql_path") == "/sql/_global/page_query.sql"

    contract = (site_root / "js" / "ducksite_contract.js").read_text(encoding="utf-8")
    assert "layoutGrid" in contract
    assert "pageConfigJson" in contract
    assert (site_root / "js" / "echarts.min.js").exists()


def test_symlinks_map_upstream_files(tmp_path: Path) -> None:
    upstream = tmp_path / "upstream"
    (upstream / "cat1").mkdir(parents=True, exist_ok=True)
    (upstream / "cat1" / "first.parquet").write_text("demo", encoding="utf-8")
    (upstream / "cat1" / "second.parquet").write_text("demo", encoding="utf-8")

    pattern = str(upstream / "cat*" / "*.parquet")
    cfg = ProjectConfig(
        root=tmp_path,
        dirs={},
        file_sources=[FileSourceConfig(name="demo", upstream_glob=pattern)],
    )

    build_symlinks(cfg)

    sqlite_path = data_map_sqlite_path(cfg.site_root)
    assert sqlite_path.exists()

    data_map = load_data_map(cfg.site_root)
    expected_keys = {
        f"data/demo/cat1/{name}" for name in ["first.parquet", "second.parquet"]
    }
    assert set(data_map.keys()) == expected_keys
    for value in data_map.values():
        assert value.startswith(str(upstream))
    fingerprints = load_fingerprints(cfg.site_root)
    assert fingerprints.get("demo")


def test_symlinks_rebuilds_each_run(tmp_path: Path) -> None:
    upstream = tmp_path / "upstream"
    (upstream / "cat1").mkdir(parents=True, exist_ok=True)
    (upstream / "cat1" / "first.parquet").write_text("demo", encoding="utf-8")

    pattern = str(upstream / "cat*" / "*.parquet")
    cfg = ProjectConfig(
        root=tmp_path,
        dirs={},
        file_sources=[FileSourceConfig(name="demo", upstream_glob=pattern)],
    )

    build_symlinks(cfg)

    sqlite_path = data_map_sqlite_path(cfg.site_root)
    assert sqlite_path.exists()
    fingerprints = load_fingerprints(cfg.site_root)
    assert fingerprints.get("demo")

    first_mtime = sqlite_path.stat().st_mtime
    time.sleep(1.0)

    build_symlinks(cfg)

    assert sqlite_path.stat().st_mtime > first_mtime
    assert sqlite_path.exists()


def test_symlinks_rebuilds_when_upstream_changes(tmp_path: Path) -> None:
    upstream = tmp_path / "upstream"
    (upstream / "cat1").mkdir(parents=True, exist_ok=True)
    (upstream / "cat1" / "first.parquet").write_text("demo", encoding="utf-8")

    pattern = str(upstream / "cat*" / "*.parquet")
    cfg = ProjectConfig(
        root=tmp_path,
        dirs={},
        file_sources=[FileSourceConfig(name="demo", upstream_glob=pattern)],
    )

    build_symlinks(cfg)

    sqlite_path = data_map_sqlite_path(cfg.site_root)
    first_mtime = sqlite_path.stat().st_mtime
    first_meta = load_fingerprints(cfg.site_root)

    time.sleep(1.0)
    (upstream / "cat1" / "second.parquet").write_text("demo2", encoding="utf-8")

    build_symlinks(cfg)

    refreshed = load_data_map(cfg.site_root)
    refreshed_meta = load_fingerprints(cfg.site_root)

    assert sqlite_path.stat().st_mtime > first_mtime
    assert sqlite_path.exists()
    assert refreshed_meta.get("demo") != first_meta.get("demo")
    assert set(refreshed) == {
        f"data/demo/cat1/{name}" for name in ["first.parquet", "second.parquet"]
    }


def test_serve_project_unknown_form_returns_400(tmp_path, monkeypatch):
    _stub_echarts(monkeypatch)

    init_project(tmp_path)

    port = 8099
    t = threading.Thread(target=serve_project, args=(tmp_path, port, "builtin"), daemon=True)
    t.start()
    time.sleep(1.0)

    conn = http.client.HTTPConnection("localhost", port, timeout=5)
    conn.request(
        "POST",
        "/api/forms/submit",
        body=b'{"form_id": "unknown"}',
        headers={"Content-Type": "application/json"},
    )
    resp = conn.getresponse()
    body = resp.read().decode("utf-8")
    conn.close()

    assert resp.status == 400
    assert "unknown form" in body


def test_range_requests_return_not_modified(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _stub_echarts(monkeypatch)
    init_project(tmp_path)
    build_project(tmp_path)

    request_log: list = []
    port = _find_free_port()
    _start_server(tmp_path, port, monkeypatch, request_log)

    sample = tmp_path / "static" / "sample.txt"
    sample.write_text("0123456789", encoding="utf-8")

    conn = http.client.HTTPConnection("localhost", port, timeout=5)
    conn.request("GET", "/sample.txt")
    resp = conn.getresponse()
    last_modified = resp.getheader("Last-Modified")
    if last_modified is None:
        last_modified = email.utils.formatdate(sample.stat().st_mtime, usegmt=True)
    resp.read()
    conn.close()

    conn = http.client.HTTPConnection("localhost", port, timeout=5)
    conn.request(
        "GET",
        "/sample.txt",
        headers={"Range": "bytes=0-4", "If-Modified-Since": last_modified},
    )
    resp2 = conn.getresponse()
    body = resp2.read()
    conn.close()

    assert resp2.status == HTTPStatus.NOT_MODIFIED
    assert body == b""


def test_duckdb_http_query_uses_range_requests(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _stub_echarts(monkeypatch)
    init_project(tmp_path)
    build_project(tmp_path)

    request_log: list = []
    port = _find_free_port()
    _start_server(tmp_path, port, monkeypatch, request_log)

    data_dir = tmp_path / "static" / "data" / "demo"
    data_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = data_dir / "sample.parquet"

    con = duckdb.connect()
    con.execute(f"COPY (SELECT i AS id FROM range(3) t(i)) TO '{parquet_path}' (FORMAT PARQUET)")
    con.close()

    url = f"{Scheme.HTTP.value}://localhost:{port}{AssetPath.DEMO_DATA.value}/sample.parquet"
    client = _connect_with_httpfs_or_skip()
    client.execute("SET enable_http_metadata_cache=true")
    client.execute(f"SELECT count(*) FROM read_parquet('{url}')")
    client.fetchall()
    client.execute(f"SELECT sum(id) FROM read_parquet('{url}')")
    client.fetchall()
    client.close()

    assert any(entry.get("range") for entry in request_log)


def test_builtin_server_uses_http_11_keep_alive(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _stub_echarts(monkeypatch)
    init_project(tmp_path)
    content = tmp_path / "content"
    content.mkdir(parents=True, exist_ok=True)
    (content / "index.md").write_text("hello", encoding="utf-8")
    build_project(tmp_path)

    port = _find_free_port()
    _start_server(tmp_path, port, monkeypatch, request_log=[])

    conn = http.client.HTTPConnection("localhost", port, timeout=5)
    conn.request("GET", AssetPath.INDEX.value, headers={"Connection": "keep-alive"})
    resp = conn.getresponse()
    resp.read()

    assert resp.version == 11
    assert resp.will_close is False

    first_sock = conn.sock

    conn.request("GET", AssetPath.CONTRACT_JS.value, headers={"Connection": "keep-alive"})
    resp2 = conn.getresponse()
    body2 = resp2.read()

    assert resp2.status == 200
    assert body2
    assert conn.sock is first_sock

    conn.close()


def test_httpfs_metadata_cache_limits_head_requests(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _stub_echarts(monkeypatch)
    init_project(tmp_path)
    build_project(tmp_path)

    request_log: list = []
    port = _find_free_port()
    _start_server(tmp_path, port, monkeypatch, request_log)

    data_dir = tmp_path / "static" / "data" / "demo"
    data_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = data_dir / "head_sample.parquet"

    con = duckdb.connect()
    con.execute(
        f"COPY (SELECT i AS id FROM range(5) t(i)) TO '{parquet_path}' (FORMAT PARQUET)"
    )
    con.close()

    url = f"{Scheme.HTTP.value}://localhost:{port}{AssetPath.DEMO_DATA.value}/head_sample.parquet"
    client = _connect_with_httpfs_or_skip()
    client.execute("SET enable_http_metadata_cache=true")

    for _ in range(2):
        client.execute(f"SELECT sum(id) FROM read_parquet('{url}')")
        client.fetchall()

    client.close()

    head_requests = [entry for entry in request_log if entry.get("method") == "HEAD"]
    assert len(head_requests) <= 1


def test_server_sets_cache_headers(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _stub_echarts(monkeypatch)
    init_project(tmp_path)
    build_project(tmp_path)

    request_log: list = []
    port = _find_free_port()
    _start_server(tmp_path, port, monkeypatch, request_log)

    asset = tmp_path / "static" / "asset.txt"
    asset.write_text("cache me", encoding="utf-8")

    conn = http.client.HTTPConnection("localhost", port, timeout=5)
    conn.request("GET", "/asset.txt")
    resp = conn.getresponse()
    cache_control = resp.getheader("Cache-Control")
    age = resp.getheader("Age")
    resp.read()
    conn.close()

    assert cache_control == "public, max-age=604800, immutable"
    assert age is None

    conn = http.client.HTTPConnection("localhost", port, timeout=5)
    conn.request("GET", AssetPath.INDEX.value)
    resp = conn.getresponse()
    cache_html = resp.getheader("Cache-Control")
    resp.read()
    conn.close()

    assert cache_html == "public, max-age=60"


def test_server_serves_gzip(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _stub_echarts(monkeypatch)
    init_project(tmp_path)
    build_project(tmp_path)

    request_log: list = []
    port = _find_free_port()
    _start_server(tmp_path, port, monkeypatch, request_log)

    asset = tmp_path / "static" / "asset.txt"
    asset.write_text("cache me", encoding="utf-8")

    conn = http.client.HTTPConnection("localhost", port, timeout=5)
    conn.request("GET", "/asset.txt", headers={"Accept-Encoding": "gzip"})
    resp = conn.getresponse()
    body = resp.read()
    conn.close()

    assert resp.getheader("Content-Encoding") == "gzip"
    assert resp.getheader("Vary") == "Accept-Encoding"
    assert resp.getheader("Cache-Control") == "public, max-age=604800, immutable"
    assert gzip.decompress(body) == b"cache me"


def test_demo_parquet_queries_run_quickly(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _stub_echarts(monkeypatch)
    monkeypatch.setattr(
        demo_init_fake_parquet, "_download_nytaxi_parquet", lambda dest: False
    )

    init_demo_project(tmp_path)
    build_project(tmp_path)

    request_log: list = []
    port = _find_free_port()
    _start_server(tmp_path, port, monkeypatch, request_log)

    client = _connect_with_httpfs_or_skip()
    client.execute("SET enable_http_metadata_cache=true")

    base = f"{Scheme.HTTP.value}://localhost:{port}{AssetPath.DEMO_DATA.value}"
    start = time.perf_counter()
    totals = []
    for name in ("demo-A", "demo-B", "demo-C"):
        client.execute(f"SELECT sum(value) FROM read_parquet('{base}/{name}.parquet')")
        totals.append(client.fetchone()[0])
    duration = time.perf_counter() - start
    client.close()

    head_requests = [entry for entry in request_log if entry.get("method") == "HEAD"]
    assert duration < 5.0
    assert len(head_requests) <= 3
    assert all(total is not None for total in totals)
