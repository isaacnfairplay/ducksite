from __future__ import annotations

from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Any, Dict, IO, List, cast

from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime
import duckdb
import gzip
import http.server
import http
import json
import cgi
import shutil
import socketserver
import threading
import time
import os
from queue import SimpleQueue
from email.utils import parsedate_to_datetime

from .config import load_project_config, ProjectConfig
from .compile_cache import (
    cache_signature,
    load_compile_cache,
    record_compiled_query,
    save_compile_cache,
)
from .cte_compiler import NetworkMetrics, compile_query, write_compiled_sql
from .data_map_cache import load_data_map, load_fingerprints
from .data_map_paths import data_map_shard
from .html_kit import (
    HtmlTag,
    HtmlAttr,
    HtmlVal,
    HtmlId,
    SitePath,
    doctype_html,
    open_tag,
    close_tag,
    empty_element,
    element,
)
from .js_assets import ensure_js_assets
from .markdown_parser import parse_markdown_page, build_page_config
from .queries import NamedQuery, build_file_source_queries, load_model_queries
from .symlinks import build_symlinks
from .utils import ensure_dir, sha256_list
from .forms import discover_forms, process_form_submission, ensure_form_target_csvs
from .auth import update_password


class CssAsset(StrEnum):
    BASE = "ducksite.css"
    CHARTS = "charts.css"


class JsAsset(StrEnum):
    ECHARTS = "echarts.min.js"
    MAIN = "main.js"


class HtmlPage(StrEnum):
    INDEX = "index.html"


class NavCssClass(StrEnum):
    NAV = "ducksite-nav"
    NAV_CURRENT = "ducksite-nav-current"


CSS_BASE = PurePosixPath(SitePath.CSS.value)
JS_BASE = PurePosixPath(SitePath.JS.value)
ROOT_URL = PurePosixPath(SitePath.ROOT.value)
INDEX_URL = ROOT_URL / HtmlPage.INDEX.value


def css_href(asset: CssAsset) -> str:
    return str(CSS_BASE / asset.value)


def js_src(asset: JsAsset) -> str:
    return str(JS_BASE / asset.value)


def _clean_site(site_root: Path, preserve_data_maps: bool = True) -> None:
    if not site_root.exists():
        ensure_dir(site_root)
        return

    if not preserve_data_maps:
        shutil.rmtree(site_root)
        ensure_dir(site_root)
        return

    for child in site_root.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()

    ensure_dir(site_root)


def _fingerprint_token(site_root: Path) -> str:
    fps = load_fingerprints(site_root)
    if not fps:
        return ""
    return sha256_list([f"{k}:{v}" for k, v in sorted(fps.items())])


def _all_sql_hash(queries: Dict[str, NamedQuery]) -> str:
    return sha256_list([f"{name}:{q.sql}" for name, q in sorted(queries.items())])


def _page_url(section: str | None) -> str:
    if not section or section == ".":
        return str(INDEX_URL)
    return str(ROOT_URL / section / HtmlPage.INDEX.value)


def _build_nav_html(page_rel: Path, all_pages: List[Path]) -> str:
    parts: List[str] = []
    p = parts.append

    p(
        open_tag(
            HtmlTag.NAV,
            {HtmlAttr.CLASS: NavCssClass.NAV.value},
        )
    )

    link_pieces: List[str] = []
    for rel in sorted(all_pages):
        if rel.name != "index.md":
            continue
        if rel == Path("index.md"):
            section = None
            label = "home"
        else:
            section = rel.parent.as_posix()
            label = section
        url = _page_url(section)
        link_pieces.append(f'<a href="{url}">{label}</a>')
    if link_pieces:
        p("  " + " | ".join(link_pieces))

    section = page_rel.as_posix() if page_rel != Path(".") else None
    current_url = _page_url(section)
    current_span = element(
        HtmlTag.SPAN,
        content=f"(current: {current_url})",
        attrs={HtmlAttr.CLASS: NavCssClass.NAV_CURRENT.value},
    )
    p("  " + current_span)

    p(close_tag(HtmlTag.NAV))
    return "\n".join(parts) + "\n"


def _write_sitemap(site_root: Path, all_pages: List[Path]) -> None:
    urls: List[str] = []
    for rel in sorted(all_pages):
        if rel.name != "index.md":
            continue
        if rel == Path("index.md"):
            urls.append(str(INDEX_URL))
        else:
            section = rel.parent.as_posix()
            urls.append(_page_url(section))

    sitemap = {"routes": urls}
    out = site_root / "sitemap.json"
    ensure_dir(out.parent)
    out.write_text(json.dumps(sitemap, indent=2), encoding="utf-8")
    print(f"[ducksite] wrote sitemap {out}")


def _prune_deleted_markdown(cfg: ProjectConfig, current_md: set[Path]) -> None:
    if not cfg.site_root.exists():
        return

    for html_path in cfg.site_root.rglob("*.html"):
        rel_html = html_path.relative_to(cfg.site_root)
        # Skip non-content outputs (e.g. SQL, data, forms)
        if rel_html.parts and rel_html.parts[0] in {"sql", "data", "forms"}:
            continue

        rel_md = rel_html.with_suffix(".md")
        if rel_md in current_md:
            continue

        try:
            html_path.unlink()
            print(f"[ducksite] removed stale page {html_path}")
        except FileNotFoundError:
            continue

        # Remove any page-local SQL outputs for the deleted page.
        sql_dir = cfg.site_root / "sql" / rel_md.parent
        if sql_dir.exists() and sql_dir.is_dir() and sql_dir.name != "_global":
            shutil.rmtree(sql_dir)


def _log_step_start(label: str) -> float:
    print(f"[ducksite] {label}...")
    return time.perf_counter()


def _log_step_end(label: str, start: float) -> None:
    elapsed = time.perf_counter() - start
    print(f"[ducksite] {label} in {elapsed:.2f}s")


def _build_page_html(
    nav_html: str,
    body_inner_html: str,
    page_cfg_json: str,
) -> str:
    parts: List[str] = []
    p = parts.append

    p(doctype_html())
    p(open_tag(HtmlTag.HTML))
    p(open_tag(HtmlTag.HEAD))

    p(empty_element(HtmlTag.META, {HtmlAttr.CHARSET: HtmlVal.UTF8.value}))

    p(
        empty_element(
            HtmlTag.LINK,
            {
                HtmlAttr.REL: HtmlVal.STYLESHEET.value,
                HtmlAttr.HREF: css_href(CssAsset.BASE),
            },
        )
    )
    p(
        empty_element(
            HtmlTag.LINK,
            {
                HtmlAttr.REL: HtmlVal.STYLESHEET.value,
                HtmlAttr.HREF: css_href(CssAsset.CHARTS),
            },
        )
    )

    p(
        element(
            HtmlTag.SCRIPT,
            content="",
            attrs={HtmlAttr.SRC: js_src(JsAsset.ECHARTS)},
        )
    )
    p(
        element(
            HtmlTag.SCRIPT,
            content="",
            attrs={
                HtmlAttr.SRC: js_src(JsAsset.MAIN),
                HtmlAttr.TYPE: HtmlVal.MODULE.value,
            },
        )
    )

    p(close_tag(HtmlTag.HEAD))
    p(open_tag(HtmlTag.BODY))

    p(nav_html)
    p(body_inner_html)

    p(
        open_tag(
            HtmlTag.SCRIPT,
            {
                HtmlAttr.ID: HtmlId.PAGE_CONFIG_JSON.value,
                HtmlAttr.TYPE: HtmlVal.APPLICATION_JSON.value,
            },
        )
    )
    p(page_cfg_json)
    p(close_tag(HtmlTag.SCRIPT))

    p(close_tag(HtmlTag.BODY))
    p(close_tag(HtmlTag.HTML))

    return "\n".join(parts) + "\n"


def _build_global_queries(
    cfg: ProjectConfig,
    con: duckdb.DuckDBPyConnection,
    named_queries: Dict[str, NamedQuery],
    compile_cache: Dict[str, Dict[str, object]],
    fingerprint_token: str,
) -> None:
    """
    Compile *all* known NamedQuery entries into global SQL views and
    write a manifest so the browser-side SQL editor can expose them.
    """
    if not named_queries:
        return

    site_root = cfg.site_root
    global_rel = Path("_global")
    manifest: Dict[str, Dict[str, object]] = {}

    sql_hash = _all_sql_hash(named_queries)
    signature = cache_signature(sql_hash, fingerprint_token)

    to_compile: list[str] = []
    compiled_results: Dict[str, tuple[str, NetworkMetrics, list[str]]] = {}

    for name, nq in named_queries.items():
        try:
            cached = compile_cache.get(name)

            if cached and cached.get("signature") == signature:
                cached_metrics = cached.get("metrics")
                cached_sql = cached.get("compiled_sql")
                if isinstance(cached_metrics, dict) and isinstance(cached_sql, str):
                    metrics = NetworkMetrics(**cached_metrics)
                    deps = [str(d) for d in cached.get("deps", [])]
                    out_path = write_compiled_sql(
                        site_root,
                        global_rel,
                        name,
                        cached_sql,
                        metrics,
                        include_metrics_header=False,
                    )
                    print(f"[ducksite] reused cached SQL for '{name}'")
                    rel = out_path.relative_to(site_root).as_posix()
                    sql_path = "/" + rel
                    manifest[name] = {
                        "kind": nq.kind,
                        "deps": deps,
                        "sql_path": sql_path,
                        "num_files": metrics.num_files,
                    }
                    continue

            to_compile.append(name)
        except Exception as e:
            print(f"[ducksite] ERROR compiling global query '{name}': {e}")
            raise

    if to_compile:
        max_workers = min(len(to_compile), max(1, (os.cpu_count() or 1)))

        connection_pool: SimpleQueue[duckdb.DuckDBPyConnection] = SimpleQueue()
        pooled_connections: list[duckdb.DuckDBPyConnection] = []

        for _ in range(max_workers):
            con = duckdb.connect()
            pooled_connections.append(con)
            connection_pool.put(con)

        def _compile_one(query_name: str) -> tuple[str, str, NetworkMetrics, list[str]]:
            local_con = connection_pool.get()
            try:
                compiled_sql, metrics, deps = compile_query(
                    site_root, local_con, named_queries, query_name
                )
                return query_name, compiled_sql, metrics, deps
            finally:
                connection_pool.put(local_con)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {executor.submit(_compile_one, name): name for name in to_compile}
            for future in as_completed(future_map):
                q_name, compiled_sql, metrics, deps = future.result()
                compiled_results[q_name] = (compiled_sql, metrics, deps)

        for con in pooled_connections:
            try:
                con.close()
            except Exception:
                pass

    for name in to_compile:
        compiled_sql, metrics, deps = compiled_results[name]
        record_compiled_query(compile_cache, name, signature, compiled_sql, metrics, deps)

        out_path = write_compiled_sql(
            site_root,
            global_rel,
            name,
            compiled_sql,
            metrics,
            include_metrics_header=False,
        )
        rel = out_path.relative_to(site_root).as_posix()
        sql_path = "/" + rel  # e.g. "/sql/_global/demo_chain_agg.sql"

        manifest[name] = {
            "kind": nq.kind,
            "deps": deps,
            "sql_path": sql_path,
            "num_files": metrics.num_files,
        }

    manifest_path = site_root / "sql" / "_manifest.json"
    ensure_dir(manifest_path.parent)
    manifest_path.write_text(json.dumps({"views": manifest}, indent=2), encoding="utf-8")
    print(f"[ducksite] wrote SQL manifest {manifest_path}")


def build_project(root: Path, clean: bool = False) -> None:
    build_started = time.perf_counter()
    cfg: ProjectConfig = load_project_config(root)
    print(
        f"[ducksite] building project rooted at {cfg.root} with site output {cfg.site_root}"
    )

    compile_cache: Dict[str, Dict[str, object]] = load_compile_cache(cfg.root)

    if clean:
        step = _log_step_start("cleaning site directory")
        _clean_site(cfg.site_root)
        _log_step_end("cleaned site directory", step)

    step = _log_step_start("ensuring JS/CSS assets")
    ensure_js_assets(root, cfg.site_root)
    _log_step_end("ensured JS/CSS assets", step)

    # Build the virtual data_map.json for all file_sources; no real symlinks/copies.
    step = _log_step_start("building data map and virtual symlinks")
    build_symlinks(cfg)
    _log_step_end("built data map and virtual symlinks", step)

    fingerprint_token = _fingerprint_token(cfg.site_root)

    # Ensure stub CSVs exist for any form targets (e.g. forms/feedback.csv) so that
    # build-time EXPLAINs over read_csv_auto(...) can succeed.
    step = _log_step_start("ensuring form target CSV stubs")
    ensure_form_target_csvs(cfg)
    _log_step_end("ensured form target CSV stubs", step)

    step = _log_step_start("loading file source and model queries")
    named_queries: Dict[str, NamedQuery] = {}
    named_queries.update(build_file_source_queries(cfg))
    named_queries.update(load_model_queries(cfg))
    _log_step_end(f"loaded {len(named_queries)} named queries", step)

    all_md: List[Path] = []
    if cfg.content_dir.exists():
        step = _log_step_start("discovering markdown content")
        for md_path in cfg.content_dir.rglob("*.md"):
            all_md.append(md_path.relative_to(cfg.content_dir))
        _log_step_end(f"found {len(all_md)} markdown files", step)

    step = _log_step_start("pruning deleted markdown outputs")
    _prune_deleted_markdown(cfg, set(all_md))
    _log_step_end("pruned deleted markdown outputs", step)

    con = duckdb.connect()
    try:
        if not cfg.content_dir.exists():
            print(f"[ducksite] content dir not found: {cfg.content_dir}")
        else:
            step = _log_step_start(f"building {len(all_md)} markdown pages")
            for md_path in cfg.content_dir.rglob("*.md"):
                rel = md_path.relative_to(cfg.content_dir)
                page_rel_dir = rel.parent
                out_page_dir = cfg.site_root / page_rel_dir
                ensure_dir(out_page_dir)

                pq = parse_markdown_page(md_path, page_rel_dir)

                # Register page-level queries so they are available for
                # dependency resolution and the global SQL manifest.
                for qid, sql in pq.sql_blocks.items():
                    named_queries[qid] = NamedQuery(name=qid, sql=sql, kind="page_query")

                # Compile per-page queries into page-local SQL files.
                sql_hash = _all_sql_hash(named_queries)

                for qid in pq.sql_blocks.keys():
                    signature = cache_signature(sql_hash, fingerprint_token)
                    cached = compile_cache.get(qid)

                    if cached and cached.get("signature") == signature:
                        cached_metrics = cached.get("metrics")
                        cached_sql = cached.get("compiled_sql")
                        if isinstance(cached_metrics, dict) and isinstance(cached_sql, str):
                            metrics = NetworkMetrics(**cached_metrics)
                            write_compiled_sql(cfg.site_root, page_rel_dir, qid, cached_sql, metrics)
                            print(f"[ducksite] reused cached SQL for page query '{qid}'")
                            continue

                    compiled_sql, metrics, deps = compile_query(
                        cfg.site_root,
                        con,
                        named_queries,
                        qid,
                    )
                    write_compiled_sql(cfg.site_root, page_rel_dir, qid, compiled_sql, metrics)
                    record_compiled_query(
                        compile_cache,
                        qid,
                        signature,
                        compiled_sql,
                        metrics,
                        deps,
                    )

                page_cfg_json = build_page_config(pq)
                nav_html = _build_nav_html(page_rel_dir, all_md)

                html_path = out_page_dir / (rel.stem + ".html")
                full_html = _build_page_html(
                    nav_html=nav_html,
                    body_inner_html=pq.html,
                    page_cfg_json=page_cfg_json,
                )
                html_path.write_text(full_html, encoding="utf-8")
                print(f"[ducksite] wrote {html_path}")
            _log_step_end("finished building markdown pages", step)

        global_step = _log_step_start("compiling global SQL views")
        _build_global_queries(cfg, con, named_queries, compile_cache, fingerprint_token)
        _log_step_end("compiled global SQL views", global_step)
    finally:
        try:
            con.close()
        except Exception:
            pass

    sitemap_step = _log_step_start("writing sitemap")
    _write_sitemap(cfg.site_root, all_md)
    _log_step_end("wrote sitemap", sitemap_step)

    save_compile_cache(cfg.root, compile_cache)

    build_elapsed = time.perf_counter() - build_started
    print(f"[ducksite] build complete in {build_elapsed:.2f}s.")


def serve_project(
    root: Path,
    port: int = 8080,
    backend: str = "builtin",
    clean: bool = False,
    host: str = "127.0.0.1",
) -> None:
    """
    Serve the built site with a simple HTTP server and a background watcher.

    backend:
      - "builtin": Python's built-in ThreadingHTTPServer (default).
      - "uvicorn": FastAPI/Starlette static server (requires extra deps).
    """
    from .watcher import watch_and_build

    cfg = load_project_config(root)
    forms_map = discover_forms(cfg)

    def watch_loop() -> None:
        watch_and_build(root, interval=2.0, clean=clean)

    t = threading.Thread(target=watch_loop, daemon=True)
    t.start()

    if backend == "uvicorn":
        from .fast_server import serve_fast

        serve_fast(cfg, port=port, host=host)
        return

    directory = str(cfg.site_root)

    class DucksiteRequestHandler(http.server.SimpleHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        _headers_buffer: list[bytes]

        """
        Custom HTTP handler that implements *virtual symlinks* for /data/...

        On each request:
          - If the requested path matches a key in the cached data map
            (e.g. 'data/demo/demo-data.parquet'), we serve the file from
            the mapped upstream filesystem path.
          - Otherwise, we serve from cfg.site_root as usual.
        """

        def translate_path(self, path: str) -> str:
            from urllib.parse import unquote

            # Strip query/fragment and decode.
            raw = path.split("?", 1)[0].split("#", 1)[0]
            cleaned = unquote(raw)
            key = cleaned.lstrip("/")  # e.g. "data/demo/demo-data.parquet"

            shard = data_map_shard(key)
            data_map = load_data_map(cfg.site_root, shard_hint=shard)

            if key in data_map:
                upstream = data_map[key]
                print(f"[ducksite] virtual data hit: {key} -> {upstream}")
                return upstream

            return super().translate_path(path)

        _COMPRESSIBLE_SUFFIXES = {
            ".html",
            ".js",
            ".css",
            ".json",
            ".txt",
            ".svg",
        }

        def _maybe_send_gzip(self) -> bool:
            if self.command not in {"GET", "HEAD"}:
                return False
            if "gzip" not in (self.headers.get("Accept-Encoding", "")):
                return False
            if self.headers.get("Range"):
                return False

            mapped_path = Path(self.translate_path(self.path))
            if not mapped_path.exists() or mapped_path.is_dir():
                return False
            if mapped_path.suffix.lower() not in self._COMPRESSIBLE_SUFFIXES:
                return False

            stat = mapped_path.stat()
            last_modified = datetime.datetime.fromtimestamp(
                stat.st_mtime, tz=datetime.timezone.utc
            ).replace(microsecond=0)
            ims = self.headers.get("If-Modified-Since")
            if ims:
                try:
                    ims_dt = parsedate_to_datetime(ims)
                except (TypeError, ValueError):
                    ims_dt = None
                if ims_dt:
                    if ims_dt.tzinfo is None:
                        ims_dt = ims_dt.replace(tzinfo=datetime.timezone.utc)
                    if ims_dt >= last_modified:
                        self.send_response(http.HTTPStatus.NOT_MODIFIED)
                        self.send_header(
                            "Last-Modified",
                            self.date_time_string(last_modified.timestamp()),
                        )
                        self.send_header("Vary", "Accept-Encoding")
                        self.end_headers()
                        return True

            try:
                original = mapped_path.read_bytes()
            except OSError:
                return False

            payload = gzip.compress(original)
            self.send_response(http.HTTPStatus.OK)
            self.send_header("Content-type", self.guess_type(str(mapped_path)))
            self.send_header("Content-Encoding", "gzip")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header(
                "Last-Modified", self.date_time_string(last_modified.timestamp())
            )
            self.send_header("Vary", "Accept-Encoding")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(payload)
            return True

        def _cache_control_header(self) -> str | None:
            if self.command not in {"GET", "HEAD"}:
                return None

            path = self.path.split("?", 1)[0]
            if path.startswith("/api/"):
                return None

            ext = Path(path).suffix.lower()
            if ext == ".html":
                return "public, max-age=60"

            long_lived = {
                ".js",
                ".css",
                ".json",
                ".parquet",
                ".png",
                ".jpg",
                ".jpeg",
                ".gif",
                ".svg",
                ".ico",
                ".txt",
                ".csv",
                ".wasm",
            }
            if ext in long_lived:
                return "public, max-age=604800, immutable"

            return "public, max-age=300"

        def end_headers(self) -> None:
            cache_control = self._cache_control_header()
            if cache_control:
                existing = any(b"Cache-Control" in header for header in self._headers_buffer)
                if not existing:
                    self.send_header("Cache-Control", cache_control)
            if self.protocol_version >= "HTTP/1.1" and not self.close_connection:
                has_conn = any(b"Connection" in header for header in self._headers_buffer)
                if not has_conn:
                    self.send_header("Connection", "keep-alive")
            super().end_headers()

        def do_POST(self) -> None:
            if self.path == "/api/forms/submit":
                self._handle_form_submit()
                return
            if self.path == "/api/auth/update_password":
                self._handle_update_password()
                return
            super().do_POST()  # type: ignore[misc]

        def do_HEAD(self) -> None:
            if self._maybe_send_gzip():
                return
            super().do_HEAD()

        def do_GET(self) -> None:
            if self._maybe_send_gzip():
                return
            super().do_GET()

        def _handle_form_submit(self) -> None:
            ctype = self.headers.get("Content-Type", "")
            payload: Dict[str, object] = {}
            files: Dict[str, bytes] = {}
            if ctype.startswith("multipart/form-data"):
                environ = {
                    "REQUEST_METHOD": "POST",
                    "CONTENT_TYPE": ctype,
                }
                form = cgi.FieldStorage(
                    fp=cast(IO[bytes], self.rfile),
                    headers=self.headers,
                    environ=environ,
                )
                for key in form.keys():
                    field = form[key]
                    if getattr(field, "filename", None):
                        files[key] = field.file.read()
                    else:
                        try:
                            payload[key] = json.loads(field.value)
                        except Exception:
                            payload[key] = field.value
            else:
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length) if length > 0 else b"{}"
                try:
                    payload = json.loads(raw.decode("utf-8"))
                except json.JSONDecodeError:
                    payload = {}

            form_id_obj = payload.get("form_id")
            if not isinstance(form_id_obj, str) or form_id_obj not in forms_map:
                msg = json.dumps({"error": "unknown form"}).encode("utf-8")
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(msg)))
                self.end_headers()
                self.wfile.write(msg)
                return

            form_id = form_id_obj

            try:
                result = process_form_submission(cfg, forms_map[form_id], payload, files)
                body = json.dumps(result).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception as e:
                msg = json.dumps({"error": str(e)}).encode("utf-8")
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(msg)))
                self.end_headers()
                self.wfile.write(msg)

        def _handle_update_password(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length > 0 else b"{}"
            try:
                payload = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError:
                payload = {}

            email = payload.get("email")
            old_password_raw = payload.get("old_password")
            new_password_raw = payload.get("new_password")
            old_password = str(old_password_raw) if old_password_raw is not None else ""
            new_password = str(new_password_raw) if new_password_raw is not None else ""

            if not isinstance(email, str):
                msg = json.dumps({"error": "email required"}).encode("utf-8")
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(msg)))
                self.end_headers()
                self.wfile.write(msg)
                return

            try:
                update_password(cfg, email, old_password, new_password)
                body = json.dumps({"status": "ok"}).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except ValueError as e:
                msg = json.dumps({"error": str(e)}).encode("utf-8")
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(msg)))
                self.end_headers()
                self.wfile.write(msg)

    class ThreadingHTTPServer(http.server.ThreadingHTTPServer):
        allow_reuse_address = True

    def handler(*args: Any, **kwargs: Any) -> DucksiteRequestHandler:
        return DucksiteRequestHandler(*args, directory=directory, **kwargs)

    with ThreadingHTTPServer((host, port), handler) as httpd:
        display_host = "localhost" if host in {"127.0.0.1", "::1", "localhost"} else host
        print(
            f"[ducksite] serving {directory} at http://{display_host}:{port}/ (builtin threaded)"
        )
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n[ducksite] server stopped.")


if __name__ == "__main__":
    build_project(Path(".").resolve())
