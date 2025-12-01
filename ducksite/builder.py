from __future__ import annotations

from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Dict, List

import duckdb
import http.server
import json
import cgi
import shutil
import socketserver
import threading

from .config import load_project_config, ProjectConfig
from .cte_compiler import compile_query, write_compiled_sql
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
from .utils import ensure_dir
from .forms import discover_forms, process_form_submission, ensure_form_target_csvs


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


def _clean_site(site_root: Path) -> None:
    if site_root.exists():
        shutil.rmtree(site_root)
    ensure_dir(site_root)


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

    for name, nq in named_queries.items():
        try:
            compiled_sql, metrics, deps = compile_query(site_root, con, named_queries, name)
        except Exception as e:
            print(f"[ducksite] ERROR compiling global query '{name}': {e}")
            raise

        out_path = write_compiled_sql(site_root, global_rel, name, compiled_sql, metrics)
        rel = out_path.relative_to(site_root).as_posix()
        sql_path = "/" + rel  # e.g. "/sql/_global/demo_chain_agg.sql"

        manifest[name] = {
            "kind": nq.kind,
            "deps": deps,
            "sql_path": sql_path,
        }

    manifest_path = site_root / "sql" / "_manifest.json"
    ensure_dir(manifest_path.parent)
    manifest_path.write_text(json.dumps({"views": manifest}, indent=2), encoding="utf-8")
    print(f"[ducksite] wrote SQL manifest {manifest_path}")


def build_project(root: Path) -> None:
    cfg: ProjectConfig = load_project_config(root)

    _clean_site(cfg.site_root)
    ensure_js_assets(root, cfg.site_root)

    # Build the virtual data_map.json for all file_sources; no real symlinks/copies.
    build_symlinks(cfg)

    # Ensure stub CSVs exist for any form targets (e.g. forms/feedback.csv) so that
    # build-time EXPLAINs over read_csv_auto(...) can succeed.
    ensure_form_target_csvs(cfg)

    named_queries: Dict[str, NamedQuery] = {}
    named_queries.update(build_file_source_queries(cfg))
    named_queries.update(load_model_queries(cfg))

    all_md: List[Path] = []
    if cfg.content_dir.exists():
        for md_path in cfg.content_dir.rglob("*.md"):
            all_md.append(md_path.relative_to(cfg.content_dir))

    con = duckdb.connect()
    try:
        if not cfg.content_dir.exists():
            print(f"[ducksite] content dir not found: {cfg.content_dir}")
        else:
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
                for qid in pq.sql_blocks.keys():
                    compiled_sql, metrics, _deps = compile_query(
                        cfg.site_root,
                        con,
                        named_queries,
                        qid,
                    )
                    write_compiled_sql(cfg.site_root, page_rel_dir, qid, compiled_sql, metrics)

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

        _build_global_queries(cfg, con, named_queries)
    finally:
        try:
            con.close()
        except Exception:
            pass

    _write_sitemap(cfg.site_root, all_md)
    print("[ducksite] build complete.")
