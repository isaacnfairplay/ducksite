from __future__ import annotations
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Dict, List
import re
import json

import markdown  # render non-block markdown sections to HTML

from .html_kit import HtmlTag, HtmlAttr, open_tag, close_tag, element

SQL_BLOCK_RE = re.compile(r"```sql\s+([A-Za-z0-9_]+)\s*\n(.*?)```", re.DOTALL)
ECHART_BLOCK_RE = re.compile(r"```echart\s+([A-Za-z0-9_]+)\s*\n(.*?)```", re.DOTALL)
GRID_BLOCK_RE = re.compile(r"```grid(.*?)\n(.*?)```", re.DOTALL)
INPUT_BLOCK_RE = re.compile(r"```input\s+([A-Za-z0-9_]+)\s*\n(.*?)```", re.DOTALL)


@dataclass
class PageQueries:
    sql_blocks: Dict[str, str] = field(default_factory=dict)
    echart_blocks: Dict[str, Dict[str, str]] = field(default_factory=dict)
    grid_specs: List[Dict] = field(default_factory=list)
    input_defs: Dict[str, Dict[str, str]] = field(default_factory=dict)
    html: str = ""
    page_rel: Path = Path("")


class CssClass(StrEnum):
    LAYOUT_GRID = "layout-grid"
    LAYOUT_ROW = "layout-row"
    LAYOUT_COL = "layout-col"
    VIZ_CONTAINER = "viz-container"
    TABLE_CONTAINER = "table-container"
    SPAN_PREFIX = "span-"


def _parse_key_values(body: str) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for line in body.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        result[k.strip()] = v.strip().strip('"').strip("'")
    return result


def _parse_grid_block(args: str, body: str) -> Dict:
    args = args.strip()
    cols = 2
    gap = "md"
    for tok in args.split():
        if "=" in tok:
            k, v = tok.split("=", 1)
            if k == "cols":
                cols = int(v)
            elif k == "gap":
                gap = v
    rows_spec: List[List[Dict]] = []
    for line in body.splitlines():
        line = line.strip()
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        row: List[Dict] = []
        for cell in cells:
            if not cell or cell == ".":
                continue
            if ":" in cell:
                _id, span_str = cell.split(":", 1)
                row.append({"id": _id.strip(), "span": int(span_str)})
            else:
                row.append({"id": cell, "span": 1})
        if row:
            rows_spec.append(row)
    return {"cols": cols, "gap": gap, "rows": rows_spec}


def parse_markdown_page(path: Path, rel_path: Path) -> PageQueries:
    text = path.read_text(encoding="utf-8")
    pq = PageQueries(page_rel=rel_path)

    # 1) Extract all special fenced blocks
    for m in SQL_BLOCK_RE.finditer(text):
        pq.sql_blocks[m.group(1)] = m.group(2).strip()

    for m in ECHART_BLOCK_RE.finditer(text):
        viz_id = m.group(1)
        kv = _parse_key_values(m.group(2))
        pq.echart_blocks[viz_id] = kv

    for m in INPUT_BLOCK_RE.finditer(text):
        inp_name = m.group(1)
        kv = _parse_key_values(m.group(2))
        pq.input_defs[inp_name] = kv

    for m in GRID_BLOCK_RE.finditer(text):
        args = m.group(1)
        body = m.group(2)
        pq.grid_specs.append(_parse_grid_block(args, body))

    # 2) Strip those blocks out, leaving the "normal" markdown sections
    stripped = SQL_BLOCK_RE.sub("", text)
    stripped = ECHART_BLOCK_RE.sub("", stripped)
    stripped = INPUT_BLOCK_RE.sub("", stripped)
    stripped = GRID_BLOCK_RE.sub("", stripped)
    stripped_content = stripped.strip()

    html_parts: List[str] = []
    p = html_parts.append

    # 3) Render remaining markdown to HTML so it is readable
    if stripped_content:
        md_html = markdown.markdown(stripped_content)
        # Wrap in a container so CSS can style it cleanly
        p('<div class="ducksite-md">')
        p(md_html)
        p("</div>\n")

    # 4) Render grid layout scaffolding for charts/tables
    for grid in pq.grid_specs:
        cols = grid["cols"]
        gap = grid["gap"]

        p(
            open_tag(
                HtmlTag.DIV,
                {
                    HtmlAttr.CLASS: CssClass.LAYOUT_GRID.value,
                    HtmlAttr.DATA_COLS: str(cols),
                    HtmlAttr.DATA_GAP: gap,
                },
            )
        )
        p("\n")

        for row in grid["rows"]:
            p(
                open_tag(
                    HtmlTag.DIV,
                    {HtmlAttr.CLASS: CssClass.LAYOUT_ROW.value},
                )
            )
            p("\n")

            for cell in row:
                cid = cell["id"]
                span = cell["span"]
                col_class = f"{CssClass.LAYOUT_COL.value} {CssClass.SPAN_PREFIX.value}{span}"
                p(
                    open_tag(
                        HtmlTag.DIV,
                        {HtmlAttr.CLASS: col_class},
                    )
                )

                if cid in pq.echart_blocks:
                    p(
                        element(
                            HtmlTag.DIV,
                            content="",
                            attrs={
                                HtmlAttr.CLASS: CssClass.VIZ_CONTAINER.value,
                                HtmlAttr.DATA_VIZ_ID: cid,
                            },
                        )
                    )
                else:
                    p(
                        element(
                            HtmlTag.DIV,
                            content="",
                            attrs={
                                HtmlAttr.CLASS: CssClass.TABLE_CONTAINER.value,
                                HtmlAttr.DATA_TABLE_ID: cid,
                            },
                        )
                    )

                p(close_tag(HtmlTag.DIV))
                p("\n")

            p(close_tag(HtmlTag.DIV))
            p("\n")

        p(close_tag(HtmlTag.DIV))
        p("\n")

    pq.html = "".join(html_parts)
    return pq


def build_page_config(pq: PageQueries) -> str:
    config = {
        "queries": {qid: {"sql_id": qid} for qid in pq.sql_blocks.keys()},
        "visualizations": pq.echart_blocks,
        "inputs": pq.input_defs,
        "grids": pq.grid_specs,
    }
    return json.dumps(config, indent=2)


if __name__ == "__main__":
    from pprint import pprint

    sample = Path("sample.md")
    if sample.exists():
        pq = parse_markdown_page(sample, Path("."))
        pprint(pq)
        print(build_page_config(pq))
    else:
        print("No sample.md to parse.")
