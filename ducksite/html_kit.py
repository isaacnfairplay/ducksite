from __future__ import annotations

from enum import StrEnum
from typing import Mapping, Union, List


class HtmlTag(StrEnum):
    HTML = "html"
    HEAD = "head"
    BODY = "body"
    NAV = "nav"
    DIV = "div"
    PRE = "pre"
    META = "meta"
    LINK = "link"
    SCRIPT = "script"
    SPAN = "span"


class HtmlAttr(StrEnum):
    CHARSET = "charset"
    REL = "rel"
    HREF = "href"
    SRC = "src"
    TYPE = "type"
    CLASS = "class"
    ID = "id"

    # Data attributes used by ducksite (grids / viz containers)
    DATA_COLS = "data-cols"
    DATA_GAP = "data-gap"
    DATA_VIZ_ID = "data-viz-id"
    DATA_TABLE_ID = "data-table-id"


class HtmlVal(StrEnum):
    UTF8 = "utf-8"
    STYLESHEET = "stylesheet"
    MODULE = "module"
    APPLICATION_JSON = "application/json"


class HtmlId(StrEnum):
    PAGE_CONFIG_JSON = "page-config-json"


class SitePath(StrEnum):
    ROOT = "/"
    CSS = "/css"
    JS = "/js"
    SQL = "/sql"
    DATA = "/data"


AttrKey = Union[HtmlAttr, str]
Attrs = Mapping[AttrKey, str] | None


def attrs_to_string(attrs: Attrs) -> str:
    """
    Convert an attribute mapping to a ' key="value"' string (leading space if any attrs).
    """
    if not attrs:
        return ""
    parts: List[str] = []
    for key, value in attrs.items():
        k = key.value if isinstance(key, HtmlAttr) else str(key)
        parts.append(f'{k}="{value}"')
    return " " + " ".join(parts)


def open_tag(tag: HtmlTag, attrs: Attrs = None) -> str:
    return f"<{tag.value}{attrs_to_string(attrs)}>"


def close_tag(tag: HtmlTag) -> str:
    return f"</{tag.value}>"


def empty_element(tag: HtmlTag, attrs: Attrs = None) -> str:
    """
    For elements like <meta> or <link> that we treat as self-contained.
    """
    return open_tag(tag, attrs)


def element(tag: HtmlTag, content: str, attrs: Attrs = None) -> str:
    return f"{open_tag(tag, attrs)}{content}{close_tag(tag)}"


def doctype_html() -> str:
    """
    HTML5 doctype string.
    """
    return "<!doctype html>"
