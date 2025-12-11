import os
from pathlib import Path
import sys
from importlib import resources

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from ducksite.js_assets import _write_contract_module, ensure_js_assets
from ducksite.markdown_parser import CssClass
from ducksite.html_kit import HtmlAttr, HtmlId, SitePath


def test_write_contract_module_uses_enums(tmp_path):
    _write_contract_module(tmp_path)
    text = (tmp_path / "ducksite_contract.js").read_text(encoding="utf-8")

    assert f'vizContainer: "{CssClass.VIZ_CONTAINER.value}"' in text
    assert f'tableContainer: "{CssClass.TABLE_CONTAINER.value}"' in text
    assert f'vizId: "{HtmlAttr.DATA_VIZ_ID.value}"' in text
    assert f'tableId: "{HtmlAttr.DATA_TABLE_ID.value}"' in text
    assert f'pageConfigJson: "{HtmlId.PAGE_CONFIG_JSON.value}"' in text
    assert f'sqlRoot: "{SitePath.SQL.value}"' in text


def test_ensure_js_assets_copies_static(tmp_path):
    site_root = tmp_path / "static"
    js_root = site_root / "js"
    css_root = site_root / "css"

    js_root.mkdir(parents=True)
    css_root.mkdir(parents=True)
    (js_root / "echarts.min.js").write_text("// stub", encoding="utf-8")

    ensure_js_assets(tmp_path, site_root)

    expected_js = [
        "main.js",
        "page_runtime.js",
        "render.js",
        "page_config.js",
        "inputs.js",
    ]
    for name in expected_js:
        assert (js_root / name).exists()

    expected_css = ["ducksite.css", "charts.css"]
    for name in expected_css:
        assert (css_root / name).exists()


def test_ensure_js_assets_does_not_rename_assets(tmp_path: Path) -> None:
    site_root = tmp_path / "static"
    js_root = site_root / "js"
    css_root = site_root / "css"
    js_root.mkdir(parents=True)
    css_root.mkdir(parents=True)
    (js_root / "echarts.min.js").write_text("// stub", encoding="utf-8")

    ensure_js_assets(tmp_path, site_root)

    for name in ["main.js", "page_runtime.js", "render.js"]:
        assert (js_root / name).is_file()
    for name in ["ducksite.css", "charts.css"]:
        assert (css_root / name).is_file()


def test_ensure_js_assets_skips_unchanged_static_files(tmp_path: Path) -> None:
    site_root = tmp_path / "static"
    js_root = site_root / "js"
    css_root = site_root / "css"
    js_root.mkdir(parents=True)
    css_root.mkdir(parents=True)
    (js_root / "echarts.min.js").write_text("// stub", encoding="utf-8")

    main_src = resources.files("ducksite").joinpath("static_src/main.js")
    with resources.as_file(main_src) as src_path:
        dest = js_root / src_path.name
        dest.write_bytes(src_path.read_bytes())
    os.utime(dest, (1, 1))
    mtime_before = dest.stat().st_mtime_ns

    ensure_js_assets(tmp_path, site_root)

    assert dest.stat().st_mtime_ns == mtime_before
