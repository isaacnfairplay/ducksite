from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from ducksite.js_assets import _write_contract_module
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
