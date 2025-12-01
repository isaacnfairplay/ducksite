import http.server
import json
import subprocess
import threading
import time
from pathlib import Path
from urllib.parse import unquote

import pytest

cv2 = pytest.importorskip("cv2")
pytesseract = pytest.importorskip("pytesseract")

SNAPSHOT_HELPER = Path(__file__).resolve().parents[2] / "tools" / "snapshot_chart.js"

from ducksite import js_assets
from ducksite.builder import build_project
from ducksite.init_project import init_project


@pytest.mark.slow
@pytest.mark.skipif(
    not SNAPSHOT_HELPER.exists(),
    reason="snapshot helper missing",
)
def test_gallery_titles_and_legends_visible(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_download(url: str, dest: Path) -> None:  # noqa: ARG001
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(
            """
(function () {
  function clear(el) {
    while (el.firstChild) {
      el.removeChild(el.firstChild);
    }
  }

  function renderTitle(root, option) {
    const title = option && option.title;
    const text = title && (title.text || title);
    if (!text) return;

    const h = document.createElement("div");
    h.className = "echarts-title";
    h.style.fontSize = "16px";
    h.style.fontWeight = "600";
    h.textContent = String(text);
    root.appendChild(h);
  }

  function renderLegend(root, option) {
    const legend = option && option.legend;
    if (!legend) return;
    const data = Array.isArray(legend.data)
      ? legend.data
      : legend === true
        ? []
        : legend && legend.data
          ? legend.data
          : [];
    if (!data || data.length === 0) return;

    const ul = document.createElement("ul");
    ul.className = "echarts-legend";
    ul.style.listStyle = "none";
    ul.style.display = "flex";
    ul.style.flexWrap = "wrap";
    ul.style.gap = "8px";

    for (const item of data) {
      const li = document.createElement("li");
      li.textContent = typeof item === "string" ? item : String(item?.name ?? item);
      ul.appendChild(li);
    }

    root.appendChild(ul);
  }

  const echarts = {
    init(el) {
      const root = el;

      return {
        setOption(option) {
          clear(root);
          renderTitle(root, option || {});
          renderLegend(root, option || {});
          const marker = document.createElement("div");
          marker.className = "echarts-stub";
          marker.textContent = "mock chart";
          root.appendChild(marker);
        },
        resize() {},
        dispose() {},
      };
    },
  };

  window.echarts = echarts;
})();
            """,
            encoding="utf-8",
        )

    monkeypatch.setattr(js_assets, "_download_with_ssl_bypass", fake_download)

    init_project(tmp_path)
    build_project(tmp_path)

    site_root = tmp_path / "static"
    data_map_path = site_root / "data_map.json"
    data_map: dict[str, str] = {}
    if data_map_path.exists():
        data_map = json.loads(data_map_path.read_text(encoding="utf-8"))

    class Handler(http.server.SimpleHTTPRequestHandler):
        directory = str(site_root)

        def translate_path(self, path: str) -> str:  # type: ignore[override]
            raw = path.split("?", 1)[0].split("#", 1)[0]
            cleaned = unquote(raw)
            key = cleaned.lstrip("/")
            if key in data_map:
                return data_map[key]
            return super().translate_path(path)

    port = 8098
    server = http.server.ThreadingHTTPServer(("localhost", port), Handler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    time.sleep(1.0)

    out_png = tmp_path / "gallery.png"
    try:
        subprocess.run(
            [
                "node",
                str(SNAPSHOT_HELPER),
                f"http://localhost:{port}/gallery/index.html",
                str(out_png),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:  # pragma: no cover - environment specific
        server.shutdown()
        pytest.skip(f"snapshot failed: {exc.stderr or exc.stdout}")

    assert out_png.exists()
    image = cv2.imread(str(out_png))
    server.shutdown()
    ocr_text = pytesseract.image_to_string(image).lower()

    assert "pie: share by category" in ocr_text
    assert "sankey: simple source" in ocr_text
    assert "a" in ocr_text and "b" in ocr_text
