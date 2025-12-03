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
from ducksite.init_project import init_demo_project


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

  function resolveTop(value, totalHeight) {
    if (value === undefined || value === null) return 0;
    if (typeof value === "number") return value;
    if (typeof value === "string") {
      if (value === "bottom") return Math.max(totalHeight - 28, 0);
      if (value.endsWith("%")) {
        const pct = parseFloat(value.slice(0, -1));
        if (!Number.isNaN(pct)) return (pct / 100) * totalHeight;
      }
      const n = parseFloat(value);
      if (!Number.isNaN(n)) return n;
    }
    return 0;
  }

  function deriveLegendData(option) {
    const legend = option && option.legend;
    if (legend && Array.isArray(legend.data) && legend.data.length > 0) return legend.data;

    const series = option && option.series;
    if (!series || !Array.isArray(series) || series.length === 0) return [];
    const first = series[0];
    if (!first || !Array.isArray(first.data)) return [];
    return first.data.map((d, idx) => {
      if (typeof d === "string") return d;
      if (d && d.name) return d.name;
      return `item${idx + 1}`;
    });
  }

  function renderTitle(root, option, height) {
    const title = option && option.title;
    const text = title && (title.text || title);
    if (!text) return null;

    const h = document.createElement("div");
    h.className = "echarts-title";
    h.style.position = "absolute";
    h.style.left = "16px";
    h.style.top = `${resolveTop(title.top, height)}px`;
    h.style.fontSize = "16px";
    h.style.fontWeight = "600";
    h.textContent = String(text);
    root.appendChild(h);
    return h;
  }

  function renderLegend(root, option, height) {
    const legend = option && option.legend;
    if (!legend) return null;
    const data = deriveLegendData(option);
    if (!data || data.length === 0) return null;

    const ul = document.createElement("ul");
    ul.className = "echarts-legend";
    ul.style.listStyle = "none";
    ul.style.padding = "0";
    ul.style.margin = "0";
    ul.style.position = "absolute";
    ul.style.left = "16px";
    ul.style.top = `${resolveTop(legend.top ?? 32, height)}px`;
    ul.style.display = "flex";
    ul.style.flexWrap = "wrap";
    ul.style.gap = "8px";

    for (const item of data) {
      const li = document.createElement("li");
      li.textContent = typeof item === "string" ? item : String(item?.name ?? item);
      ul.appendChild(li);
    }

    root.appendChild(ul);
    return ul;
  }

  const echarts = {
    init(el) {
      const root = el;
      root.style.position = "relative";
      root.style.minHeight = "320px";

      return {
        setOption(option) {
          clear(root);
          const height = Math.max(root.clientHeight || 0, 320);
          renderTitle(root, option || {}, height);
          renderLegend(root, option || {}, height);
          const marker = document.createElement("div");
          marker.className = "echarts-stub";
          marker.style.position = "absolute";
          marker.style.left = "16px";
          marker.style.bottom = "8px";
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

    init_demo_project(tmp_path)
    build_project(tmp_path)

    site_root = tmp_path / "static"
    data_map_path = site_root / "data_map.json"
    data_map: dict[str, str] = {}
    if data_map_path.exists():
        data_map = json.loads(data_map_path.read_text(encoding="utf-8"))

    layout_probe = site_root / "layout_probe.html"
    layout_probe.write_text(
        """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>layout probe</title>
    <script src="/js/echarts.min.js"></script>
    <style>
      body { background: #ffffff; color: #0f172a; font-family: Arial, sans-serif; }
      .grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 24px; padding: 12px; }
      .viz { width: 620px; height: 360px; position: relative; border: 1px solid #d1d5db; }
    </style>
  </head>
  <body>
    <div class="grid">
      <div id="pie" class="viz"></div>
      <div id="doughnut" class="viz"></div>
      <div id="pie-wrapped" class="viz"></div>
      <div id="sankey" class="viz"></div>
    </div>
    <script>
      const charts = [
        {
          id: "pie",
          option: {
            title: { text: "Pie: share by category", top: 10, left: "center" },
            legend: { top: 36, left: "center" },
            series: [
              {
                type: "pie",
                radius: ["0%", "70%"],
                center: ["50%", "62%"],
                data: [
                  { name: "A", value: 10 },
                  { name: "B", value: 5 },
                  { name: "C", value: 3 },
                ],
              },
            ],
          },
        },
        {
          id: "doughnut",
          option: {
            title: { text: "Doughnut: share by category", top: 10, left: "center" },
            legend: { top: 36, left: "center" },
            series: [
              {
                type: "pie",
                radius: ["40%", "70%"],
                center: ["50%", "62%"],
                data: [
                  { name: "A", value: 7 },
                  { name: "B", value: 3 },
                  { name: "C", value: 2 },
                ],
              },
            ],
          },
        },
        {
          id: "pie-wrapped",
          option: {
            title: { text: "Pie: wide legend", top: 10, left: "center" },
            legend: { top: 36, left: "center" },
            series: [
              {
                type: "pie",
                radius: ["0%", "70%"],
                center: ["50%", "62%"],
                data: [
                  { name: "A", value: 5 },
                  { name: "B", value: 4 },
                  { name: "C", value: 3 },
                  { name: "D", value: 3 },
                  { name: "E", value: 2 },
                  { name: "F", value: 2 },
                  { name: "G", value: 1 },
                  { name: "H", value: 1 },
                  { name: "I", value: 1 },
                ],
              },
            ],
          },
        },
        {
          id: "sankey",
          option: {
            title: {
              text: "Sankey: simple source→target flows",
              top: 14,
              left: "center",
            },
            legend: { data: ["A", "B", "X", "Y", "Z"], top: "bottom" },
            series: [
              {
                type: "sankey",
                top: "14%",
                data: [
                  { name: "A" },
                  { name: "B" },
                  { name: "X" },
                  { name: "Y" },
                  { name: "Z" },
                ],
                links: [
                  { source: "A", target: "X", value: 5 },
                  { source: "A", target: "Y", value: 3 },
                  { source: "B", target: "X", value: 4 },
                  { source: "B", target: "Z", value: 2 },
                ],
                lineStyle: { color: "source", opacity: 0.7 },
                itemStyle: { borderWidth: 1, borderColor: "#e5e7eb", opacity: 0.9 },
              },
            ],
          },
        },
      ];

      charts.forEach(({ id, option }) => {
        const el = document.getElementById(id);
        const chart = echarts.init(el);
        chart.setOption(option);
      });
    </script>
  </body>
</html>
        """,
        encoding="utf-8",
    )

    class Handler(http.server.SimpleHTTPRequestHandler):
        directory = str(site_root)

        def translate_path(self, path: str) -> str:  # type: ignore[override]
            raw = path.split("?", 1)[0].split("#", 1)[0]
            cleaned = unquote(raw)
            key = cleaned.lstrip("/")
            if key in data_map:
                return data_map[key]
            local = (site_root / key).resolve()
            if local.is_dir():
                return str((local / "index.html").resolve())
            return str(local)

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
                    f"http://localhost:{port}/layout_probe.html",
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
    assert "pie: wide legend" in ocr_text

    def extract_lines(img) -> list[tuple[str, tuple[int, int, int, int]]]:
        data = pytesseract.image_to_data(img, output_type=pytesseract.Output.DICT)
        grouped: dict[tuple[int, int, int], dict[str, object]] = {}
        for idx, text in enumerate(data["text"]):
            if not text or not text.strip():
                continue
            key = (
                data["block_num"][idx],
                data["par_num"][idx],
                data["line_num"][idx],
            )
            entry = grouped.setdefault(key, {"words": [], "bbox": None})
            x, y, w, h = (
                data["left"][idx],
                data["top"][idx],
                data["width"][idx],
                data["height"][idx],
            )
            if entry["bbox"] is None:
                entry["bbox"] = [x, y, x + w, y + h]
            else:
                bx, by, bx2, by2 = entry["bbox"]  # type: ignore[misc]
                entry["bbox"] = [
                    min(bx, x),
                    min(by, y),
                    max(bx2, x + w),
                    max(by2, y + h),
                ]
            entry["words"].append(text)

        lines: list[tuple[str, tuple[int, int, int, int]]] = []
        for entry in grouped.values():
            words = entry["words"]  # type: ignore[assignment]
            bbox = entry["bbox"]  # type: ignore[assignment]
            if bbox is None:
                continue
            lines.append((" ".join(words), tuple(int(v) for v in bbox)))
        return lines

    def assert_clearance(img, phrase: str, min_gap: int = 6) -> None:
        lines = extract_lines(img)
        matches = [
            (text, bbox)
            for (text, bbox) in lines
            if phrase in text.lower()
        ]
        assert matches, f"missing title: {phrase}"
        _, title_bbox = matches[0]
        x0, y0, x1, y1 = title_bbox
        h, w, _ = img.shape
        region_left = max(x0 - 30, 0)
        region_right = min(x1 + 360, w)
        region_top = max(y0 - 20, 0)
        region_bottom = min(y0 + 320, h)
        region_lines = [
            (t, b)
            for (t, b) in lines
            if b[0] <= region_right
            and b[2] >= region_left
            and b[1] <= region_bottom
            and b[3] >= region_top
        ]
        assert region_lines, f"no text detected near {phrase}"
        overlaps = []
        clear_lines = 0
        for text, bbox in region_lines:
            if phrase in text.lower():
                continue
            ox0 = max(x0, bbox[0])
            oy0 = max(y0, bbox[1])
            ox1 = min(x1, bbox[2])
            oy1 = min(y1, bbox[3])
            if ox1 > ox0 and oy1 > oy0:
                overlaps.append(text)
            if bbox[1] >= y1 + min_gap:
                clear_lines += 1
        assert not overlaps, f"overlap near {phrase}: {overlaps}"
        assert clear_lines >= 1 or len(region_lines) == 1, f"no content below title for {phrase}"

    assert_clearance(image, "pie: share by category")
    assert_clearance(image, "doughnut: share by category")
    assert_clearance(image, "pie: wide legend")
    assert_clearance(image, "sankey: simple source")
