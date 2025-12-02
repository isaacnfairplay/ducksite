import pytest

cv2 = pytest.importorskip("cv2")
pytesseract = pytest.importorskip("pytesseract")

from tests.layout_probe_utils import SNAPSHOT_HELPER, layout_probe_image


@pytest.mark.slow
@pytest.mark.skipif(
    not SNAPSHOT_HELPER.exists(),
    reason="snapshot helper missing",
)
def test_gallery_titles_and_legends_visible(tmp_path: pytest.PathLike[str], monkeypatch: pytest.MonkeyPatch) -> None:
    with layout_probe_image(tmp_path, monkeypatch) as image:
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
