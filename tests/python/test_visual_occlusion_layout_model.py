import pytest

np = pytest.importorskip("numpy")
cv2 = pytest.importorskip("cv2")
lp = pytest.importorskip("layoutparser")

from tests.layout_probe_utils import SNAPSHOT_HELPER, layout_probe_image


@pytest.mark.slow
@pytest.mark.skipif(
    not SNAPSHOT_HELPER.exists(),
    reason="snapshot helper missing",
)
def test_layout_model_separates_titles(tmp_path: pytest.PathLike[str], monkeypatch: pytest.MonkeyPatch) -> None:
    model = _load_layout_model()
    with layout_probe_image(tmp_path, monkeypatch) as image:
        layout = model.detect(image[:, :, ::-1])  # convert BGR to RGB
        chart_boxes = _find_chart_boxes(image)
        assert len(chart_boxes) >= 3

        for x, y, w, h in chart_boxes:
            region = (x, y, x + w, y + h)
            blocks = [b for b in layout if _inside(region, b)]
            titles = [b for b in blocks if b.type.lower() == "title"]
            if not titles:
                pytest.skip("layout model did not return title blocks")
            title = sorted(titles, key=lambda b: b.block.y_1)[0]
            for block in blocks:
                if block is title:
                    continue
                assert not _overlap(title, block), "layout overlap detected"


def _load_layout_model():
    try:
        return lp.EfficientDetLayoutModel(
            "lp://PubLayNet/tf_efficientdet_d0/config",
            extra_config={"box_score_thresh": 0.35},
        )
    except Exception as exc:  # pragma: no cover - external dependency
        pytest.skip(f"layout model unavailable: {exc}")


def _find_chart_boxes(image):
    lower = np.array([200, 206, 212], dtype=np.uint8)
    upper = np.array([218, 222, 226], dtype=np.uint8)
    mask = cv2.inRange(image, lower, upper)
    contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    boxes: list[tuple[int, int, int, int]] = []
    for cnt in contours:
        x, y, w, h = cv2.boundingRect(cnt)
        if w > 200 and h > 200:
            boxes.append((x, y, w, h))
    boxes.sort(key=lambda b: (b[1], b[0]))
    return boxes


def _inside(region, block) -> bool:
    x0, y0, x1, y1 = region
    cx = (block.block.x_1 + block.block.x_2) / 2
    cy = (block.block.y_1 + block.block.y_2) / 2
    return x0 <= cx <= x1 and y0 <= cy <= y1


def _overlap(a, b) -> bool:
    ax0, ay0, ax1, ay1 = a.block.x_1, a.block.y_1, a.block.x_2, a.block.y_2
    bx0, by0, bx1, by1 = b.block.x_1, b.block.y_1, b.block.x_2, b.block.y_2
    return ax0 < bx1 and ax1 > bx0 and ay0 < by1 and ay1 > by0
