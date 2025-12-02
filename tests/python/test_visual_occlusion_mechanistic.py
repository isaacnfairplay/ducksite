import pytest

np = pytest.importorskip("numpy")
cv2 = pytest.importorskip("cv2")

from tests.layout_probe_utils import SNAPSHOT_HELPER, layout_probe_image


@pytest.mark.slow
@pytest.mark.skipif(
    not SNAPSHOT_HELPER.exists(),
    reason="snapshot helper missing",
)
def test_titles_do_not_overlap_legends(tmp_path: pytest.PathLike[str], monkeypatch: pytest.MonkeyPatch) -> None:
    with layout_probe_image(tmp_path, monkeypatch) as image:
        chart_boxes = _find_chart_boxes(image)
        assert len(chart_boxes) >= 3

        for x, y, w, h in chart_boxes:
            crop = image[y : y + h, x : x + w]
            components = _dark_components(crop)
            assert len(components) >= 2, "expected at least a title and legend"
            components.sort(key=lambda c: c[1])
            title = components[0]
            legend = components[1]
            assert title[1] + title[3] <= legend[1] - 2, "legend overlaps title"
            if len(components) > 2:
                third = components[2]
                assert legend[1] + legend[3] <= third[1] - 2, "content overlaps legend"


def _find_chart_boxes(image: np.ndarray) -> list[tuple[int, int, int, int]]:
    # Borders are light gray (#d1d5db) against a white background. OpenCV loads
    # images as BGR, so use the BGR tuple (219, 213, 209) with a small tolerance.
    lower = np.array([213, 207, 203], dtype=np.uint8)
    upper = np.array([225, 219, 215], dtype=np.uint8)
    mask = cv2.inRange(image, lower, upper)
    contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    boxes: list[tuple[int, int, int, int]] = []
    for cnt in contours:
        x, y, w, h = cv2.boundingRect(cnt)
        if w > 200 and h > 200:
            boxes.append((x, y, w, h))
    boxes.sort(key=lambda b: (b[1], b[0]))
    return boxes


def _dark_components(crop: np.ndarray) -> list[tuple[int, int, int, int]]:
    gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
    _, mask = cv2.threshold(gray, 240, 255, cv2.THRESH_BINARY_INV)
    num_labels, labels, stats, _ = cv2.connectedComponentsWithStats(mask, connectivity=8)
    components: list[tuple[int, int, int, int]] = []
    for idx in range(1, num_labels):
        x, y, w, h, area = stats[idx]
        if area < 30:
            continue
        components.append((int(x), int(y), int(w), int(h)))
    return components
