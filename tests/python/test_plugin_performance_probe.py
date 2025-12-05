from __future__ import annotations

import pytest

from tools import plugin_performance_probe


@pytest.mark.slow
@pytest.mark.integration
def test_plugin_probe_reports_asset_and_dataset_timings() -> None:
    report = plugin_performance_probe.run_plugin_probe()

    assets = report["assets"]
    assert len(assets["cold"]) == 4
    assert len(assets["cached"]) == 4
    assert all(entry["duration_ms"] > 0 for entry in assets["cold"])

    datasets = report["datasets"]
    assert set(datasets) == {"static", "plugin"}

    static_cold = datasets["static"]["cold"]["duration_ms"]
    plugin_cold = datasets["plugin"]["cold"]["duration_ms"]
    assert plugin_cold <= static_cold * 3

    static_cached_status = datasets["static"]["cached"]["status"]
    plugin_cached_status = datasets["plugin"]["cached"]["status"]
    assert static_cached_status in {200, 304}
    assert plugin_cached_status in {200, 304}

    pareto = report["pareto"]
    assert pareto[0]["cold_ms"] >= pareto[-1]["cold_ms"]
    assert any(item["label"].endswith("index.html") for item in pareto)
    for entry in pareto:
        assert entry["action"]
        assert "impact_ms" in entry

    plugin_entry = next(item for item in pareto if item["label"] == "plugin parquet vs static")
    assert plugin_entry["action"] == "Keep plugin overhead within static bounds"
    assert plugin_entry["impact_ms"] == pytest.approx(
        plugin_cold - static_cold, rel=0.5
    )

    asset_cold = {entry["path"]: entry["duration_ms"] for entry in assets["cold"]}
    asset_cached = {entry["path"]: entry["duration_ms"] for entry in assets["cached"]}
    for asset_entry in (item for item in pareto if item["label"] in asset_cold):
        assert asset_entry["action"] == "Send If-Modified-Since for assets"
        expected = max(0.0, asset_cold[asset_entry["label"]] - asset_cached[asset_entry["label"]])
        assert asset_entry["impact_ms"] == pytest.approx(expected, rel=0.5)
