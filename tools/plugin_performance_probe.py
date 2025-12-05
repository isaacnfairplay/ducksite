"""Probe plugin-backed parquet performance against static sources."""
from __future__ import annotations

import json
import sys
import tempfile
from dataclasses import asdict
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from ducksite import demo_init_fake_parquet
from ducksite.builder import build_project
from ducksite.demo_init_virtual_plugin import DEMO_PLUGIN_NAME
from ducksite.init_project import init_demo_project
from ducksite.sternum import AssetPath
from tools import performance_probe as base_probe


def _time_dataset(path: str, port: int) -> dict:
    first = base_probe._time_request(path, port)
    cached = base_probe._time_request(
        path,
        port,
        {"If-Modified-Since": first.last_modified} if first.last_modified else None,
    )
    return {"cold": asdict(first), "cached": asdict(cached)}


def run_plugin_probe() -> dict:
    base_probe._stub_echarts()
    demo_init_fake_parquet._download_nytaxi_parquet = lambda dest: False  # noqa: ARG005

    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        init_demo_project(root)
        build_project(root)

        port = base_probe._find_free_port()
        base_probe._start_server(root, port)

        asset_paths = [
            AssetPath.INDEX.value,
            AssetPath.CONTRACT_JS.value,
            AssetPath.ECHARTS_JS.value,
            AssetPath.DUCKDB_BUNDLE_JS.value,
        ]
        cold_assets = [base_probe._time_request(path, port) for path in asset_paths]
        cached_assets = [
            base_probe._time_request(
                path,
                port,
                {"If-Modified-Since": timing.last_modified}
                if timing.last_modified
                else None,
            )
            for timing, path in zip(cold_assets, asset_paths)
        ]

        dataset_timings = {
            "static": _time_dataset("/data/demo/demo-A.parquet", port),
            "plugin": _time_dataset(
                f"/data/{DEMO_PLUGIN_NAME}/demo-A.parquet",
                port,
            ),
        }

        def pareto() -> list[dict]:
            items = []
            for cold_timing, cached_timing in zip(cold_assets, cached_assets):
                items.append(
                    {
                        "label": cold_timing.path,
                        "action": "Send If-Modified-Since for assets",
                        "impact_ms": max(
                            0.0, cold_timing.duration_ms - cached_timing.duration_ms
                        ),
                        "cold_ms": cold_timing.duration_ms,
                        "cached_ms": cached_timing.duration_ms,
                    }
                )

            plugin_cold = dataset_timings["plugin"]["cold"]["duration_ms"]
            static_cold = dataset_timings["static"]["cold"]["duration_ms"]
            items.append(
                {
                    "label": "plugin parquet vs static",
                    "action": "Keep plugin overhead within static bounds",
                    "impact_ms": plugin_cold - static_cold,
                    "cold_ms": plugin_cold,
                    "cached_ms": dataset_timings["plugin"]["cached"]["duration_ms"],
                }
            )

            return sorted(items, key=lambda entry: entry["cold_ms"], reverse=True)

        return {
            "assets": {
                "cold": [asdict(t) for t in cold_assets],
                "cached": [asdict(t) for t in cached_assets],
            },
            "datasets": dataset_timings,
            "pareto": pareto(),
        }


if __name__ == "__main__":
    print(json.dumps(run_plugin_probe(), indent=2))
