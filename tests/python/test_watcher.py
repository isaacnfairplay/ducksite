from __future__ import annotations

from pathlib import Path

import ducksite.watcher as watcher


def test_watch_and_build_stops_when_config_removed(monkeypatch, tmp_path: Path) -> None:
    (tmp_path / "ducksite.toml").write_text("[dirs]", encoding="utf-8")

    snapshots = [
        {tmp_path / "ducksite.toml": 1.0},
        {},
    ]

    def fake_snapshot(root: Path) -> dict[Path, float]:  # noqa: ARG001
        return snapshots.pop(0) if snapshots else {}

    calls: list[bool] = []

    def fake_build(root: Path, clean: bool = False) -> None:  # noqa: ARG001
        calls.append(clean)
        if len(calls) > 1:
            raise FileNotFoundError("Config file not found")

    monkeypatch.setattr(watcher, "_snapshot_paths", fake_snapshot)
    monkeypatch.setattr(watcher, "build_project", fake_build)
    monkeypatch.setattr(watcher.time, "sleep", lambda _: None)

    watcher.watch_and_build(tmp_path, interval=0.01)

    assert len(calls) == 2
