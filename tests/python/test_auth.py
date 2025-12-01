import json
from pathlib import Path

import pytest

from ducksite.auth import ensure_initial_password, update_password, verify_password
from ducksite.config import ProjectConfig


def make_cfg(tmp_path: Path) -> ProjectConfig:
    return ProjectConfig(root=tmp_path, dirs={})


def test_update_password_round_trip(tmp_path: Path) -> None:
    cfg = make_cfg(tmp_path)
    status = ensure_initial_password(cfg, "u@example.com", "oldpass123")
    assert status == "set"

    update_password(cfg, "u@example.com", "oldpass123", "newpass123")
    with pytest.raises(ValueError):
        update_password(cfg, "u@example.com", "wrong", "another")

    store_path = cfg.root / "auth" / "users.json"
    data = json.loads(store_path.read_text(encoding="utf-8"))
    assert "u@example.com" in data
    encoded = data["u@example.com"]
    assert encoded.startswith("pbkdf2_sha256$310000$")
    assert verify_password("newpass123", encoded)
