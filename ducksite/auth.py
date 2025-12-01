from __future__ import annotations

import hashlib
import hmac
import json
import os
from pathlib import Path
from typing import Dict

from .config import ProjectConfig
from .utils import ensure_dir

ALGORITHM = "pbkdf2_sha256"
ITERATIONS = 310_000
SALT_BYTES = 16


def _user_store_path(cfg: ProjectConfig) -> Path:
    return cfg.root / "auth" / "users.json"


def _load_user_store(cfg: ProjectConfig) -> Dict[str, str]:
    path = _user_store_path(cfg)
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    if not isinstance(data, dict):
        return {}
    return {str(k): str(v) for k, v in data.items()}


def _save_user_store(cfg: ProjectConfig, store: Dict[str, str]) -> None:
    path = _user_store_path(cfg)
    ensure_dir(path.parent)
    text = json.dumps(store, indent=2)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def hash_password(plain: str) -> str:
    salt = os.urandom(SALT_BYTES)
    dk = hashlib.pbkdf2_hmac(
        "sha256",
        plain.encode("utf-8"),
        salt,
        ITERATIONS,
    )
    return f"{ALGORITHM}${ITERATIONS}${salt.hex()}${dk.hex()}"


def verify_password(plain: str, encoded: str) -> bool:
    try:
        algo, iterations_s, salt_hex, hash_hex = encoded.split("$", 3)
        if algo != ALGORITHM:
            return False
        iterations = int(iterations_s)
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(hash_hex)
    except Exception:
        return False
    dk = hashlib.pbkdf2_hmac(
        "sha256",
        plain.encode("utf-8"),
        salt,
        iterations,
    )
    return hmac.compare_digest(dk, expected)


def ensure_initial_password(cfg: ProjectConfig, email: str, password: str) -> str:
    store = _load_user_store(cfg)
    email_key = email.strip().lower()
    if email_key not in store:
        if not password:
            raise ValueError("password required")
        store[email_key] = hash_password(password)
        _save_user_store(cfg, store)
        return "set"
    if not password:
        raise ValueError("unauthorized")
    if not verify_password(password, store[email_key]):
        raise ValueError("unauthorized")
    return "ok"


def update_password(cfg: ProjectConfig, email: str, old_password: str, new_password: str) -> None:
    email_key = email.strip().lower()
    store = _load_user_store(cfg)
    if email_key not in store:
        raise ValueError("unknown user")
    if not old_password or not verify_password(old_password, store[email_key]):
        raise ValueError("unauthorized")
    if not new_password or len(new_password) < 8:
        raise ValueError("weak password")
    store[email_key] = hash_password(new_password)
    _save_user_store(cfg, store)
