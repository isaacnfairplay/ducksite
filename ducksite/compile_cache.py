from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Dict, Any

import json

from .cte_compiler import NetworkMetrics
from .utils import sha256_list

_CACHE_FILENAME = ".ducksite_compile_cache.json"


def _cache_path(project_root: Path) -> Path:
    return project_root / _CACHE_FILENAME


def load_compile_cache(project_root: Path) -> Dict[str, Dict[str, Any]]:
    path = _cache_path(project_root)
    if not path.exists():
        return {}

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        print(f"[ducksite] WARNING: failed to load compile cache {path}: {e}")
        return {}

    if not isinstance(raw, dict):
        return {}

    return {k: v for k, v in raw.items() if isinstance(v, dict)}


def save_compile_cache(project_root: Path, cache: Dict[str, Dict[str, Any]]) -> None:
    path = _cache_path(project_root)
    try:
        path.write_text(json.dumps(cache, indent=2), encoding="utf-8")
    except OSError as e:
        print(f"[ducksite] WARNING: failed to write compile cache {path}: {e}")


def record_compiled_query(
    cache: Dict[str, Dict[str, Any]],
    name: str,
    signature: str,
    compiled_sql: str,
    metrics: NetworkMetrics,
    deps: list[str],
) -> None:
    cache[name] = {
        "signature": signature,
        "compiled_sql": compiled_sql,
        "metrics": asdict(metrics),
        "deps": deps,
    }


def cache_signature(sql_hash: str, fingerprint_token: str) -> str:
    return sha256_list([sql_hash, fingerprint_token])
