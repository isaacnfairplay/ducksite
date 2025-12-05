from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
import importlib.util
import json
from pathlib import Path
import sys
from types import ModuleType
from typing import Callable, Iterable
from contextlib import contextmanager

from .config import ProjectConfig


DEFAULT_PLUGIN_CALLABLE = "build_manifest"


@dataclass
class VirtualParquetFile:
    http_path: str
    physical_path: str
    row_filter: str | None = None


@dataclass
class VirtualParquetManifest:
    files: list[VirtualParquetFile]
    template_name: str | None = None
    row_filter_template: str | None = None


def _split_plugin_ref(raw: str) -> tuple[str, str]:
    if not raw:
        raise ValueError("plugin reference cannot be empty")
    if ":" in raw:
        module_ref, attr = raw.rsplit(":", 1)
    else:
        module_ref, attr = raw, DEFAULT_PLUGIN_CALLABLE
    if not module_ref or not attr:
        raise ValueError(f"invalid plugin reference: {raw}")
    return module_ref, attr


def _module_from_path(module_ref: str, project_root: Path) -> ModuleType:
    path = Path(module_ref)
    if not path.is_absolute():
        path = (project_root / path).resolve()

    if not path.exists():
        raise FileNotFoundError(f"plugin file not found: {path}")

    spec = importlib.util.spec_from_file_location(
        f"ducksite_plugin_{hash(path)}", path
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"unable to load plugin from {path}")

    module = importlib.util.module_from_spec(spec)
    with _prepend_sys_path(str(path.parent)):
        spec.loader.exec_module(module)
    return module


def _import_plugin_module(module_ref: str, project_root: Path) -> ModuleType:
    # Treat anything that looks like a path as a path-based module load.
    if Path(module_ref).suffix == ".py" or "/" in module_ref or "\\" in module_ref:
        return _module_from_path(module_ref, project_root)
    # Otherwise assume an importable module path.
    with _prepend_sys_path(str(project_root)):
        return import_module(module_ref)


@contextmanager
def _prepend_sys_path(path: str):
    sys_path_was = list(sys.path)
    if path not in sys.path:
        sys.path.insert(0, path)
    try:
        yield
    finally:
        sys.path[:] = sys_path_was


def _ensure_virtual_file_paths(files: Iterable[VirtualParquetFile], project_root: Path) -> list[VirtualParquetFile]:
    normalized: list[VirtualParquetFile] = []
    for f in files:
        http_path = f.http_path.replace("\\", "/")
        physical = Path(f.physical_path)
        if not physical.is_absolute():
            physical = (project_root / physical).resolve()
        normalized.append(
            VirtualParquetFile(
                http_path=http_path,
                physical_path=str(physical),
                row_filter=f.row_filter,
            )
        )
    return normalized


def load_virtual_parquet_manifest(plugin_ref: str, cfg: ProjectConfig) -> VirtualParquetManifest:
    module_ref, attr = _split_plugin_ref(plugin_ref)
    module = _import_plugin_module(module_ref, cfg.root)
    try:
        func: Callable[[ProjectConfig], VirtualParquetManifest] = getattr(module, attr)
    except AttributeError as exc:
        raise ImportError(
            f"virtual parquet plugin target '{attr}' not found in {module_ref!r}"
        ) from exc
    if not callable(func):
        raise TypeError(
            f"virtual parquet plugin target '{attr}' from {module_ref!r} is not callable"
        )
    manifest = func(cfg)
    if not isinstance(manifest, VirtualParquetManifest):
        raise TypeError(
            "virtual parquet plugin must return VirtualParquetManifest; "
            f"got {type(manifest)!r}"
        )
    files = _ensure_virtual_file_paths(manifest.files, cfg.root)
    return VirtualParquetManifest(
        files=files,
        template_name=manifest.template_name,
        row_filter_template=manifest.row_filter_template,
    )


def write_row_filter_meta(site_root: Path, filters: dict[str, str]) -> None:
    if not filters:
        meta_path = site_root / "data_map_meta.json"
        if meta_path.exists():
            meta_path.unlink()
        return
    meta_path = site_root / "data_map_meta.json"
    meta_path.write_text(json.dumps({"row_filters": filters}, indent=2), encoding="utf-8")

