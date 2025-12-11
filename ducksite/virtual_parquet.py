from __future__ import annotations

from dataclasses import dataclass
import os
from importlib import import_module
import importlib.util
from pathlib import Path
import sqlite3
import sys
from types import ModuleType
from typing import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager

import duckdb

from .config import ProjectConfig
from .data_map_cache import load_data_map, load_row_filters
from .data_map_paths import data_map_dir, data_map_sqlite_path
from .cte_compiler import compile_query, _find_read_parquet_paths
from .queries import NamedQuery, build_file_source_queries, load_model_queries
from .utils import ensure_dir


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


def _scandir_parquet(root: Path) -> list[os.DirEntry[str]]:
    entries: list[os.DirEntry[str]] = []
    if not root.is_dir():
        return entries

    with os.scandir(root) as it:
        for entry in it:
            if entry.is_file() and entry.name.lower().endswith(".parquet"):
                entries.append(entry)
    return entries


def _split_plugin_ref(raw: str) -> tuple[str, str]:
    if not raw:
        raise ValueError("plugin reference cannot be empty")
    module_ref, attr = raw, DEFAULT_PLUGIN_CALLABLE
    if ":" in raw:
        # Avoid treating a Windows drive letter (e.g., "C:\\path") as the
        # separator between the module and callable name.
        colon_index = raw.rfind(":")
        if not (
            colon_index == 1
            and raw[0].isalpha()
            and raw[2:3] in {"\\", "/"}
        ):
            module_ref, attr = raw.rsplit(":", 1)
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
def _prepend_sys_path(path: str) -> Iterator[None]:
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


def _http_remap(http_path: str, http_prefix: str | None) -> str:
    if not http_prefix:
        return http_path

    rel = Path(http_path.replace("\\", "/"))
    try:
        rel = rel.relative_to("data")
    except ValueError:
        pass

    return str(Path(http_prefix.rstrip("/")) / rel)


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


def manifest_from_file_source(
    cfg: ProjectConfig,
    file_source: str,
    *,
    http_prefix: str | None = None,
    data_map: Mapping[str, str] | None = None,
    row_filters: Mapping[str, str] | None = None,
) -> VirtualParquetManifest:
    """
    Build a manifest that re-exposes an existing file source under a new HTTP prefix.

    This is useful when chaining plugins: a downstream plugin can reuse the
    upstream file list without re-globbing storage.
    """

    lookup = data_map or load_data_map(cfg.site_root, shard_hint=file_source)
    filters = row_filters or load_row_filters(cfg.site_root)

    prefix = f"data/{file_source}/"
    files: list[VirtualParquetFile] = []
    for http_path, physical in lookup.items():
        if not http_path.startswith(prefix):
            continue

        files.append(
            VirtualParquetFile(
                http_path=_http_remap(http_path, http_prefix),
                physical_path=physical,
                row_filter=filters.get(http_path),
            )
        )

    if not files:
        raise ValueError(f"file source '{file_source}' has no files in the current data map")

    template_name: str | None = None
    row_filter_template: str | None = None
    for fs in cfg.file_sources:
        if fs.name == file_source:
            template_name = fs.template_name
            row_filter_template = fs.row_filter_template
            break

    return VirtualParquetManifest(
        files=files,
        template_name=template_name,
        row_filter_template=row_filter_template,
    )


def manifest_from_model_views(
    cfg: ProjectConfig,
    view_names: str | Sequence[str],
    *,
    http_prefix: str | None = None,
    queries: Mapping[str, NamedQuery] | None = None,
    data_map: Mapping[str, str] | None = None,
    row_filters: Mapping[str, str] | None = None,
) -> VirtualParquetManifest:
    """
    Compile one or more model views and expose their Parquet dependencies.

    The returned manifest lists every `read_parquet('data/...')` path
    referenced by the compiled view(s). When `http_prefix` is provided the
    logical HTTP paths are remapped under that prefix while preserving the
    relative directory structure of the original data paths.
    """

    view_list: list[str] = [view_names] if isinstance(view_names, str) else list(view_names)
    if not view_list:
        raise ValueError("at least one view name is required")

    all_queries: dict[str, NamedQuery] = {}
    if queries:
        all_queries.update(queries)
    else:
        all_queries.update(build_file_source_queries(cfg))
        all_queries.update(load_model_queries(cfg))

    con = duckdb.connect()
    try:
        compiled_paths: list[str] = []
        for view in view_list:
            compiled_sql, _, _ = compile_query(cfg.site_root, con, all_queries, view)
            compiled_paths.extend(_find_read_parquet_paths(compiled_sql))
    finally:
        try:
            con.close()
        except Exception:
            pass

    lookup = data_map or load_data_map(cfg.site_root)
    filters = row_filters or load_row_filters(cfg.site_root)
    view_label = view_names if isinstance(view_names, str) else ", ".join(view_list)

    files: list[VirtualParquetFile] = []
    for http_path in dict.fromkeys(compiled_paths):
        physical = lookup.get(http_path)
        if not physical:
            raise KeyError(
                f"view '{view_label}' references {http_path} but it is not present in the data map"
            )
        files.append(
            VirtualParquetFile(
                http_path=_http_remap(http_path, http_prefix),
                physical_path=physical,
                row_filter=filters.get(http_path),
            )
        )

    if not files:
        raise ValueError("compiled view(s) did not reference any Parquet files")

    return VirtualParquetManifest(files=files)


def manifest_from_parquet_dir(
    cfg: ProjectConfig,
    directory: str | Path,
    *,
    http_prefix: str,
    recursive: bool = False,
    template_name: str | None = None,
    row_filter_template: str | None = None,
) -> VirtualParquetManifest:
    """
    Build a manifest from a directory of Parquet files.

    Paths are remapped under ``http_prefix`` while preserving the relative
    layout beneath ``directory``. ``directory`` may be absolute or relative to
    the project root.
    """

    root = Path(directory)
    if not root.is_absolute():
        root = (cfg.root / root).resolve()

    if not root.exists():
        raise FileNotFoundError(f"parquet directory not found: {root}")
    if not root.is_dir():
        raise NotADirectoryError(root)

    http_base = Path(http_prefix.rstrip("/"))
    stack = [root]
    entries: list[os.DirEntry[str]] = []

    while stack:
        current = stack.pop()
        entries.extend(_scandir_parquet(current))
        if recursive:
            try:
                with os.scandir(current) as it:
                    for entry in it:
                        if entry.is_dir(follow_symlinks=False):
                            stack.append(Path(entry.path))
            except OSError:
                continue

    if not entries:
        raise ValueError(f"no parquet files found under {root}")

    files = [
        VirtualParquetFile(
            http_path=str(http_base / Path(entry.path).relative_to(root)),
            physical_path=str(Path(entry.path)),
        )
        for entry in entries
    ]

    files.sort(key=lambda f: f.http_path)
    return VirtualParquetManifest(
        files=files,
        template_name=template_name,
        row_filter_template=row_filter_template,
    )


def write_row_filter_meta(
    site_root: Path, filters: dict[str, str], fingerprints: dict[str, str] | None = None
) -> None:
    sqlite_path = data_map_sqlite_path(site_root)
    ensure_dir(sqlite_path.parent)

    con = sqlite3.connect(sqlite_path)
    try:
        con.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
        con.execute(
            "CREATE TABLE IF NOT EXISTS row_filters (http_path TEXT PRIMARY KEY, filter TEXT)"
        )
        con.execute("DELETE FROM row_filters")
        if filters:
            con.executemany(
                "INSERT INTO row_filters (http_path, filter) VALUES (?, ?)",
                ((str(k), str(v)) for k, v in filters.items()),
            )
        con.execute("DELETE FROM meta")
        if fingerprints:
            con.executemany(
                "INSERT INTO meta (key, value) VALUES (?, ?)",
                ((f"fingerprint:{k}", v) for k, v in fingerprints.items()),
            )
        con.commit()
    finally:
        con.close()

    legacy_meta_path = data_map_dir(site_root) / "data_map_meta.json"
    if legacy_meta_path.exists():
        legacy_meta_path.unlink()

