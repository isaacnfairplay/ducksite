from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Match, Optional
import re
import tomllib


DIR_VAR_PATTERN = re.compile(r"\$\{([A-Za-z0-9_]+)\}")


@dataclass
class FileSourceConfig:
    # Optional logical name for the base file-source view.
    # When set, a NamedQuery with this name is created that SELECTs from
    # the union of all matching Parquet files.
    name: Optional[str] = None

    # Optional template name used to generate *per-value* file-source views.
    #
    # Semantics:
    #   - If template_name is None, only the base `name` view is created.
    #   - If template_name contains a single `[ ... ]` segment, the content
    #     inside the brackets is treated as a valid SQL expression evaluated
    #     against each Parquet row (e.g. `[left(Barcode,10)]`).
    #   - During build, DuckDB is used to compute DISTINCT values of that
    #     expression across the file list, and for each value `v` a separate
    #     NamedQuery is created with a slugified suffix based on `v`.
    #
    # Example:
    #   template_name = "barcode_prefix_[left(Barcode,10)]"
    #
    #   → expression: left(Barcode,10)
    #   → base name: "barcode_prefix_"
    #   → distinct values: 'ACVCDEFG12', 'XYZ1234567', ...
    #   → views:
    #       barcode_prefix_ACVCDEFG12
    #       barcode_prefix_XYZ1234567
    template_name: Optional[str] = None

    # pattern is always relative to site_root ("static")
    pattern: str = "data/*.parquet"
    upstream_glob: Optional[str] = None
    union_mode: str = "union_all_by_name"
    time_window: Optional[Dict[str, Any]] = None

    # Optional static row filter applied to *all* rows for this file source.
    # When set, it is ANDed into both the base file-source view and any
    # templated per-value views.
    row_filter: Optional[str] = None

    # Optional template for building per-value predicates when template_name
    # is used.
    #
    # If template_name is set and row_filter_template is None, ducksite
    # defaults to:
    #   "<expr> = ?"
    # where <expr> is the SQL expression extracted from the [ ... ] segment
    # in template_name.
    #
    # If row_filter_template is provided, it must contain a single "?"
    # placeholder that will be replaced with a correctly quoted SQL literal
    # representing the distinct value seen for that expression.
    #
    # Example:
    #   template_name       = "barcode_prefix_[left(Barcode,10)]"
    #   row_filter_template = "left(Barcode,10) = ?"
    #
    #   distinct value v    = 'ACVCDEFG12'
    #   => predicate        = "left(Barcode,10) = 'ACVCDEFG12'"
    #
    # This predicate is ANDed with row_filter (if present).
    row_filter_template: Optional[str] = None

    # Behaviour when no matching Parquet files are found for this source.
    # Currently only "error" is recognised.
    on_empty: str = "error"


@dataclass
class ProjectConfig:
    root: Path
    dirs: Dict[str, str]
    file_sources: List[FileSourceConfig] = field(default_factory=list)
    content_dir: Path = field(init=False)
    sources_sql_dir: Path = field(init=False)
    site_root: Path = field(init=False)

    def __post_init__(self) -> None:
        self.content_dir = self.root / "content"
        self.sources_sql_dir = self.root / "sources_sql"
        # site_root is the only directory we serve
        self.site_root = self.root / "static"


def _substitute_dirs(value: str, dirs: Dict[str, str]) -> str:
    def repl(m: Match[str]) -> str:
        var = m.group(1)
        if not var.startswith("DIR_"):
            raise ValueError(f"Only DIR_* variables allowed, saw {var}")
        if var not in dirs:
            raise ValueError(f"Unknown DIR variable {var}")
        return dirs[var]

    return DIR_VAR_PATTERN.sub(repl, value)


def load_project_config(root: Path) -> ProjectConfig:
    cfg_path = root / "ducksite.toml"
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config file not found: {cfg_path}")

    with cfg_path.open("rb") as f:
        data = tomllib.load(f)

    dirs: Dict[str, str] = data.get("dirs", {}) or {}

    file_sources_cfg: List[FileSourceConfig] = []
    for fs in data.get("file_sources", []) or []:
        file_sources_cfg.append(
            FileSourceConfig(
                name=fs.get("name"),
                template_name=fs.get("template_name"),
                pattern=fs.get("pattern", "data/*.parquet"),
                upstream_glob=fs.get("upstream_glob"),
                union_mode=fs.get("union_mode", "union_all_by_name"),
                time_window=fs.get("time_window"),
                row_filter=fs.get("row_filter"),
                row_filter_template=fs.get("row_filter_template"),
                on_empty=fs.get("on_empty", "error"),
            )
        )

    cfg = ProjectConfig(root=root, dirs=dirs, file_sources=file_sources_cfg)

    for fs in cfg.file_sources:
        if fs.upstream_glob:
            fs.upstream_glob = _substitute_dirs(fs.upstream_glob, cfg.dirs)

    return cfg


if __name__ == "__main__":
    from pprint import pprint

    root = Path(".").resolve()
    try:
        cfg = load_project_config(root)
        pprint(cfg)
    except FileNotFoundError as e:
        print(e)
