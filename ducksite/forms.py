from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Match, Optional
import json
import re
import duckdb
import datetime

from .markdown_parser import parse_markdown_page
from .config import DIR_VAR_PATTERN, ProjectConfig, _substitute_dirs
from .utils import ensure_dir, sha256_text


@dataclass
class FormSpec:
    id: str
    label: Optional[str]
    target_csv: str
    inputs: List[str]
    sql_relation_query: str
    image_field: Optional[str] = None
    image_dir: Optional[str] = None
    auth_required: bool = False
    allowed_email_domains: Optional[str] = None
    max_rows_per_user: Optional[int] = None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "FormSpec":
        raw_inputs_obj = data.get("inputs")
        if isinstance(raw_inputs_obj, str):
            try:
                raw_inputs_obj = json.loads(raw_inputs_obj)
            except Exception:
                raw_inputs_obj = [raw_inputs_obj]

        raw_inputs: List[str]
        if isinstance(raw_inputs_obj, list):
            raw_inputs = [str(v) for v in raw_inputs_obj]
        elif raw_inputs_obj is None:
            raw_inputs = []
        else:
            try:
                raw_inputs = [str(v) for v in list(raw_inputs_obj)]
            except Exception:
                raw_inputs = []

        label = data.get("label")
        image_field = data.get("image_field")
        image_dir = data.get("image_dir")
        allowed_email_domains = data.get("allowed_email_domains")
        max_rows_raw = data.get("max_rows_per_user")
        if isinstance(max_rows_raw, (int, float)):
            max_rows = int(max_rows_raw)
        elif isinstance(max_rows_raw, str) and max_rows_raw != "":
            max_rows = int(max_rows_raw)
        else:
            max_rows = None

        return cls(
            id=str(data.get("id", "")),
            label=str(label) if label is not None else None,
            target_csv=str(data.get("target_csv", "")),
            inputs=raw_inputs,
            sql_relation_query=str(data.get("sql_relation_query", "")),
            image_field=str(image_field) if image_field is not None else None,
            image_dir=str(image_dir) if image_dir is not None else None,
            auth_required=str(data.get("auth_required", "false")).lower() == "true",
            allowed_email_domains=
                str(allowed_email_domains) if allowed_email_domains is not None else None,
            max_rows_per_user=max_rows,
        )

    def resolve_paths(self, cfg: ProjectConfig) -> "FormSpec":
        """
        Apply ${DIR_*} substitutions to target_csv/image_dir but keep them as
        logical paths. We only join to cfg.root when we actually touch the FS.
        """
        target = (
            _substitute_dirs(self.target_csv, cfg.dirs)
            if DIR_VAR_PATTERN.search(self.target_csv)
            else self.target_csv
        )
        image_dir = None
        if self.image_dir:
            image_dir = (
                _substitute_dirs(self.image_dir, cfg.dirs)
                if DIR_VAR_PATTERN.search(self.image_dir)
                else self.image_dir
            )
        return FormSpec(
            id=self.id,
            label=self.label,
            target_csv=target,
            inputs=self.inputs,
            sql_relation_query=self.sql_relation_query,
            image_field=self.image_field,
            image_dir=image_dir,
            auth_required=self.auth_required,
            allowed_email_domains=self.allowed_email_domains,
            max_rows_per_user=self.max_rows_per_user,
        )


def _absolute_under_root(cfg: ProjectConfig, logical_path: str | None) -> Path:
    """
    Resolve a logical (possibly relative) path under cfg.root.

    - If logical_path is absolute, return it as-is.
    - If logical_path is relative, treat it as cfg.root / logical_path.
    - If logical_path is None/empty, fall back to cfg.root.
    """
    if not logical_path:
        return cfg.root
    p = Path(logical_path)
    if p.is_absolute():
        return p
    return cfg.root / p


def substitute_inputs(template: str, inputs: Dict[str, object]) -> str:
    def repl(match: Match[str]) -> str:
        key = match.group(1)
        val = inputs.get(key)
        if val is None:
            return "NULL"
        escaped = str(val).replace("'", "''")
        return f"'{escaped}'"

    return re.sub(r"\$\{inputs\.([A-Za-z0-9_]+)\}", repl, template)


def evaluate_form_sql(form: FormSpec, inputs: Dict[str, object]) -> List[Dict[str, object]]:
    sql = substitute_inputs(form.sql_relation_query, inputs)
    con = duckdb.connect()
    try:
        res = con.execute(sql).fetchall()
        cols = [c[0] for c in con.description]
        rows = [dict(zip(cols, r)) for r in res]
        return rows
    finally:
        con.close()


# --- Schema inference helpers (no DuckDB execution) -------------------------

_ALIAS_RE = re.compile(r"\bAS\s+([A-Za-z_][A-Za-z0-9_]*)\b", re.IGNORECASE)


def _infer_columns_from_sql(sql: str) -> List[str]:
    """
    Infer output column names from a SELECT ... AS col expression list.

    We keep this intentionally dumb but robust:
      - Scan for 'AS <identifier>' tokens.
      - Preserve first-seen order, de-duplicate later occurrences.
      - If nothing is found, fall back to ['value'].
    """
    cols: List[str] = []
    seen: set[str] = set()
    for name in _ALIAS_RE.findall(sql or ""):
        if name not in seen:
            seen.add(name)
            cols.append(name)
    if not cols:
        cols = ["value"]
    return cols


def _with_enrichment_columns(cols: List[str]) -> List[str]:
    """
    Ensure the submitter/time enrichment columns are present in the schema.
    """
    out = list(cols)
    if "submitted_by" not in out:
        out.append("submitted_by")
    if "submitted_at" not in out:
        out.append("submitted_at")
    return out


def _schema_hash(cols: List[str]) -> str:
    """
    Compute a short, stable schema hash from the ordered column list.
    """
    return sha256_text("\n".join(cols))[:8]


# --- Runtime helpers --------------------------------------------------------


def _save_image(image_dir: Path, filename: str, data: bytes) -> Path:
    ensure_dir(image_dir)
    target = image_dir / filename
    target.write_bytes(data)
    return target


def append_rows_to_csv(
    csv_path: Path,
    rows: List[Dict[str, object]],
    max_rows_per_user: Optional[int] = None,
    user_email: Optional[str] = None,
) -> None:
    """
    Append rows to csv_path, evolving schema as needed, and also write a
    schema-versioned snapshot:

        <stem>__schema_<hash>.csv

    where <hash> is based on the final ordered column list.

    If multiple schema versions exist for the same base, we emit a warning
    and an example SQL pattern to inspect / merge them.
    """
    ensure_dir(csv_path.parent)
    con = duckdb.connect()
    try:
        has_existing = csv_path.exists()
        if has_existing:
            con.execute(
                f"CREATE OR REPLACE TABLE existing AS SELECT * FROM read_csv_auto('{csv_path}')"
            )
            existing_cols = [r[1] for r in con.execute("PRAGMA table_info('existing')").fetchall()]
        else:
            sample_cols = list(rows[0].keys()) if rows else []
            if sample_cols:
                cols_sql = ", ".join(f"{c} VARCHAR" for c in sample_cols)
            else:
                cols_sql = "stub VARCHAR"
            con.execute(f"CREATE OR REPLACE TABLE existing ({cols_sql})")
            existing_cols = sample_cols

        new_cols = set(existing_cols)
        for row in rows:
            new_cols.update(row.keys())

        for col in new_cols:
            if col not in existing_cols:
                con.execute(f"ALTER TABLE existing ADD COLUMN {col} VARCHAR")

        email_col = None
        if user_email:
            for candidate in ("submitted_by", "user_email", "email"):
                if any(c.lower() == candidate for c in new_cols):
                    email_col = candidate
                    break
        if max_rows_per_user and email_col and user_email:
            result = con.execute(
                f"SELECT COUNT(*) FROM existing WHERE {email_col} = ?", [user_email]
            ).fetchone()
            cnt = int(result[0]) if result else 0
            if cnt >= max_rows_per_user:
                raise ValueError("max_rows_per_user exceeded")

        ordered_cols = list(new_cols)
        for row in rows:
            values = [row.get(col) for col in ordered_cols]
            placeholders = ", ".join(["?" for _ in ordered_cols])
            col_list = ", ".join(ordered_cols)
            con.execute(
                f"INSERT INTO existing ({col_list}) VALUES ({placeholders})",
                values,
            )

        # Canonical CSV (union of all data)
        con.execute(f"COPY existing TO '{csv_path}' (HEADER, DELIMITER ',')")

        # Schema-hash snapshot
        schema_id = _schema_hash(ordered_cols)
        stem = csv_path.stem
        suffix = csv_path.suffix
        hashed_name = f"{stem}__schema_{schema_id}{suffix}"
        hashed_path = csv_path.with_name(hashed_name)
        con.execute(f"COPY existing TO '{hashed_path}' (HEADER, DELIMITER ',')")

        # Warn if multiple schema versions exist
        pattern = f"{stem}__schema_*.{suffix.lstrip('.')}"
        variants = sorted(csv_path.parent.glob(pattern))
        if len(variants) > 1:
            print(f"[ducksite] WARNING: multiple schema versions detected for {csv_path.name}:")
            for p in variants:
                print(f"  - {p.name}")
            if len(variants) >= 2:
                a, b = variants[0].name, variants[1].name
                print("[ducksite] Example SQL to inspect/merge two versions:")
                print(
                    f"  SELECT * FROM read_csv_auto('{a}', HEADER=TRUE, ALL_VARCHAR=TRUE)\n"
                    f"  UNION ALL\n"
                    f"  SELECT * FROM read_csv_auto('{b}', HEADER=TRUE, ALL_VARCHAR=TRUE);"
                )
    finally:
        con.close()


def process_form_submission(
    cfg: ProjectConfig,
    form: FormSpec,
    payload: Dict[str, object],
    files: Optional[Dict[str, bytes]] = None,
) -> Dict[str, object]:
    """
    Process a form submission:

      - Validate auth / domain constraints.
      - Evaluate form.sql_relation_query.
      - Enrich with server-side metadata:
          submitted_by, submitted_at (UTC ISO8601).
      - Append to CSV (canonical + schema-hash snapshot).
    """
    raw_inputs = payload.get("inputs")
    if isinstance(raw_inputs, dict):
        inputs: Dict[str, object] = {str(k): v for k, v in raw_inputs.items()}
    else:
        inputs = {}
    user_email_val = inputs.get("_user_email")
    user_email = str(user_email_val) if user_email_val is not None else None
    allowed_domains: List[str] = []
    if form.allowed_email_domains:
        parts = re.split(r"[,\s]+", str(form.allowed_email_domains))
        allowed_domains = [p.lstrip("@").lower() for p in parts if p.strip()]

    if form.auth_required and not user_email:
        raise ValueError("authentication required")
    if form.max_rows_per_user and not user_email:
        raise ValueError("user email required")
    if allowed_domains:
        if not user_email:
            raise ValueError("user email required")
        domain = str(user_email).split("@")[-1].lower()
        if domain not in allowed_domains:
            raise ValueError("email domain not allowed")

    resolved = form.resolve_paths(cfg)

    # Evaluate the user-defined relation.
    rows = evaluate_form_sql(resolved, inputs)
    if not rows:
        raise ValueError("Form query returned no rows")

    # Server-side enrichment: submitter + timestamp.
    # We overwrite any columns with the same names to ensure these are
    # trustworthy from the server's perspective.
    submitter = user_email or ""
    submitted_at = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    for row in rows:
        row["submitted_by"] = submitter
        row["submitted_at"] = submitted_at

    # Optional image handling.
    if resolved.image_field and files and resolved.image_field in files:
        image_bytes = files[resolved.image_field]
        fname = f"{resolved.id}_{len(image_bytes)}.bin"
        image_dir = _absolute_under_root(cfg, resolved.image_dir)
        image_path = _save_image(image_dir, fname, image_bytes)
        for row in rows:
            row[resolved.image_field] = str(image_path)

    csv_path = _absolute_under_root(cfg, resolved.target_csv)
    append_rows_to_csv(csv_path, rows, resolved.max_rows_per_user, user_email)
    return {"status": "ok", "rows_appended": len(rows)}


def discover_forms(cfg: ProjectConfig) -> Dict[str, FormSpec]:
    forms: Dict[str, FormSpec] = {}
    if not cfg.content_dir.exists():
        return forms

    for md_path in cfg.content_dir.rglob("*.md"):
        pq = parse_markdown_page(md_path, md_path.parent)
        for raw in pq.form_defs:
            spec = FormSpec.from_dict(raw)
            forms[spec.id] = spec
    return forms


def ensure_form_target_csvs(cfg: ProjectConfig) -> None:
    """
    Ensure a header-only CSV exists for each form's target_csv so that
    build-time EXPLAINs over read_csv_auto(...) succeed even before any
    submissions have been made.

    The schema is:

      - Columns inferred from sql_relation_query (AS aliases)
      - Plus: submitted_by, submitted_at
    """
    forms = discover_forms(cfg)
    if not forms:
        return

    for spec in forms.values():
        resolved = spec.resolve_paths(cfg)
        csv_path = _absolute_under_root(cfg, resolved.target_csv)

        if csv_path.exists():
            continue

        cols = _infer_columns_from_sql(resolved.sql_relation_query)
        cols = _with_enrichment_columns(cols)

        ensure_dir(csv_path.parent)
        header = ",".join(cols) + "\n"
        csv_path.write_text(header, encoding="utf-8")
        print(f"[ducksite] created stub CSV for form '{spec.id}' at {csv_path}")
