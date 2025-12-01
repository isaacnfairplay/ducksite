from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional
import json
import re
import duckdb

from .markdown_parser import parse_markdown_page
from .config import DIR_VAR_PATTERN, ProjectConfig, _substitute_dirs
from .utils import ensure_dir


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
    def from_dict(cls, data: Dict[str, object]) -> "FormSpec":
        raw_inputs = data.get("inputs", [])
        if isinstance(raw_inputs, str):
            try:
                raw_inputs = json.loads(raw_inputs)
            except Exception:
                raw_inputs = [raw_inputs]
        return cls(
            id=str(data.get("id")),
            label=data.get("label"),
            target_csv=str(data.get("target_csv")),
            inputs=list(raw_inputs),
            sql_relation_query=str(data.get("sql_relation_query", "")),
            image_field=data.get("image_field"),
            image_dir=data.get("image_dir"),
            auth_required=str(data.get("auth_required", "false")).lower() == "true",
            allowed_email_domains=data.get("allowed_email_domains"),
            max_rows_per_user=int(data.get("max_rows_per_user"))
            if data.get("max_rows_per_user") not in (None, "")
            else None,
        )

    def resolve_paths(self, cfg: ProjectConfig) -> "FormSpec":
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


def substitute_inputs(template: str, inputs: Dict[str, object]) -> str:
    def repl(match: re.Match) -> str:
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


def _build_dummy_inputs_for_form(form: FormSpec) -> Dict[str, object]:
    """
    Build a minimal dummy inputs dict for introspecting a form's SQL.

    We intentionally avoid any auth/domain checks here; this is only used
    at *build time* to discover the output columns of sql_relation_query
    so we can construct a header-only CSV stub.
    """
    dummy: Dict[str, object] = {}

    for name in form.inputs or []:
        dummy[name] = f"sample_{name}"

    # Common auth-related fields that may appear in sql_relation_query.
    dummy.setdefault("_user_email", "ducksite@example.com")
    dummy.setdefault("_user_id", "ducksite-demo")

    return dummy


def _ensure_form_csv_stub(cfg: ProjectConfig, form: FormSpec) -> None:
    """
    Ensure the target CSV for a form exists with at least a header row.

    This allows DuckDB EXPLAIN to successfully bind queries that read
    the CSV (e.g. via read_csv_auto('forms/feedback.csv', ...)) even
    before any real submissions have occurred.
    """
    resolved = form.resolve_paths(cfg)
    csv_path = Path(resolved.target_csv)

    if csv_path.exists():
        # Real data (or an existing stub) is already present.
        return

    dummy_inputs = _build_dummy_inputs_for_form(resolved)

    try:
        rows = evaluate_form_sql(resolved, dummy_inputs)
        cols = list(rows[0].keys()) if rows else []
    except Exception as e:
        # Best-effort: if we can't introspect the SQL, fall back to a single
        # generic column so DuckDB can still parse the CSV.
        print(
            f"[ducksite] WARNING: failed to introspect form '{resolved.id}' "
            f"sql_relation_query for stub CSV: {e}"
        )
        cols = []

    if not cols:
        cols = ["value"]

    header = ",".join(cols) + "\n"
    ensure_dir(csv_path.parent)
    csv_path.write_text(header, encoding="utf-8")
    print(f"[ducksite] created stub CSV for form '{resolved.id}' at {csv_path}")


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
            cnt = con.execute(
                f"SELECT COUNT(*) FROM existing WHERE {email_col} = ?", [user_email]
            ).fetchone()[0]
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

        con.execute(f"COPY existing TO '{csv_path}' (HEADER, DELIMITER ',')")
    finally:
        con.close()


def process_form_submission(
    cfg: ProjectConfig,
    form: FormSpec,
    payload: Dict[str, object],
    files: Optional[Dict[str, bytes]] = None,
) -> Dict[str, object]:
    inputs = payload.get("inputs") or {}
    user_email = inputs.get("_user_email")
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

    rows = evaluate_form_sql(resolved, inputs)
    if not rows:
        raise ValueError("Form query returned no rows")

    if resolved.image_field and files and resolved.image_field in files:
        image_bytes = files[resolved.image_field]
        fname = f"{resolved.id}_{len(image_bytes)}.bin"
        image_path = _save_image(Path(resolved.image_dir), fname, image_bytes)
        for row in rows:
            row[resolved.image_field] = str(image_path)

    append_rows_to_csv(
        Path(resolved.target_csv), rows, resolved.max_rows_per_user, user_email
    )
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
            # Ensure the backing CSV exists (header-only stub) so build-time
            # DuckDB EXPLAIN calls do not fail on missing files.
            try:
                _ensure_form_csv_stub(cfg, spec)
            except Exception as e:
                print(
                    f"[ducksite] WARNING: failed to ensure CSV stub for form '{spec.id}': {e}"
                )
    return forms
