from __future__ import annotations
from pathlib import Path
from typing import Dict, List, Tuple
import re
import fnmatch
import sys
import subprocess

# ============================
# LANGUAGE / EXTENSION CONTRACTS
# ============================
LANG_BY_EXT: Dict[str, set[str]] = {
    "py": {"python", "py"},
    "js": {"javascript", "js"},
    "ts": {"typescript", "ts"},
    "css": {"css"},
    "html": {"html"},
    "toml": {"toml"},
    "sql": {"sql"},
    "txt": {"text"},
}

EXT_WHITELIST = set(LANG_BY_EXT.keys()) | {"md"}

# Represent a fenced block opener as a string constant so we never have
# literal triple-backticks in this file (which keeps ChatGPT markdown sane).
TRIPLE_BACKTICK = "`" * 3

# Extremely permissive path pattern
PATH_RE = re.compile(r"(?P<path>[A-Za-z0-9_\-./\\ ]+?\.[A-Za-z0-9_]+)")

# ============================
# AI RESPONSE NOTE
# ============================
AI_RESPONSE_NOTE = f"""
---
NOTE FOR AI MODELS CONSUMING THIS FILE

When you propose code changes to this repository, always answer using the
*scaffold* format shown here. Each file you want to create or modify
should be emitted as:
 <Some comments on the problem we are solvig in plain english>
 <Somme comments on our decision making process in plain english>
 <Some comments on the changes in plain english>
  <relative/path/from/repo/root>
  {TRIPLE_BACKTICK}<ext>
  <full file contents>
  {TRIPLE_BACKTICK}

Examples:

  ducksite/init_project.py
  {TRIPLE_BACKTICK}py
  from __future__ import annotations
  from pathlib import Path
  ...
  {TRIPLE_BACKTICK}

  demo_src/content/filter_demo/index.md
  {TRIPLE_BACKTICK}md
  # Filter Demo
  ...
  {TRIPLE_BACKTICK}

Rules for responses:

- For each file, first output a single line containing the relative path
  from the repo root (for example: "demo_src/content/index.md").
- Immediately after that line, output a fenced code block whose language
  tag is the file extension (py, js, ts, css, html, toml, sql, md).
- The fenced code block must contain the *entire* desired contents of
  the file after the change is applied.
- Do NOT nest additional triple-backtick fences inside these outer
  fences; if you need to describe code that itself uses fences, either
  escape it or explain it in comments instead.
- Do NOT put explanatory prose between the path line and the fenced code
  block. Any commentary should go before or after the path+block pair.

In any conversation where codebase.md is present, you should treat this
convention as the canonical way to describe edits to the repository.
"""

# ============================
# .gitignore SUPPORT
# ============================
def load_gitignore(root: Path) -> List[str]:
    ig = root / ".gitignore"
    if not ig.exists():
        return []
    patterns: List[str] = []
    for line in ig.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        patterns.append(line)
    return patterns


def _is_plain_dir_pattern(patt: str) -> bool:
    # Something like ".venv" or "build" (no wildcard, no slash at end)
    if any(ch in patt for ch in "*?[]"):
        return False
    if patt.endswith("/"):
        return False
    return True


def is_ignored(path: Path, patterns: List[str], root: Path) -> bool:
    """
    Returns True if path matches any .gitignore pattern.

    Approximates git semantics for common cases:
      - "foo/" ignores folder and all contents
      - "foo" (no wildcard) ignores folder "foo" and contents, and file "foo"
      - "demo/*" and "*.pyc" use fnmatch
    """
    try:
        rel = str(path.relative_to(root)).replace("\\", "/")
    except Exception as e:  # pragma: no cover - debugging path issues
        input(str(e))
        rel = str(path)
    if '_ai_in_out.py' in rel or '.git' in rel:
        return True

    for patt in patterns:
        # Folder ignore: "foo/" -> ignore foo/... and foo itself
        if patt.endswith("/"):
            base = patt.rstrip("/")
            if rel == base or rel.startswith(base + "/"):
                return True

        # Plain name: ".venv" or "build" (no wildcard, no slash)
        elif _is_plain_dir_pattern(patt):
            # Exact file match
            if rel == patt:
                return True
            # Directory match
            if rel.startswith(patt + "/"):
                return True

        # General fnmatch pattern (demo/*, *.pyc, etc.)
        if fnmatch.fnmatch(rel, patt):
            return True

    return False


# ============================
# PATH EXTRACTION
# ============================
def extract_candidate_paths(raw: str) -> List[str]:
    paths: List[str] = []
    for m in PATH_RE.finditer(raw):
        p = m.group("path")
        p = p.strip("`*_[]()\"' ").replace("\\", "/")
        paths.append(p)
    return paths


# ============================
# PATH RESOLUTION ABOVE A FENCE
# ============================
def resolve_path_for_block(
    lines: List[str],
    fence_idx: int,
    root: Path,
    max_lookback: int = 12,
) -> Tuple[str | None, str | None]:
    """
    Look upward from a fenced {TRIPLE_BACKTICK} block to find any path-like string.

    - the closest match wins
    - if the file exists → auto-accept
    - if ambiguous → prompt
    - respects .gitignore
    """
    patterns = load_gitignore(root)

    candidates: List[Tuple[int, str]] = []
    start = max(0, fence_idx - max_lookback)

    for i in range(fence_idx - 1, start - 1, -1):
        raw = lines[i]
        for p in extract_candidate_paths(raw):
            if "." not in p:
                continue
            ext = p.rsplit(".", 1)[1].lower()
            if ext not in EXT_WHITELIST:
                continue
            full = root / p
            if is_ignored(full, patterns, root):
                continue
            candidates.append((i, p))

    if not candidates:
        return None, None

    # choose nearest to fence
    candidates.sort(key=lambda t: abs(fence_idx - t[0]))
    idx, path = candidates[0]
    ext = path.rsplit(".", 1)[1].lower()
    full = root / path

    # exists? trust it
    if full.exists():
        return path, ext

    # otherwise suspicious → confirm
    suspicious = len(candidates) > 1 or idx != fence_idx - 1
    if suspicious:
        print(f"\n[scaffold] Suspicious file-path inference near line {fence_idx+1}")
        for i, p in candidates[:5]:
            print(f"  line {i+1}: {p}")
        resp = input(f"[scaffold] Use '{path}'? [y/N]: ").strip().lower()
        if resp not in ("y", "yes"):
            return None, None

    return path, ext


# ============================
# LANG VALIDATION
# ============================
def validate_lang(ext: str, lang: str | None) -> bool:
    if ext not in LANG_BY_EXT:
        return False
    if not lang:
        return True
    return lang.lower() in LANG_BY_EXT[ext]


# ============================
# FENCED CODE PARSING (NESTED BACKTICKS SAFE)
# ============================
FENCE_OPEN_RE = re.compile(
    r"^(?P<indent>[ \t]*)(?P<fence>`{3,}|~{3,})(?P<info>[^\n`]*)$"
)


def parse_markdown(text: str, root: Path) -> List[Tuple[str, str]]:
    """
    Parse codebase.md with a CommonMark-style fenced code parser so nested backticks
    are handled correctly inside fenced code blocks.

    We support fences like:

      {TRIPLE_BACKTICK}lang
      code with `backticks` and even more backticks inside
      {TRIPLE_BACKTICK}

    Returns list of (relative_path, code_text).
    """
    lines = text.splitlines()
    blocks: List[Tuple[str, str]] = []

    inside = False
    fence_seq = ""
    fence_indent = ""
    lang: str | None = None
    buf: List[str] = []
    fence_idx: int | None = None

    for i, line in enumerate(lines):
        if not inside:
            m = FENCE_OPEN_RE.match(line.rstrip("\n"))
            if not m:
                continue

            fence_seq = m.group("fence")
            fence_indent = m.group("indent") or ""
            info = m.group("info").strip()
            lang = info.split(None, 1)[0] if info else None
            inside = True
            buf = []
            fence_idx = i
            continue

        # inside fenced block: look for closing fence
        stripped = line.rstrip("\n")
        # Closing fence: same indent (or less) and at least as many of the same fence char
        if stripped.startswith(fence_indent + fence_seq):
            # Anything after the fence is ignored (per CommonMark)
            # Close block.
            if fence_idx is None:
                inside = False
                buf = []
                lang = None
                fence_seq = ""
                fence_indent = ""
                continue

            file_path, ext = resolve_path_for_block(lines, fence_idx, root)
            if file_path and ext and validate_lang(ext, lang):
                blocks.append((file_path, "\n".join(buf).rstrip("\n")))
            # reset state
            inside = False
            buf = []
            lang = None
            fence_seq = ""
            fence_indent = ""
            fence_idx = None
            continue

        # Normal code line
        # Strip only trailing newline, preserve everything else including backticks.
        buf.append(line.rstrip("\n"))

    return blocks


# ============================
# APPLY CHANGES FIRST
# ============================
def scaffold_from_markdown(md_path: Path, root: Path) -> None:
    text = md_path.read_text(encoding="utf-8")
    blocks = parse_markdown(text, root)

    if not blocks:
        print("[scaffold] no valid code blocks found.")
        return

    patterns = load_gitignore(root)

    for rel_path, code in blocks:
        out = root / rel_path
        if is_ignored(out, patterns, root):
            print(f"[scaffold] SKIP (ignored by gitignore): {out}")
            continue
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(code.rstrip() + "\n", encoding="utf-8")
        print(f"[scaffold] wrote {out}")


# ============================
# MYPY INTEGRATION
# ============================
_MYPY_AVAILABLE: bool | None = None


def run_mypy_for_file(root: Path, rel: Path) -> str | None:
    """
    Run mypy in strict *module* mode for a given Python file.

    - Converts "pkg/sub/file.py" → module "pkg.sub.file"
    - Executes: python -m mypy --strict -m <module>
    - Returns the combined stdout/stderr if there are any errors,
      otherwise returns None.

    If mypy is not available, prints a one-time warning and returns None.
    """
    global _MYPY_AVAILABLE

    if rel.suffix.lower() != ".py" or rel.stem == '__init__':
        return None

    # If we've previously determined that mypy is unavailable, skip quickly.
    if _MYPY_AVAILABLE is False:
        return None

    # Derive module name from relative path (module mode assumption).
    module = rel.with_suffix("").as_posix().replace("/", ".")
    cmd = [sys.executable, "-m", "mypy", "--strict", "-m", module]

    try:
        proc = subprocess.run(
            cmd,
            cwd=root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except FileNotFoundError:
        # mypy is not installed / not on PATH
        if _MYPY_AVAILABLE is not False:
            print("[scaffold] mypy not found; skipping type checks.")
        _MYPY_AVAILABLE = False
        return None

    _MYPY_AVAILABLE = True

    if proc.returncode == 0:
        # No errors in strict mode for this module.
        return None

    stdout = proc.stdout.strip()
    stderr = proc.stderr.strip()
    body_parts: List[str] = []

    # Include the exact command for reproducibility.
    body_parts.append(f"$ {' '.join(cmd)}")

    if stdout:
        body_parts.append(stdout)
    if stderr:
        body_parts.append(stderr)

    if not stdout and not stderr:
        body_parts.append(
            f"mypy exited with code {proc.returncode} and produced no output."
        )

    return "\n\n".join(body_parts)


# ============================
# GENERATE CODEBASE.md AFTER UPDATES
# ============================
def generate_codebase_md(root: Path) -> Path:
    patterns = load_gitignore(root)
    out = root / "codebase.md"

    lines: List[str] = []
    lines.append("Codebase Snapshot")
    lines.append("Auto-generated after applying codebase.md updates.\n")

    for f in sorted(root.rglob("*")):
        if not f.is_file():
            continue

        rel = f.relative_to(root)
        ext = f.suffix.lower().lstrip(".")

        if ext not in EXT_WHITELIST:
            continue
        if f.name in ("codebase.md",):
            continue
        if is_ignored(f, patterns, root):
            continue

        content = f.read_text(encoding="utf-8", errors="replace").rstrip()

        lines.append(f"File: {rel.as_posix()}")
        lines.append(f"{TRIPLE_BACKTICK}{ext}")
        lines.append(content)
        lines.append(TRIPLE_BACKTICK)

        # If this is a Python file, and mypy --strict reports any errors,
        # append a dedicated mypy block immediately after the Python block.
        if ext == "py":
            mypy_output = run_mypy_for_file(root, rel)
            if mypy_output:
                # Use "mypy" as the language tag so the scaffold parser ignores
                # this block when writing files (it only recognizes tags from
                # LANG_BY_EXT). This keeps mypy diagnostics visible to humans
                # without ever being treated as file contents.
                lines.append(f"{TRIPLE_BACKTICK}mypy")
                lines.append(mypy_output)
                lines.append(TRIPLE_BACKTICK)

        lines.append("")  # blank line between entries

    # Append the AI response format note so any model seeing codebase.md
    # knows how to structure scaffold responses.
    lines.append(AI_RESPONSE_NOTE.strip("\n"))

    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"[scaffold] wrote updated {out}")
    return out


# ============================
# ENTRY POINT
# ============================
def main() -> None:
    if len(sys.argv) >= 3:
        ai_path = Path(sys.argv[1]).resolve()
        root = Path(sys.argv[2]).resolve()
    else:
        ai_path = Path("codebase.md").resolve()
        root = ai_path.parent

    if not ai_path.exists():
        print(f"[scaffold] codebase.md not found at: {ai_path}")
    else:
        scaffold_from_markdown(ai_path, root)

    generate_codebase_md(root)


if __name__ == "__main__":
    main()
