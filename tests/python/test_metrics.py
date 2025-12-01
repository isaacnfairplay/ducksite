import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _functions_in_file(path: Path):
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            yield node


def test_render_module_size_soft_cap():
    render_js = ROOT / 'ducksite' / 'static_src' / 'render.js'
    lines = render_js.read_text().splitlines()
    # Soft cap intentionally generous to act as a future guardrail.
    assert len(lines) < 2500


def test_python_function_lengths_under_soft_cap():
    target = ROOT / 'ducksite' / 'builder.py'
    lengths = [func.end_lineno - func.lineno for func in _functions_in_file(target)]
    # Allow very loose threshold today.
    assert lengths and max(lengths) < 400
