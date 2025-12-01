import subprocess
import sys


def _run_mypy_module(mod: str) -> int:
    proc = subprocess.run(
        [sys.executable, "-m", "mypy", "--strict", "-m", mod],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    if proc.returncode != 0:
        print(proc.stdout)
    return proc.returncode


def test_mypy_config_match_strict() -> None:
    assert _run_mypy_module("ducksite.config") == 0


def test_mypy_forms_match_strict() -> None:
    assert _run_mypy_module("ducksite.forms") == 0


def test_mypy_cte_compiler_match_strict() -> None:
    assert _run_mypy_module("ducksite.cte_compiler") == 0
