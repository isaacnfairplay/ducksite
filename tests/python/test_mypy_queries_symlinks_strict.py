import subprocess
import sys


def test_mypy_queries_strict() -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "mypy", "--strict", "-m", "ducksite.queries"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    if proc.returncode != 0:
        print(proc.stdout)
    assert proc.returncode == 0


def test_mypy_symlinks_strict() -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "mypy", "--strict", "-m", "ducksite.symlinks"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    if proc.returncode != 0:
        print(proc.stdout)
    assert proc.returncode == 0
