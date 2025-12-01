import subprocess
import sys


def test_mypy_markdown_parser_strict() -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "mypy", "--strict", "-m", "ducksite.markdown_parser"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    if proc.returncode != 0:
        print(proc.stdout)
    assert proc.returncode == 0
