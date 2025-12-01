import subprocess
import sys


def test_mypy_fast_server_strict() -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "mypy", "--strict", "-m", "ducksite.fast_server"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    if proc.returncode != 0:
        print(proc.stdout)
    assert proc.returncode == 0
