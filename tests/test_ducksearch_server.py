import json
import socket
import subprocess
import time
from pathlib import Path
from urllib import request


def _make_minimal_root(tmp_path: Path) -> Path:
    (tmp_path / "config.toml").write_text("name='demo'\n")
    for name in [
        "reports",
        "composites",
        "cache/artifacts",
        "cache/slices",
        "cache/materialize",
        "cache/literal_sources",
        "cache/bindings",
        "cache/facets",
        "cache/charts",
        "cache/manifests",
        "cache/tmp",
    ]:
        (tmp_path / name).mkdir(parents=True, exist_ok=True)

    report = tmp_path / "reports/demo/example.sql"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text("select 42 as answer\n")
    return tmp_path


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_for_health(base_url: str, proc: subprocess.Popen[str], timeout: float = 5.0) -> None:
    last_err: Exception | None = None
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            out, err = proc.communicate()
            raise AssertionError(f"server exited early: {out}\n{err}")
        try:
            with request.urlopen(f"{base_url}/health", timeout=1) as resp:
                if resp.status == 200:
                    return
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            time.sleep(0.1)
    raise AssertionError(f"server did not become healthy: {last_err}")


def test_serve_runs_report(tmp_path: Path):
    root = _make_minimal_root(tmp_path)
    host = "127.0.0.1"
    port = _pick_free_port()
    base_url = f"http://{host}:{port}"

    proc = subprocess.Popen(
        [
            "python",
            "-m",
            "ducksearch.cli",
            "serve",
            "--root",
            str(root),
            "--host",
            host,
            "--port",
            str(port),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _wait_for_health(base_url, proc)

        with request.urlopen(f"{base_url}/report?report=demo/example.sql", timeout=5) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        assert "base_parquet" in payload

        parquet_url = f"{base_url}/{payload['base_parquet']}"
        with request.urlopen(parquet_url, timeout=5) as resp:
            data = resp.read()
        assert data
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
