from __future__ import annotations
import argparse
from pathlib import Path
from .builder import build_project, serve_project
from .watcher import watch_and_build
from .init_project import init_project


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="ducksite",
        description="Static DuckDB-powered dashboard builder",
    )
    parser.add_argument(
        "command",
        nargs="?",
        default="build",
        choices=["build", "init", "serve"],
        help="Command to run ('build', 'init', or 'serve').",
    )
    parser.add_argument(
        "--root",
        type=str,
        default=".",
        help="Project root containing ducksite.toml (default = current directory).",
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Watch for changes and rebuild automatically (build only).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="Port for 'serve' (default 8080).",
    )
    parser.add_argument(
        "--server",
        choices=["builtin", "uvicorn"],
        default="builtin",
        help="Server backend for 'serve' (default = builtin threaded, 'uvicorn' requires extra deps).",
    )

    args = parser.parse_args()
    root = Path(args.root).resolve()

    if args.command == "init":
        init_project(root)
    elif args.command == "build":
        if args.reload:
            watch_and_build(root)
        else:
            build_project(root)
    elif args.command == "serve":
        serve_project(root, port=args.port, backend=args.server)


if __name__ == "__main__":
    main()
