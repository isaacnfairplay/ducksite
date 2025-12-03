from __future__ import annotations
import argparse
from pathlib import Path
from .builder import build_project, serve_project
from .watcher import watch_and_build
from .init_project import init_project, init_demo_project
from . import tuy_md, tuy_sql, tuy_toml


RESOURCE_HANDLERS = {
    "toml": tuy_toml,
    "sql": tuy_sql,
    "md": tuy_md,
}


RESOURCE_COMMANDS = {"add", "modify", "remove"}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ducksite",
        description="Static DuckDB-powered dashboard builder",
    )
    parser.add_argument(
        "command",
        nargs="?",
        default="build",
        choices=["build", "init", "demo", "serve", *sorted(RESOURCE_COMMANDS)],
        help="Command to run ('build', 'init', 'demo', 'serve', 'add', 'modify', or 'remove').",
    )
    parser.add_argument(
        "resource",
        nargs="?",
        help="Resource type for 'add', 'modify', or 'remove' ('toml', 'sql', or 'md').",
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
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    root = Path(args.root).resolve()

    if args.command in RESOURCE_COMMANDS:
        if not args.resource:
            parser.error(f"{args.command} requires a resource type ('toml', 'sql', 'md').")
        handler_module = RESOURCE_HANDLERS.get(args.resource)
        if handler_module is None:
            parser.error(f"Unknown resource type '{args.resource}' (choose 'toml', 'sql', or 'md').")
        handler_module.handle(args.command, root)
        return

    if args.command == "init":
        init_project(root)
    elif args.command == "demo":
        init_demo_project(root)
    elif args.command == "build":
        if args.reload:
            watch_and_build(root)
        else:
            build_project(root)
    elif args.command == "serve":
        serve_project(root, port=args.port, backend=args.server)


if __name__ == "__main__":
    main()
