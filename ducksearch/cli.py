"""Entry point for the ducksearch command line interface."""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

from .loader import validate_root
from .report_parser import parse_report_sql
from .server import run_server


def handle_serve(args: argparse.Namespace) -> None:
    layout = validate_root(Path(args.root))
    _lint_reports(layout.reports)
    print(
        "ducksearch serve ready on "
        f"{args.host}:{args.port} with root {layout.root} (workers={args.workers}, dev={args.dev})"
    )
    run_server(layout, args.host, args.port, dev=args.dev, workers=args.workers)


def handle_lint(args: argparse.Namespace) -> None:
    layout = validate_root(Path(args.root))
    _lint_reports(layout.reports)
    print(f"ducksearch lint passed for {layout.root}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ducksearch",
        description="DuckDB-backed search experiments.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve_parser = subparsers.add_parser("serve", help="Run the ducksearch server.")
    serve_parser.add_argument("--root", required=True, help="ducksearch root directory")
    serve_parser.add_argument("--host", default="127.0.0.1", help="host to bind")
    serve_parser.add_argument("--port", type=int, default=8080, help="port to bind")
    serve_parser.add_argument("--workers", type=int, default=1, help="number of worker processes")
    serve_parser.add_argument("--dev", action="store_true", help="enable development mode")
    serve_parser.set_defaults(func=handle_serve)

    lint_parser = subparsers.add_parser("lint", help="Lint the ducksearch project.")
    lint_parser.add_argument("--root", required=True, help="ducksearch root directory")
    lint_parser.set_defaults(func=handle_lint)

    return parser


def main(argv: Iterable[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


def _lint_reports(reports_dir: Path) -> None:
    for path in reports_dir.rglob("*.sql"):
        parse_report_sql(path)


if __name__ == "__main__":
    main()
