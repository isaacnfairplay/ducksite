"""Entry point for the ducksearch command line interface."""
from __future__ import annotations

import argparse
from typing import Iterable


def handle_serve(_: argparse.Namespace) -> None:
    """Placeholder handler for future server startup logic."""
    print("ducksearch serve is not yet implemented.")


def handle_lint(_: argparse.Namespace) -> None:
    """Placeholder handler for future linting logic."""
    print("ducksearch lint is not yet implemented.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ducksearch",
        description="DuckDB-backed search experiments.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve_parser = subparsers.add_parser("serve", help="Run the ducksearch server.")
    serve_parser.set_defaults(func=handle_serve)

    lint_parser = subparsers.add_parser("lint", help="Lint the ducksearch project.")
    lint_parser.set_defaults(func=handle_lint)

    return parser


def main(argv: Iterable[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
