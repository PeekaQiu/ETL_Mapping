#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import sys

from scripts._runtime import (
    bootstrap,
    configure_logging,
    default_controller_variable,
    resolve_path,
)

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate local integration configs and optionally load them into Prefect Variables.",
    )
    parser.add_argument(
        "--controller",
        default="config/bulk_sources_controller.json",
        help="Bulk controller config file path.",
    )
    parser.add_argument(
        "--controller-variable",
        default=default_controller_variable(),
        help="Prefect Variable name for the bulk controller config.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing Prefect Variables.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configs only; do not write Prefect Variables.",
    )
    parser.add_argument(
        "--prepare-dirs",
        action="store_true",
        help="Create runtime directories declared in source configs.",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    project_root = bootstrap()
    configure_logging(args.verbose)

    controller_path = resolve_path(args.controller, project_root=project_root)
    try:
        from data_integration.flows.config_refresh import refresh_config

        refresh_config(
            controller_config_path=str(controller_path.relative_to(project_root)),
            controller_variable_name=args.controller_variable,
            overwrite=args.overwrite,
            dry_run=args.dry_run,
            prepare_dirs=args.prepare_dirs,
        )
    except Exception as exc:
        logger.error("Config refresh failed: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
