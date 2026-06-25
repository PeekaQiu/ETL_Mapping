#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging

from scripts._runtime import (
    add_common_args,
    add_loop_args,
    bootstrap,
    load_controller,
    resolve_path,
    run_scheduled_loop,
)
from data_integration.logging_setup import configure_logging

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run archive -> classify -> prepublish integration cycles using Prefect Variables.",
    )
    add_common_args(parser)
    add_loop_args(parser)
    parser.add_argument(
        "--stop-on-failure",
        action="store_true",
        help="Stop the loop when any source flow fails.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    project_root = bootstrap()
    configure_logging(verbose=args.verbose)

    try:
        controller_path = resolve_path(args.controller, project_root_path=project_root)
        controller = load_controller(controller_path, project_root_path=project_root)
    except Exception as exc:
        logger.error("Startup validation failed: %s", exc)
        return 1

    from data_integration.flows.integration_loop import run_integration_cycle

    def run_cycle() -> dict[str, str | None]:
        return run_integration_cycle(
            controller_bootstrap_path=str(controller_path.relative_to(project_root)),
            stop_on_failure=args.stop_on_failure or controller.loop.stop_on_failure,
        )

    def log_cycle_result(results: dict[str, str | None]) -> None:
        for source_name, run_id in results.items():
            if run_id is None:
                logger.info("No new work for %s.", source_name)
            else:
                logger.info("Integration flow for %s completed run %s.", source_name, run_id)

    return run_scheduled_loop(
        args=args,
        project_root_path=project_root,
        controller_path=controller_path,
        controller=controller,
        cycle_label="Integration",
        run_cycle=run_cycle,
        log_cycle_result=log_cycle_result,
    )


if __name__ == "__main__":
    raise SystemExit(main())
