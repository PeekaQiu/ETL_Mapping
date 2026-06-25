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
        description="Run scheduled formal publish cycles for prepublished files.",
    )
    add_common_args(parser)
    add_loop_args(parser)
    parser.add_argument(
        "--stop-on-failure",
        action="store_true",
        help="Stop the loop when any source publish fails.",
    )
    parser.add_argument(
        "--source",
        action="append",
        default=[],
        metavar="NAME",
        help="Only publish for the given source flow name. Repeatable.",
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

    from data_integration.flows.publish import run_publish_cycle

    source_filter = args.source or None

    def run_cycle() -> dict[str, int]:
        return run_publish_cycle(
            controller_bootstrap_path=str(controller_path.relative_to(project_root)),
            source_names=source_filter,
            stop_on_failure=args.stop_on_failure or controller.loop.stop_on_failure,
        )

    def log_cycle_result(results: dict[str, int]) -> None:
        for source_name, published_count in results.items():
            logger.info(
                "Publish flow for %s completed; %s file(s) published.",
                source_name,
                published_count,
            )

    return run_scheduled_loop(
        args=args,
        project_root_path=project_root,
        controller_path=controller_path,
        controller=controller,
        cycle_label="Publish",
        run_cycle=run_cycle,
        log_cycle_result=log_cycle_result,
    )


if __name__ == "__main__":
    raise SystemExit(main())
