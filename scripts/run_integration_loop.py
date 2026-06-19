#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging

from scripts._runtime import (
    GracefulStop,
    add_common_args,
    add_loop_args,
    bootstrap,
    configure_logging,
    load_controller,
    resolve_path,
)
from data_integration.logging_setup import log_fields

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run archive -> classify -> prepublish integration cycles from local config files.",
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
    configure_logging(args.verbose)
    stop = GracefulStop()
    stop.install()

    try:
        controller_path = resolve_path(args.controller, project_root=project_root)
        controller = load_controller(controller_path)
    except Exception as exc:
        logger.error("Startup validation failed: %s", exc)
        return 1

    delay_seconds = (
        args.delay_seconds
        if args.delay_seconds is not None
        else controller.loop.delay_seconds
    )
    max_cycles = 1 if args.once else args.cycles
    if max_cycles is None:
        max_cycles = controller.loop.cycles
    stop_on_failure = args.stop_on_failure or controller.loop.stop_on_failure
    logger.info(
        "Integration loop configured | %s",
        log_fields(
            controller=controller_path,
            delay_seconds=delay_seconds,
            max_cycles=max_cycles or "unlimited",
            stop_on_failure=stop_on_failure,
        ),
    )

    from data_integration.flows.integration_loop import run_integration_cycle

    cycle = 0
    cycle_failed = False
    try:
        while not stop.requested():
            cycle += 1
            logger.info("Starting integration cycle %s.", cycle)
            cycle_failed = False

            try:
                results = run_integration_cycle(
                    controller_config_path=str(controller_path.relative_to(project_root)),
                    stop_on_failure=stop_on_failure,
                )
            except Exception:
                logger.exception("Integration cycle flow failed.")
                cycle_failed = True
                if stop_on_failure:
                    return 1
            else:
                for source_name, run_id in results.items():
                    if run_id is None:
                        logger.info("No new work for %s.", source_name)
                    else:
                        logger.info("Integration flow for %s completed run %s.", source_name, run_id)

            if max_cycles is not None and cycle >= max_cycles:
                logger.info("Reached configured cycle limit: %s.", max_cycles)
                break
            if args.once:
                break
            if stop.requested():
                break

            logger.info("Integration cycle %s finished; sleeping %s second(s).", cycle, delay_seconds)
            stop.sleep(delay_seconds)
    finally:
        stop.restore()

    if stop.requested():
        logger.info("Integration loop stopped by signal.")
        return 130
    logger.info("Integration loop exited normally.")
    return 1 if cycle_failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
