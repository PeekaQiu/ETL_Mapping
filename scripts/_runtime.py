from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from data_integration.config.loader import DEFAULT_CONTROLLER_PATH, DEFAULT_CONTROLLER_VAR
from data_integration.config.schema import BulkIntegrationConfig
from data_integration.flows.controller import load_controller_variable, project_root, resolve_project_path
from data_integration.logging_setup import configure_logging, log_fields


def bootstrap() -> Path:
    root = project_root()
    src_path = root / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
    if Path.cwd().resolve() != root.resolve():
        import os

        os.chdir(root)
    logging.getLogger(__name__).debug("Bootstrapped project root | path=%s", root)
    return root


class GracefulStop:
    def __init__(self) -> None:
        self._stop = False
        self._previous_handlers: dict[int, object] = {}

    def install(self) -> None:
        for signum in (signal.SIGINT, signal.SIGTERM):
            self._previous_handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, self._handle_signal)

    def restore(self) -> None:
        for signum, handler in self._previous_handlers.items():
            signal.signal(signum, handler)

    def requested(self) -> bool:
        return self._stop

    def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            return
        deadline = time.monotonic() + seconds
        while not self._stop:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(remaining, 0.5))

    def _handle_signal(self, signum: int, _frame: object) -> None:
        if not self._stop:
            logging.getLogger(__name__).warning(
                "Received signal %s; stopping after current step.",
                signal.Signals(signum).name,
            )
        self._stop = True


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--controller",
        default=DEFAULT_CONTROLLER_PATH,
        help="Controller bootstrap config file path (used only when Prefect Variable is missing).",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging.")


def add_loop_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single cycle and exit.",
    )
    parser.add_argument(
        "--delay-seconds",
        type=int,
        default=None,
        help="Override loop delay between cycles.",
    )
    parser.add_argument(
        "--cycles",
        type=int,
        default=None,
        help="Maximum number of cycles before exit.",
    )


def load_controller(controller_bootstrap_path: Path, *, project_root_path: Path | None = None) -> BulkIntegrationConfig:
    root = project_root_path or project_root()
    return load_controller_variable(
        variable_name=DEFAULT_CONTROLLER_VAR,
        bootstrap_path=controller_bootstrap_path,
        project_root_path=root,
    )


def resolve_path(path: str | Path, *, project_root_path: Path | None = None) -> Path:
    return resolve_project_path(path, project_root_path or project_root())


def loop_settings(args: argparse.Namespace, controller: BulkIntegrationConfig) -> tuple[int, int | None, bool]:
    delay_seconds = args.delay_seconds if args.delay_seconds is not None else controller.loop.delay_seconds
    max_cycles = 1 if args.once else args.cycles
    if max_cycles is None:
        max_cycles = controller.loop.cycles
    stop_on_failure = args.stop_on_failure or controller.loop.stop_on_failure
    return delay_seconds, max_cycles, stop_on_failure


def run_scheduled_loop(
    *,
    args: argparse.Namespace,
    project_root_path: Path,
    controller_path: Path,
    controller: BulkIntegrationConfig,
    cycle_label: str,
    run_cycle: Callable[[], Any],
    log_cycle_result: Callable[[Any], None],
) -> int:
    logger = logging.getLogger(__name__)
    delay_seconds, max_cycles, stop_on_failure = loop_settings(args, controller)
    logger.info(
        "%s loop configured | %s",
        cycle_label,
        log_fields(
            controller=controller_path,
            delay_seconds=delay_seconds,
            max_cycles=max_cycles or "unlimited",
            stop_on_failure=stop_on_failure,
        ),
    )

    stop = GracefulStop()
    stop.install()
    cycle = 0
    cycle_failed = False
    try:
        while not stop.requested():
            cycle += 1
            logger.info("Starting %s cycle %s.", cycle_label.lower(), cycle)
            cycle_failed = False
            try:
                result = run_cycle()
            except Exception:
                logger.exception("%s cycle flow failed.", cycle_label)
                cycle_failed = True
                if stop_on_failure:
                    return 1
            else:
                log_cycle_result(result)

            if max_cycles is not None and cycle >= max_cycles:
                logger.info("Reached configured cycle limit: %s.", max_cycles)
                break
            if args.once or stop.requested():
                break

            logger.info(
                "%s cycle %s finished; sleeping %s second(s).",
                cycle_label,
                cycle,
                delay_seconds,
            )
            stop.sleep(delay_seconds)
    finally:
        stop.restore()

    if stop.requested():
        logger.info("%s loop stopped by signal.", cycle_label)
        return 130
    logger.info("%s loop exited normally.", cycle_label)
    return 1 if cycle_failed else 0
