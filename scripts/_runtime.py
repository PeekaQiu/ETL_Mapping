from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
from pathlib import Path

from data_integration.config.loader import (
    DEFAULT_BULK_CONFIG_VARIABLE,
    DEFAULT_BULK_CONTROLLER_CONFIG_PATH,
)
from data_integration.flows.controller import (
    load_controller as _load_controller,
    resolve_project_path as _resolve_project_path,
)
from data_integration.logging_setup import configure_logging as _configure_logging


def bootstrap() -> Path:
    project_root = find_project_root()
    src_path = project_root / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
    if Path.cwd().resolve() != project_root.resolve():
        import os

        os.chdir(project_root)
    logging.getLogger(__name__).debug("Bootstrapped project root | path=%s", project_root)
    return project_root


def find_project_root() -> Path:
    current = Path(__file__).resolve().parent
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").is_file():
            return candidate
    raise RuntimeError("could not locate project root (missing pyproject.toml)")


def configure_logging(verbose: bool) -> None:
    _configure_logging(verbose=verbose)


def resolve_path(path: str | Path, *, project_root: Path | None = None) -> Path:
    root = project_root or find_project_root()
    return _resolve_project_path(path, root)


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
        default=DEFAULT_BULK_CONTROLLER_CONFIG_PATH,
        help="Bulk controller config file path.",
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


def load_controller(controller_path: Path):
    return _load_controller(controller_path)


def default_controller_variable() -> str:
    return DEFAULT_BULK_CONFIG_VARIABLE
