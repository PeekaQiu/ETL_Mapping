from __future__ import annotations

import logging
from typing import Any

from prefect import get_run_logger

LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
LOG_DATEFMT = "%H:%M:%S"

_QUIET_LOGGERS = ("prefect", "sqlalchemy.engine", "urllib3")


def configure_logging(*, verbose: bool = False, level: int | None = None) -> None:
    resolved = level if level is not None else (logging.DEBUG if verbose else logging.INFO)
    logging.basicConfig(level=resolved, format=LOG_FORMAT, datefmt=LOG_DATEFMT, force=True)
    if not verbose:
        for name in _QUIET_LOGGERS:
            logging.getLogger(name).setLevel(logging.WARNING)


def task_logger(module: str) -> logging.Logger:
    try:
        return get_run_logger()
    except Exception:
        return logging.getLogger(module)


def log_fields(**fields: Any) -> str:
    return " ".join(f"{key}={value}" for key, value in fields.items())
