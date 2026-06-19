import json
import logging
from pathlib import Path
from typing import Any

from prefect import get_run_logger
from prefect.variables import Variable

from data_integration.config.loader import read_config_file, validate_config_payload
from data_integration.config.schema import IntegrationConfig
from data_integration.notifications.base import LoggingNotifier, NotificationMessage
from data_integration.runtime.locks import integration_lock
from data_integration.storage.db import build_engine, build_session_factory, init_db
from data_integration.storage.repository import IntegrationRepository
from data_integration.tasks.archive import archive_files
from data_integration.tasks.classify import ClassificationRunError, classify_files
from data_integration.tasks.retention import cleanup_archive_retention
from data_integration.tasks.validate import validate_and_prepublish_files


def run_data_integration(
    config_variable: str | None = None,
    config_path: str | None = None,
    source_name: str | None = None,
) -> str | None:
    return run_data_integration_impl(
        config_variable=config_variable,
        config_path=config_path,
        source_name=source_name,
    )


def run_data_integration_impl(
    config_variable: str | None = None,
    config_path: str | None = None,
    source_name: str | None = None,
) -> str | None:
    logger = _logger()
    config = _read_integration_config(config_variable=config_variable, config_path=config_path)
    notifier = LoggingNotifier()

    run_id: str | None = None
    with integration_lock(config.directories.lock_file, enabled=config.runtime.no_overlap):
        try:
            run_id = _source_task(archive_files, source_name, "Step 1: Archive Files")(config)
            if run_id is None:
                logger.info("No files were archived; flow completed with no work.")
                return None
            _source_task(classify_files, source_name, "Step 2: Classify Files")(config, run_id)
            _source_task(validate_and_prepublish_files, source_name, "Step 3: Validate and Prepublish Files")(
                config,
                run_id,
            )
            _source_task(cleanup_archive_retention, source_name, "Step 4: Cleanup Archive Retention")(config)
            _log_run_summary(config, run_id, logger)
            return run_id
        except ClassificationRunError:
            raise
        except Exception as exc:
            if run_id is not None:
                _log_run_summary(config, run_id, logger)
            notifier.send(
                NotificationMessage(
                    subject="Data Integration Process failed",
                    body=str(exc),
                    recipients=config.notifications.recipients,
                )
            )
            raise


def _log_run_summary(config: IntegrationConfig, run_id: str, logger: logging.Logger) -> None:
    engine = build_engine(config.directories.sqlite_path)
    init_db(engine)
    repository = IntegrationRepository(build_session_factory(engine))
    run = repository.get_run(run_id)
    summary = repository.summarize_run(run_id)
    logger.info(
        "Run %s finished with status %s: %s",
        run_id,
        run.status,
        json.dumps(summary, ensure_ascii=False),
    )


def _read_integration_config(
    *,
    config_variable: str | None = None,
    config_path: str | Path | None = None,
) -> IntegrationConfig:
    if config_path is not None:
        return read_config_file(config_path)
    if config_variable is None:
        raise ValueError("Either config_variable or config_path must be provided")

    raw_config = Variable.get(config_variable, default=None)
    if raw_config is None:
        raise ValueError(f"Prefect Variable not found: {config_variable}")
    if isinstance(raw_config, str):
        raw_config = json.loads(raw_config)
    return validate_config_payload(raw_config)


def _source_task(task_fn: Any, source_name: str | None, task_name: str) -> Any:
    if source_name is None:
        return task_fn
    return task_fn.with_options(name=f"{source_name} - {task_name}")


def _logger() -> logging.Logger:
    try:
        return get_run_logger()
    except Exception:
        return logging.getLogger(__name__)
