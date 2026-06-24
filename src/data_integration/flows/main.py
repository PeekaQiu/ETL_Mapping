import json
from pathlib import Path
from typing import Any

from prefect.variables import Variable

from data_integration.config.loader import read_config_file, validate_config_payload
from data_integration.config.schema import IntegrationConfig
from data_integration.logging_setup import log_fields, task_logger
from data_integration.prefect_ui import (
    TASK_ARCHIVE,
    TASK_CLASSIFY,
    TASK_RETENTION,
    TASK_VALIDATE_PREPUBLISH,
    source_step_desc,
    source_step_name,
)
from data_integration.runtime.locks import FlowAlreadyRunningError, integration_lock
from data_integration.storage.db import build_engine, build_session_factory, init_db
from data_integration.storage.repository import IntegrationRepository
from data_integration.tasks.archive import archive_files
from data_integration.tasks.classify import ClassificationRunError, classify_files
from data_integration.tasks.retention import cleanup_archive_retention
from data_integration.tasks.validate import validate_and_prepublish_files

_LOGGER = task_logger(__name__)


def run_data_integration_impl(
    config_variable: str | None = None,
    config_path: str | None = None,
    source_name: str | None = None,
) -> str | None:
    config_ref = config_path or config_variable or "unknown"
    logger = _LOGGER
    config = _read_integration_config(config_variable=config_variable, config_path=config_path)
    logger.info(
        "Starting data integration | %s",
        log_fields(
            source=source_name or "-",
            config=config_ref,
            lock=config.directories.lock_file,
            no_overlap=config.runtime.no_overlap,
        ),
    )

    run_id: str | None = None
    try:
        with integration_lock(config.directories.lock_file, enabled=config.runtime.no_overlap):
            run_id = _run_integration_steps(config, source_name, logger)
    except FlowAlreadyRunningError:
        logger.warning(
            "Integration skipped because lock is held | %s",
            log_fields(source=source_name or "-", lock_file=config.directories.lock_file),
        )
        raise

    if run_id is None:
        logger.info("Integration finished with no work | %s", log_fields(source=source_name or "-"))
    else:
        logger.info("Integration finished | %s", log_fields(source=source_name or "-", run_id=run_id))
    return run_id


def _run_integration_steps(
    config: IntegrationConfig,
    source_name: str | None,
    logger: Any,
) -> str | None:
    run_id: str | None = None
    try:
        logger.info("Step 1/4: archive")
        run_id = _source_task(archive_files, source_name, TASK_ARCHIVE)(config)
        if run_id is None:
            logger.info("No files archived; remaining steps skipped.")
            return None

        logger.info("Step 2/4: classify | %s", log_fields(run_id=run_id))
        _source_task(classify_files, source_name, TASK_CLASSIFY)(config, run_id)

        logger.info("Step 3/4: validate and prepublish | %s", log_fields(run_id=run_id))
        _source_task(validate_and_prepublish_files, source_name, TASK_VALIDATE_PREPUBLISH)(
            config,
            run_id,
        )

        logger.info("Step 4/4: archive retention cleanup | %s", log_fields(run_id=run_id))
        deleted = _source_task(cleanup_archive_retention, source_name, TASK_RETENTION)(config)
        logger.info("Retention cleanup complete | %s", log_fields(run_id=run_id, deleted=deleted))

        _log_run_summary(config, run_id, logger)
        return run_id
    except ClassificationRunError:
        raise
    except Exception as exc:
        if run_id is not None:
            _log_run_summary(config, run_id, logger)
        logger.exception(
            "Integration failed | %s",
            log_fields(source=source_name or "-", run_id=run_id or "-", error=str(exc)),
        )
        recipients = ",".join(config.notifications.recipients) or "-"
        logger.warning(
            "Integration failure notification | %s",
            log_fields(
                subject="Data Integration Process failed",
                recipients=recipients,
                body=str(exc),
            ),
        )
        raise


def _log_run_summary(config: IntegrationConfig, run_id: str, logger: Any) -> None:
    engine = build_engine(config.directories.sqlite_path)
    init_db(engine)
    repository = IntegrationRepository(build_session_factory(engine))
    run = repository.get_run(run_id)
    summary = repository.summarize_run(run_id)
    logger.info(
        "Run summary | %s",
        log_fields(run_id=run_id, status=run.status, summary=json.dumps(summary, ensure_ascii=False)),
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
    return task_fn.with_options(
        name=source_step_name(source_name, task_name),
        description=source_step_desc(source_name, task_name),
    )
