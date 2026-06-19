from __future__ import annotations

from prefect import task

from data_integration.flows.main import _read_integration_config, run_data_integration_impl
from data_integration.logging_setup import log_fields, task_logger
from data_integration.prefect_ui import (
    TASK_INTEGRATE_SOURCE,
    TASK_INTEGRATE_SOURCE_DESC,
    TASK_PUBLISH_SOURCE,
    TASK_PUBLISH_SOURCE_DESC,
)
from data_integration.storage.db import build_engine, build_session_factory, init_db
from data_integration.storage.repository import IntegrationRepository
from data_integration.tasks.publish import publish_prepublished_run_impl

_LOGGER = task_logger(__name__)


@task(name=TASK_INTEGRATE_SOURCE, description=TASK_INTEGRATE_SOURCE_DESC, retries=0)
def integrate_source(
    config_variable: str | None = None,
    config_path: str | None = None,
    source_name: str | None = None,
) -> str | None:
    config_ref = config_path or config_variable or "unknown"
    _LOGGER.debug(
        "Integrate source task invoked | %s",
        log_fields(source=source_name or "-", config=config_ref),
    )
    return run_data_integration_impl(
        config_variable=config_variable,
        config_path=config_path,
        source_name=source_name,
    )


@task(name=TASK_PUBLISH_SOURCE, description=TASK_PUBLISH_SOURCE_DESC, retries=0)
def publish_source(
    config_variable: str | None = None,
    config_path: str | None = None,
    source_name: str | None = None,
) -> int:
    config_ref = config_path or config_variable or "unknown"
    _LOGGER.info(
        "Publish source task starting | %s",
        log_fields(source=source_name or "-", config=config_ref),
    )
    config = _read_integration_config(config_variable=config_variable, config_path=config_path)
    engine = build_engine(config.directories.sqlite_path)
    init_db(engine)
    repository = IntegrationRepository(build_session_factory(engine))
    published = publish_prepublished_run_impl(config, repository, run_id=None)
    _LOGGER.info(
        "Publish source task finished | %s",
        log_fields(source=source_name or "-", published=published),
    )
    return published
