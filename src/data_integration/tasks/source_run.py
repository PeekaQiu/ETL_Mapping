from __future__ import annotations

from prefect import task

from data_integration.flows.main import load_source_config, run_integration
from data_integration.logging_setup import log_fields, task_logger
from data_integration.prefect_ui import (
    TASK_INTEGRATE_SOURCE,
    TASK_INTEGRATE_SOURCE_DESC,
    TASK_PUBLISH_SOURCE,
    TASK_PUBLISH_SOURCE_DESC,
)
from data_integration.storage.db import open_repository
from data_integration.tasks.publish import publish_run_impl

_LOGGER = task_logger(__name__)


@task(name=TASK_INTEGRATE_SOURCE, description=TASK_INTEGRATE_SOURCE_DESC, retries=0)
def integrate_source(
    *,
    config_variable: str,
    bootstrap_path: str | None = None,
    source_name: str | None = None,
) -> str | None:
    return run_integration(
        config_variable=config_variable,
        bootstrap_path=bootstrap_path,
        source_name=source_name,
    )


@task(name=TASK_PUBLISH_SOURCE, description=TASK_PUBLISH_SOURCE_DESC, retries=0)
def publish_source(
    *,
    config_variable: str,
    bootstrap_path: str | None = None,
    source_name: str | None = None,
) -> int:
    config = load_source_config(config_variable=config_variable, bootstrap_path=bootstrap_path)
    repository = open_repository(config.directories.sqlite_path)
    published = publish_run_impl(config, repository, run_id=None)
    _LOGGER.info(
        "Publish source task finished | %s",
        log_fields(source=source_name or "-", published=published),
    )
    return published
