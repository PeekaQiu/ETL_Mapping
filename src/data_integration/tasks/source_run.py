from __future__ import annotations

from prefect import task

from data_integration.flows.main import run_data_integration_impl
from data_integration.flows.main import _read_integration_config
from data_integration.storage.db import build_engine, build_session_factory, init_db
from data_integration.storage.repository import IntegrationRepository
from data_integration.tasks.publish import publish_prepublished_run_impl


@task(name="Integrate Source", retries=0)
def integrate_source(
    config_variable: str | None = None,
    config_path: str | None = None,
    source_name: str | None = None,
) -> str | None:
    return run_data_integration_impl(
        config_variable=config_variable,
        config_path=config_path,
        source_name=source_name,
    )


@task(name="Publish Source", retries=0)
def publish_source(
    config_variable: str | None = None,
    config_path: str | None = None,
    source_name: str | None = None,
) -> int:
    config = _read_integration_config(config_variable=config_variable, config_path=config_path)
    engine = build_engine(config.directories.sqlite_path)
    init_db(engine)
    repository = IntegrationRepository(build_session_factory(engine))
    return publish_prepublished_run_impl(config, repository, run_id=None)
