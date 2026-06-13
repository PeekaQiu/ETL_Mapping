from __future__ import annotations

from prefect import flow, get_run_logger

from data_integration.config.loader import (
    DEFAULT_BULK_CONFIG_VARIABLE,
    DEFAULT_BULK_CONTROLLER_CONFIG_PATH,
    DEFAULT_BULK_SOURCE_CONFIG_FILES,
    initialize_bulk_sources_from_files,
)


@flow(name="bulk-sources-config-admin")
def initialize_bulk_sources_variables(
    controller_config_path: str = DEFAULT_BULK_CONTROLLER_CONFIG_PATH,
    controller_variable_name: str = DEFAULT_BULK_CONFIG_VARIABLE,
    source_config_files: dict[str, str] | None = None,
    overwrite: bool = False,
) -> dict[str, object]:
    logger = get_run_logger()
    initialized_sources, controller = initialize_bulk_sources_from_files(
        controller_config_path=controller_config_path,
        controller_variable_name=controller_variable_name,
        source_config_files=source_config_files or DEFAULT_BULK_SOURCE_CONFIG_FILES,
        overwrite=overwrite,
    )
    logger.info(
        "Initialized bulk controller %s and %s source config variable(s).",
        controller_variable_name,
        len(initialized_sources),
    )
    return {
        "controller_variable": controller_variable_name,
        "source_variables": sorted(initialized_sources),
        "controller": controller.snapshot(),
    }
