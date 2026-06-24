from __future__ import annotations

from prefect import flow

from data_integration.config.loader import (
    DEFAULT_CONTROLLER_VAR,
    DEFAULT_CONTROLLER_PATH,
    DEFAULT_SOURCE_CONFIGS,
    load_bulk_sources,
    read_config_file,
)
from data_integration.flows.controller import (
    iter_enabled_sources,
    load_controller,
    project_root,
    resolve_project_path,
    validate_source_configs,
)
from data_integration.logging_setup import log_fields, task_logger
from data_integration.prefect_ui import (
    FLOW_CONFIG_REFRESH,
    FLOW_CONFIG_REFRESH_DESC,
)

_LOGGER = task_logger(__name__)


@flow(name=FLOW_CONFIG_REFRESH, description=FLOW_CONFIG_REFRESH_DESC)
def refresh_config(
    controller_config_path: str = DEFAULT_CONTROLLER_PATH,
    controller_variable_name: str = DEFAULT_CONTROLLER_VAR,
    source_config_files: dict[str, str] | None = None,
    overwrite: bool = False,
    dry_run: bool = False,
    prepare_dirs: bool = False,
) -> dict[str, object]:
    logger = _LOGGER
    root = project_root()
    controller_path = resolve_project_path(controller_config_path, root)
    controller = load_controller(controller_path)
    sources = iter_enabled_sources(controller, project_root_path=root)

    logger.info(
        "Config refresh starting | %s",
        log_fields(
            controller=controller_path,
            source_count=len(sources),
            dry_run=dry_run,
            overwrite=overwrite,
            prepare_dirs=prepare_dirs,
        ),
    )

    if prepare_dirs:
        validate_source_configs(sources)
        logger.info("Runtime directories prepared for all enabled sources.")
    else:
        for source_name, config_path in sources:
            if not config_path.is_file():
                raise FileNotFoundError(f"source config not found for {source_name}: {config_path}")
            read_config_file(config_path)

    for source_name, config_path in sources:
        logger.debug("Validated source config | %s", log_fields(source=source_name, config=config_path))

    if dry_run:
        logger.info("Config refresh dry run complete; Prefect Variables unchanged.")
        return {
            "controller_path": str(controller_path),
            "source_count": len(sources),
            "dry_run": True,
        }

    initialized_sources, loaded_controller = load_bulk_sources(
        controller_config_path=controller_path,
        controller_variable_name=controller_variable_name,
        source_config_files=source_config_files or DEFAULT_SOURCE_CONFIGS,
        overwrite=overwrite,
    )
    logger.info(
        "Config refresh complete | %s",
        log_fields(
            controller_variable=controller_variable_name,
            source_variables=len(initialized_sources),
            sources=sorted(initialized_sources),
        ),
    )
    return {
        "controller_variable": controller_variable_name,
        "source_variables": sorted(initialized_sources),
        "controller": loaded_controller.snapshot(),
        "dry_run": False,
    }
