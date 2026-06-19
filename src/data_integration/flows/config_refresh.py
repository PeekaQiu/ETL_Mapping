from __future__ import annotations

from pathlib import Path

from prefect import flow, get_run_logger

from data_integration.config.loader import (
    DEFAULT_BULK_CONFIG_VARIABLE,
    DEFAULT_BULK_CONTROLLER_CONFIG_PATH,
    DEFAULT_BULK_SOURCE_CONFIG_FILES,
    initialize_bulk_sources_from_files,
    read_config_file,
)
from data_integration.flows.controller import (
    iter_enabled_sources,
    load_controller,
    project_root,
    resolve_project_path,
    validate_source_configs,
)


@flow(name="config-refresh")
def refresh_config(
    controller_config_path: str = DEFAULT_BULK_CONTROLLER_CONFIG_PATH,
    controller_variable_name: str = DEFAULT_BULK_CONFIG_VARIABLE,
    source_config_files: dict[str, str] | None = None,
    overwrite: bool = False,
    dry_run: bool = False,
    prepare_dirs: bool = False,
) -> dict[str, object]:
    logger = get_run_logger()
    root = project_root()
    controller_path = resolve_project_path(controller_config_path, root)
    controller = load_controller(controller_path)
    sources = iter_enabled_sources(controller, project_root_path=root)

    if prepare_dirs:
        validate_source_configs(sources)
    else:
        for source_name, config_path in sources:
            if not config_path.is_file():
                raise FileNotFoundError(f"source config not found for {source_name}: {config_path}")
            read_config_file(config_path)

    logger.info(
        "Validated controller %s with %s enabled source flow(s).",
        controller_path,
        len(sources),
    )
    for source_name, config_path in sources:
        logger.info("  - %s -> %s", source_name, config_path)

    if dry_run:
        logger.info("Dry run complete; Prefect Variables were not modified.")
        return {
            "controller_path": str(controller_path),
            "source_count": len(sources),
            "dry_run": True,
        }

    initialized_sources, loaded_controller = initialize_bulk_sources_from_files(
        controller_config_path=controller_path,
        controller_variable_name=controller_variable_name,
        source_config_files=source_config_files or DEFAULT_BULK_SOURCE_CONFIG_FILES,
        overwrite=overwrite,
    )
    logger.info(
        "Loaded %s source config variable(s) and controller variable %s.",
        len(initialized_sources),
        controller_variable_name,
    )
    return {
        "controller_variable": controller_variable_name,
        "source_variables": sorted(initialized_sources),
        "controller": loaded_controller.snapshot(),
        "dry_run": False,
    }
