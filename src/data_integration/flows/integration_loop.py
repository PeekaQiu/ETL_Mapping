from __future__ import annotations

from prefect import flow, get_run_logger

from data_integration.config.loader import DEFAULT_BULK_CONTROLLER_CONFIG_PATH
from data_integration.flows.controller import (
    iter_enabled_sources,
    load_controller,
    project_root,
    resolve_project_path,
    validate_source_configs,
)
from data_integration.tasks.source_run import integrate_source


@flow(name="integration-loop")
def run_integration_cycle(
    controller_config_path: str = DEFAULT_BULK_CONTROLLER_CONFIG_PATH,
    stop_on_failure: bool = False,
) -> dict[str, str | None]:
    logger = get_run_logger()
    root = project_root()
    controller_path = resolve_project_path(controller_config_path, root)
    controller = load_controller(controller_path)
    sources = iter_enabled_sources(controller, project_root_path=root)
    validate_source_configs(sources)

    results: dict[str, str | None] = {}
    for source_name, config_path in sources:
        logger.info("Running data integration for %s (%s).", source_name, config_path)
        source_task = integrate_source.with_options(name=f"{source_name} - Integrate Source")
        try:
            run_id = source_task(
                config_path=str(config_path.relative_to(root)),
                source_name=source_name,
            )
        except Exception:
            logger.exception("Integration task failed for %s.", source_name)
            results[source_name] = None
            if stop_on_failure:
                raise
            continue

        results[source_name] = run_id
        if run_id is None:
            logger.info("No new work for %s.", source_name)
        else:
            logger.info("Integration for %s completed run %s.", source_name, run_id)
    return results
