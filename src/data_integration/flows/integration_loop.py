from __future__ import annotations

from prefect import flow

from data_integration.config.loader import DEFAULT_BULK_CONTROLLER_CONFIG_PATH
from data_integration.flows.controller import (
    iter_enabled_sources,
    load_controller,
    project_root,
    resolve_project_path,
    validate_source_configs,
)
from data_integration.logging_setup import log_fields, task_logger
from data_integration.prefect_ui import (
    FLOW_INTEGRATION_LOOP,
    FLOW_INTEGRATION_LOOP_DESC,
    source_integrate_desc,
    source_integrate_name,
)
from data_integration.tasks.source_run import integrate_source

_LOGGER = task_logger(__name__)


@flow(name=FLOW_INTEGRATION_LOOP, description=FLOW_INTEGRATION_LOOP_DESC)
def run_integration_cycle(
    controller_config_path: str = DEFAULT_BULK_CONTROLLER_CONFIG_PATH,
    stop_on_failure: bool = False,
) -> dict[str, str | None]:
    logger = _LOGGER
    root = project_root()
    controller_path = resolve_project_path(controller_config_path, root)
    controller = load_controller(controller_path)
    sources = iter_enabled_sources(controller, project_root_path=root)
    validate_source_configs(sources)

    logger.info(
        "Integration cycle starting | %s",
        log_fields(
            controller=controller_path,
            source_count=len(sources),
            stop_on_failure=stop_on_failure,
        ),
    )
    results: dict[str, str | None] = {}
    for source_name, config_path in sources:
        logger.debug("Queued source | %s", log_fields(source=source_name, config=config_path))
        logger.info("Running integration for source | %s", log_fields(source=source_name, config=config_path))
        source_task = integrate_source.with_options(
            name=source_integrate_name(source_name),
            description=source_integrate_desc(source_name),
        )
        try:
            run_id = source_task(
                config_path=str(config_path.relative_to(root)),
                source_name=source_name,
            )
        except Exception:
            logger.exception("Integration task failed | %s", log_fields(source=source_name))
            results[source_name] = None
            if stop_on_failure:
                raise
            continue

        results[source_name] = run_id
        if run_id is None:
            logger.info("No new work for source | %s", log_fields(source=source_name))
        else:
            logger.info("Integration completed for source | %s", log_fields(source=source_name, run_id=run_id))

    logger.info(
        "Integration cycle finished | %s",
        log_fields(
            source_count=len(sources),
            with_work=sum(1 for run_id in results.values() if run_id is not None),
            idle=sum(1 for run_id in results.values() if run_id is None),
        ),
    )
    return results
