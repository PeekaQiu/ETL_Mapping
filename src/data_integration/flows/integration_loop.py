from __future__ import annotations

from prefect import flow

from data_integration.config.loader import DEFAULT_CONTROLLER_PATH, DEFAULT_CONTROLLER_VAR
from data_integration.flows.controller import prepare_cycle
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
    controller_variable: str = DEFAULT_CONTROLLER_VAR,
    controller_bootstrap_path: str = DEFAULT_CONTROLLER_PATH,
    stop_on_failure: bool = False,
) -> dict[str, str | None]:
    logger = _LOGGER
    cycle = prepare_cycle(
        controller_variable=controller_variable,
        controller_bootstrap_path=controller_bootstrap_path,
    )

    logger.info(
        "Integration cycle starting | %s",
        log_fields(
            controller_variable=controller_variable,
            source_count=len(cycle.sources),
            stop_on_failure=stop_on_failure,
        ),
    )
    results: dict[str, str | None] = {}
    for source_ref in cycle.sources:
        logger.info(
            "Running integration for source | %s",
            log_fields(
                source=source_ref.source_name,
                config_variable=source_ref.config_variable,
            ),
        )
        source_task = integrate_source.with_options(
            name=source_integrate_name(source_ref.source_name),
            description=source_integrate_desc(source_ref.source_name),
        )
        try:
            run_id = source_task(
                config_variable=source_ref.config_variable,
                bootstrap_path=str(source_ref.bootstrap_path.relative_to(cycle.root)),
                source_name=source_ref.source_name,
            )
        except Exception:
            logger.exception("Integration task failed | %s", log_fields(source=source_ref.source_name))
            results[source_ref.source_name] = None
            if stop_on_failure:
                raise
            continue

        results[source_ref.source_name] = run_id
        if run_id is None:
            logger.info("No new work for source | %s", log_fields(source=source_ref.source_name))
        else:
            logger.info(
                "Integration completed for source | %s",
                log_fields(source=source_ref.source_name, run_id=run_id),
            )

    logger.info(
        "Integration cycle finished | %s",
        log_fields(
            source_count=len(cycle.sources),
            with_work=sum(1 for run_id in results.values() if run_id is not None),
            idle=sum(1 for run_id in results.values() if run_id is None),
        ),
    )
    return results
