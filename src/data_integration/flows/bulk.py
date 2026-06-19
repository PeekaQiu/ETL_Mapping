from __future__ import annotations

import time

from prefect import flow

from data_integration.config.loader import DEFAULT_BULK_CONFIG_VARIABLE, load_bulk_config
from data_integration.config.schema import BulkIntegrationConfig, FlowConfigRef
from data_integration.logging_setup import log_fields, task_logger
from data_integration.prefect_ui import (
    FLOW_BULK_CONTROLLER,
    FLOW_BULK_CONTROLLER_DESC,
    source_integrate_desc,
    source_integrate_name,
)
from data_integration.tasks.source_run import integrate_source

_LOGGER = task_logger(__name__)


@flow(name=FLOW_BULK_CONTROLLER, description=FLOW_BULK_CONTROLLER_DESC)
def run_bulk_sources_once(
    controller_config_variable: str = DEFAULT_BULK_CONFIG_VARIABLE,
    controller_config_path: str | None = None,
    controller: BulkIntegrationConfig | None = None,
) -> dict[str, str | None]:
    if controller is None:
        controller = load_bulk_config(
            variable_name=controller_config_variable,
            config_path=controller_config_path,
        )
    return run_bulk_sources_once_impl(controller)


def run_bulk_sources_loop(
    controller_config_variable: str = DEFAULT_BULK_CONFIG_VARIABLE,
    controller_config_path: str | None = None,
) -> None:
    logger = _LOGGER
    cycle = 0
    while True:
        controller = load_bulk_config(
            variable_name=controller_config_variable,
            config_path=controller_config_path,
        )
        cycle += 1
        logger.info(
            "Bulk controller cycle starting | %s",
            log_fields(cycle=cycle, enabled_sources=sum(1 for f in controller.flow_configs if f.enabled)),
        )
        run_bulk_sources_once(
            controller_config_variable=controller_config_variable,
            controller_config_path=controller_config_path,
            controller=controller,
        )

        if controller.loop.cycles is not None and cycle >= controller.loop.cycles:
            logger.info("Bulk loop reached cycle limit | %s", log_fields(cycles=controller.loop.cycles))
            return

        logger.info(
            "Bulk cycle finished; sleeping | %s",
            log_fields(cycle=cycle, delay_seconds=controller.loop.delay_seconds),
        )
        time.sleep(controller.loop.delay_seconds)


def run_bulk_sources_once_impl(controller: BulkIntegrationConfig) -> dict[str, str | None]:
    logger = _LOGGER
    results: dict[str, str | None] = {}
    for flow_config in controller.flow_configs:
        if not flow_config.enabled:
            logger.info("Skipping disabled source | %s", log_fields(source=flow_config.name))
            continue
        try:
            results[flow_config.name] = _run_single_source(flow_config)
        except Exception as exc:
            results[flow_config.name] = None
            if controller.loop.stop_on_failure:
                logger.exception(
                    "Bulk source failed; stopping controller | %s",
                    log_fields(source=flow_config.name, error=str(exc)),
                )
                raise
            logger.exception(
                "Bulk source failed; continuing | %s",
                log_fields(source=flow_config.name, error=str(exc)),
            )
    logger.info(
        "Bulk controller cycle finished | %s",
        log_fields(
            processed=len(results),
            with_work=sum(1 for run_id in results.values() if run_id is not None),
            without_run=sum(1 for run_id in results.values() if run_id is None),
        ),
    )
    return results


def _run_single_source(flow_config: FlowConfigRef) -> str | None:
    source_task = integrate_source.with_options(
        name=source_integrate_name(flow_config.name),
        description=source_integrate_desc(flow_config.name),
    )
    if flow_config.config_variable:
        return source_task(
            config_variable=flow_config.config_variable,
            config_path=None,
            source_name=flow_config.name,
        )
    return source_task(config_path=flow_config.config_path, source_name=flow_config.name)
