from __future__ import annotations

import logging
import time

from prefect import flow, get_run_logger

from data_integration.config.loader import DEFAULT_BULK_CONFIG_VARIABLE, load_bulk_config
from data_integration.config.schema import BulkIntegrationConfig, FlowConfigRef
from data_integration.flows.main import run_data_integration


@flow(name="bulk-sources-controller")
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
    logger = logging.getLogger(__name__)
    cycle = 0
    while True:
        controller = load_bulk_config(
            variable_name=controller_config_variable,
            config_path=controller_config_path,
        )
        cycle += 1
        logger.info("Starting bulk controller cycle %s.", cycle)
        run_bulk_sources_once(
            controller_config_variable=controller_config_variable,
            controller_config_path=controller_config_path,
            controller=controller,
        )

        if controller.loop.cycles is not None and cycle >= controller.loop.cycles:
            logger.info("Bulk loop reached configured cycle count: %s.", controller.loop.cycles)
            return

        logger.info("All source flows completed. External loop sleeping %s second(s).", controller.loop.delay_seconds)
        time.sleep(controller.loop.delay_seconds)


def run_bulk_sources_once_impl(controller: BulkIntegrationConfig) -> dict[str, str | None]:
    logger = _logger()
    results: dict[str, str | None] = {}
    for flow_config in controller.flow_configs:
        if not flow_config.enabled:
            logger.info("Skipping disabled source flow config: %s.", flow_config.name)
            continue
        try:
            results[flow_config.name] = _run_single_source(flow_config)
        except Exception as exc:
            results[flow_config.name] = None
            if controller.loop.stop_on_failure:
                raise
            logger.exception("Source flow failed but controller is configured to continue: %s", flow_config.name)
    return results


def _run_single_source(flow_config: FlowConfigRef) -> str | None:
    if flow_config.config_variable:
        return run_data_integration(
            config_variable=flow_config.config_variable,
            config_path=None,
            source_name=flow_config.name,
        )
    return run_data_integration(config_path=flow_config.config_path, source_name=flow_config.name)


def _logger() -> logging.Logger:
    try:
        return get_run_logger()
    except Exception:
        return logging.getLogger(__name__)
