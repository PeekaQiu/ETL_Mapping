from __future__ import annotations

from prefect import flow

from data_integration.config.loader import DEFAULT_CONTROLLER_PATH, DEFAULT_CONTROLLER_VAR
from data_integration.flows.controller import prepare_cycle
from data_integration.logging_setup import log_fields, task_logger
from data_integration.prefect_ui import (
    FLOW_PUBLISH_LOOP,
    FLOW_PUBLISH_LOOP_DESC,
    source_publish_desc,
    source_publish_name,
)
from data_integration.tasks.source_run import publish_source

_LOGGER = task_logger(__name__)


@flow(name=FLOW_PUBLISH_LOOP, description=FLOW_PUBLISH_LOOP_DESC)
def run_publish_cycle(
    controller_variable: str = DEFAULT_CONTROLLER_VAR,
    controller_bootstrap_path: str = DEFAULT_CONTROLLER_PATH,
    source_names: list[str] | None = None,
    stop_on_failure: bool = False,
) -> dict[str, int]:
    logger = _LOGGER
    cycle = prepare_cycle(
        controller_variable=controller_variable,
        controller_bootstrap_path=controller_bootstrap_path,
    )
    sources = cycle.sources
    if source_names:
        allowed = set(source_names)
        sources = [ref for ref in sources if ref.source_name in allowed]
        missing = allowed - {ref.source_name for ref in sources}
        if missing:
            raise ValueError(f"unknown source name(s): {', '.join(sorted(missing))}")

    logger.info(
        "Publish cycle starting | %s",
        log_fields(
            controller_variable=controller_variable,
            source_count=len(sources),
            source_filter=source_names or "all",
            stop_on_failure=stop_on_failure,
        ),
    )

    results: dict[str, int] = {}
    for source_ref in sources:
        logger.info(
            "Running publish for source | %s",
            log_fields(
                source=source_ref.source_name,
                config_variable=source_ref.config_variable,
            ),
        )
        source_task = publish_source.with_options(
            name=source_publish_name(source_ref.source_name),
            description=source_publish_desc(source_ref.source_name),
        )
        try:
            published_count = source_task(
                config_variable=source_ref.config_variable,
                bootstrap_path=str(source_ref.bootstrap_path.relative_to(cycle.root)),
                source_name=source_ref.source_name,
            )
        except Exception:
            logger.exception("Publish task failed | %s", log_fields(source=source_ref.source_name))
            results[source_ref.source_name] = 0
            if stop_on_failure:
                raise
            continue

        results[source_ref.source_name] = published_count
        logger.info(
            "Publish completed for source | %s",
            log_fields(source=source_ref.source_name, published=published_count),
        )

    logger.info(
        "Publish cycle finished | %s",
        log_fields(
            source_count=len(sources),
            total_published=sum(results.values()),
            sources_with_zero_publish=sum(1 for count in results.values() if count == 0),
        ),
    )
    return results
