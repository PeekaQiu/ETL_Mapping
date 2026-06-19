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
    FLOW_PUBLISH_LOOP,
    FLOW_PUBLISH_LOOP_DESC,
    source_publish_desc,
    source_publish_name,
)
from data_integration.tasks.source_run import publish_source

_LOGGER = task_logger(__name__)


@flow(name=FLOW_PUBLISH_LOOP, description=FLOW_PUBLISH_LOOP_DESC)
def run_publish_cycle(
    controller_config_path: str = DEFAULT_BULK_CONTROLLER_CONFIG_PATH,
    source_names: list[str] | None = None,
    stop_on_failure: bool = False,
) -> dict[str, int]:
    logger = _LOGGER
    root = project_root()
    controller_path = resolve_project_path(controller_config_path, root)
    controller = load_controller(controller_path)
    sources = iter_enabled_sources(controller, project_root_path=root)
    if source_names:
        allowed = set(source_names)
        sources = [(name, path) for name, path in sources if name in allowed]
        missing = allowed - {name for name, _ in sources}
        if missing:
            raise ValueError(f"unknown source name(s): {', '.join(sorted(missing))}")
    validate_source_configs(sources)

    logger.info(
        "Publish cycle starting | %s",
        log_fields(
            controller=controller_path,
            source_count=len(sources),
            source_filter=source_names or "all",
            stop_on_failure=stop_on_failure,
        ),
    )

    results: dict[str, int] = {}
    for source_name, config_path in sources:
        logger.info("Running publish for source | %s", log_fields(source=source_name, config=config_path))
        source_task = publish_source.with_options(
            name=source_publish_name(source_name),
            description=source_publish_desc(source_name),
        )
        try:
            published_count = source_task(
                config_path=str(config_path.relative_to(root)),
                source_name=source_name,
            )
        except Exception:
            logger.exception("Publish task failed | %s", log_fields(source=source_name))
            results[source_name] = 0
            if stop_on_failure:
                raise
            continue

        results[source_name] = published_count
        logger.info(
            "Publish completed for source | %s",
            log_fields(source=source_name, published=published_count),
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
