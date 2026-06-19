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
from data_integration.tasks.source_run import publish_source


def run_scheduled_publish(
    config_variable: str | None = None,
    config_path: str | None = None,
    source_name: str | None = None,
) -> int:
    task_fn = publish_source.with_options(
        name=f"{source_name} - Publish Source" if source_name else "Publish Source",
    )
    return task_fn(
        config_variable=config_variable,
        config_path=config_path,
        source_name=source_name,
    )


@flow(name="publish-loop")
def run_publish_cycle(
    controller_config_path: str = DEFAULT_BULK_CONTROLLER_CONFIG_PATH,
    source_names: list[str] | None = None,
    stop_on_failure: bool = False,
) -> dict[str, int]:
    logger = get_run_logger()
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

    results: dict[str, int] = {}
    for source_name, config_path in sources:
        logger.info("Publishing prepublished files for %s (%s).", source_name, config_path)
        source_task = publish_source.with_options(name=f"{source_name} - Publish Source")
        try:
            published_count = source_task(
                config_path=str(config_path.relative_to(root)),
                source_name=source_name,
            )
        except Exception:
            logger.exception("Publish task failed for %s.", source_name)
            results[source_name] = 0
            if stop_on_failure:
                raise
            continue

        results[source_name] = published_count
        logger.info(
            "Publish for %s completed; %s file(s) published.",
            source_name,
            published_count,
        )
    return results
