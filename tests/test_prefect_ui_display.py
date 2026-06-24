from data_integration.flows.config_refresh import refresh_config
from data_integration.flows.integration_loop import run_integration_cycle
from data_integration.flows.publish import run_publish_cycle
from data_integration.prefect_ui import (
    FLOW_CONFIG_REFRESH,
    FLOW_CONFIG_REFRESH_DESC,
    FLOW_INTEGRATION_LOOP,
    FLOW_INTEGRATION_LOOP_DESC,
    FLOW_PUBLISH_LOOP,
    FLOW_PUBLISH_LOOP_DESC,
    TASK_ARCHIVE,
    TASK_ARCHIVE_DESC,
    TASK_CLASSIFY,
    TASK_CLASSIFY_DESC,
    TASK_INTEGRATE_SOURCE,
    TASK_INTEGRATE_SOURCE_DESC,
    TASK_FORMAL_PUBLISH,
    TASK_FORMAL_PUBLISH_DESC,
    TASK_PUBLISH_SOURCE,
    TASK_PUBLISH_SOURCE_DESC,
    TASK_PREPUBLISH,
    TASK_PREPUBLISH_DESC,
    TASK_RETENTION,
    TASK_RETENTION_DESC,
)
from data_integration.tasks.archive import archive_files
from data_integration.tasks.classify import classify_files
from data_integration.tasks.publish import publish_files
from data_integration.tasks.retention import apply_retention
from data_integration.tasks.source_run import integrate_source, publish_source
from data_integration.tasks.validate import prepublish_files


def test_prefect_flows_have_business_names_and_descriptions() -> None:
    assert refresh_config.name == FLOW_CONFIG_REFRESH
    assert refresh_config.description == FLOW_CONFIG_REFRESH_DESC
    assert run_integration_cycle.name == FLOW_INTEGRATION_LOOP
    assert run_integration_cycle.description == FLOW_INTEGRATION_LOOP_DESC
    assert run_publish_cycle.name == FLOW_PUBLISH_LOOP
    assert run_publish_cycle.description == FLOW_PUBLISH_LOOP_DESC


def test_prefect_tasks_have_business_names_and_descriptions() -> None:
    assert integrate_source.name == TASK_INTEGRATE_SOURCE
    assert integrate_source.description == TASK_INTEGRATE_SOURCE_DESC
    assert publish_source.name == TASK_PUBLISH_SOURCE
    assert publish_source.description == TASK_PUBLISH_SOURCE_DESC
    assert archive_files.name == TASK_ARCHIVE
    assert archive_files.description == TASK_ARCHIVE_DESC
    assert classify_files.name == TASK_CLASSIFY
    assert classify_files.description == TASK_CLASSIFY_DESC
    assert prepublish_files.name == TASK_PREPUBLISH
    assert prepublish_files.description == TASK_PREPUBLISH_DESC
    assert publish_files.name == TASK_FORMAL_PUBLISH
    assert publish_files.description == TASK_FORMAL_PUBLISH_DESC
    assert apply_retention.name == TASK_RETENTION
    assert apply_retention.description == TASK_RETENTION_DESC
