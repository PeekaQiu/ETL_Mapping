from data_integration.flows.config_refresh import refresh_config
from data_integration.flows.integration_loop import run_integration_cycle
from data_integration.flows.publish import run_publish_cycle
from data_integration.tasks.archive import archive_files
from data_integration.tasks.classify import classify_files
from data_integration.tasks.publish import publish_prepublished_files
from data_integration.tasks.retention import cleanup_archive_retention
from data_integration.tasks.source_run import integrate_source, publish_source
from data_integration.tasks.validate import validate_and_prepublish_files


def test_prefect_flows_have_business_names() -> None:
    assert refresh_config.name == "config-refresh"
    assert run_integration_cycle.name == "integration-loop"
    assert run_publish_cycle.name == "publish-loop"


def test_prefect_tasks_have_business_names() -> None:
    assert integrate_source.name == "Integrate Source"
    assert publish_source.name == "Publish Source"
    assert archive_files.name == "Step 1: Archive Files"
    assert classify_files.name == "Step 2: Classify Files"
    assert validate_and_prepublish_files.name == "Step 3: Validate and Prepublish Files"
    assert publish_prepublished_files.name == "Publish Prepublished Files"
    assert cleanup_archive_retention.name == "Step 4: Cleanup Archive Retention"
