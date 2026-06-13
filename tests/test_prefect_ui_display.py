from data_integration.tasks.archive import archive_files
from data_integration.tasks.classify import classify_files
from data_integration.tasks.retention import cleanup_archive_retention
from data_integration.tasks.validate import validate_and_publish_files


def test_prefect_tasks_have_business_names() -> None:
    assert archive_files.name == "A - Archive XML files"
    assert classify_files.name == "B - Classify XML files"
    assert validate_and_publish_files.name == "C - Validate and publish files"
    assert cleanup_archive_retention.name == "Cleanup - Archive retention"
