from __future__ import annotations

import threading
from pathlib import Path

import pytest

from data_integration.runtime.locks import FlowAlreadyRunningError, integration_lock
from data_integration.storage.models import FileStatus, ProcessingRunStatus
from data_integration.tasks.archive import archive_run_impl
from data_integration.tasks.classify import ClassificationRunError, classify_run_impl
from data_integration.tasks.validate import ValidationRunError, validate_and_publish_run_impl
from tests.helpers import invoice_rule, make_config, make_repository, write_xml


def test_application_lock_blocks_parallel_flow_instances(tmp_path: Path) -> None:
    lock_file = tmp_path / "runtime" / "integration.lock"
    entered = threading.Event()
    release = threading.Event()
    errors: list[Exception] = []

    def holder() -> None:
        with integration_lock(lock_file):
            entered.set()
            release.wait(timeout=5)

    thread = threading.Thread(target=holder)
    thread.start()
    entered.wait(timeout=5)
    try:
        with pytest.raises(FlowAlreadyRunningError):
            with integration_lock(lock_file):
                pass
    except Exception as exc:
        errors.append(exc)
    finally:
        release.set()
        thread.join(timeout=5)

    assert not errors


def test_same_run_target_collision_quarantines_without_publish(tmp_path: Path) -> None:
    config = make_config(tmp_path, [invoice_rule("invoice/fixed.xml")])
    source_dir = config.directories.source_dirs[0]
    write_xml(source_dir / "one.xml", "<Document><Type>INVOICE</Type></Document>")
    write_xml(source_dir / "two.xml", "<Document><Type>INVOICE</Type></Document>")
    repository = make_repository(config)

    run_id = archive_run_impl(config, repository)
    assert run_id is not None

    with pytest.raises(ClassificationRunError):
        classify_run_impl(config, repository, run_id)

    run = repository.get_run(run_id)
    files = repository.get_run_files(run_id)
    assert run.status == ProcessingRunStatus.FAILED.value
    assert all(record.status == FileStatus.QUARANTINED.value for record in files)
    assert not (config.directories.output_root / "invoice" / "fixed.xml").exists()


def test_quarantined_target_does_not_block_later_fixed_source_run(tmp_path: Path) -> None:
    config = make_config(tmp_path, [invoice_rule("invoice/fixed.xml")])
    source_dir = config.directories.source_dirs[0]
    write_xml(source_dir / "one.xml", "<Document><Type>INVOICE</Type><Value>1</Value></Document>")
    write_xml(source_dir / "two.xml", "<Document><Type>INVOICE</Type><Value>2</Value></Document>")
    repository = make_repository(config)

    failed_run_id = archive_run_impl(config, repository)
    assert failed_run_id is not None
    with pytest.raises(ClassificationRunError):
        classify_run_impl(config, repository, failed_run_id)
    assert all(record.status == FileStatus.QUARANTINED.value for record in repository.get_run_files(failed_run_id))

    write_xml(source_dir / "three.xml", "<Document><Type>INVOICE</Type><Value>3</Value></Document>")
    fixed_run_id = archive_run_impl(config, repository)
    assert fixed_run_id is not None
    classify_run_impl(config, repository, fixed_run_id)

    files = repository.get_run_files(fixed_run_id)
    assert len(files) == 1
    assert files[0].status == FileStatus.CLASSIFIED_STAGING.value


def test_validation_target_collision_rolls_back_staging(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    source_dir = config.directories.source_dirs[0]
    write_xml(source_dir / "invoice.xml", "<Document><Type>INVOICE</Type></Document>")
    repository = make_repository(config)

    run_id = archive_run_impl(config, repository)
    assert run_id is not None
    classify_run_impl(config, repository, run_id)
    target = config.directories.output_root / "invoice" / "invoice.xml"
    target.parent.mkdir(parents=True)
    target.write_text("pre-existing", encoding="utf-8")

    with pytest.raises(ValidationRunError):
        validate_and_publish_run_impl(config, repository, run_id)

    files = repository.get_run_files(run_id)
    assert repository.get_run(run_id).status == ProcessingRunStatus.FAILED.value
    assert files[0].status == FileStatus.QUARANTINED.value
    assert target.read_text(encoding="utf-8") == "pre-existing"
    assert files[0].staging_path is not None
    assert not Path(files[0].staging_path).exists()


def test_file_stability_window_skips_recent_files(tmp_path: Path) -> None:
    config = make_config(tmp_path, file_stability_seconds=3600)
    source_dir = config.directories.source_dirs[0]
    write_xml(source_dir / "invoice.xml", "<Document><Type>INVOICE</Type></Document>")
    repository = make_repository(config)

    assert archive_run_impl(config, repository) is None
