from __future__ import annotations

import threading
from pathlib import Path

import pytest

from data_integration.runtime.locks import FlowAlreadyRunningError, integration_lock
from data_integration.storage.models import FileStatus, ProcessingRunStatus
from data_integration.tasks.archive import archive_run_impl
from data_integration.tasks.classify import classify_run_impl
from tests.helpers import publish_run
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
    classify_run_impl(config, repository, run_id)
    publish_run(config, repository, run_id)

    run = repository.get_run(run_id)
    files = {Path(record.source_path).name: record for record in repository.get_run_files(run_id)}
    summary = repository.summarize_run(run_id)

    assert run.status == ProcessingRunStatus.COMPLETED_WITH_ERRORS.value
    assert summary["success"] == 1
    assert summary["failed"] == 1
    assert files["one.xml"].status == FileStatus.PUBLISHED.value
    assert files["two.xml"].status == FileStatus.QUARANTINED.value
    assert (config.directories.output_root / "invoice" / "fixed.xml").exists()


def test_partial_classification_success_leaves_staged_files(tmp_path: Path) -> None:
    config = make_config(tmp_path, [invoice_rule("invoice/{source_name}")])
    source_dir = config.directories.source_dirs[0]
    write_xml(source_dir / "good.xml", "<Document><Type>INVOICE</Type></Document>")
    write_xml(source_dir / "bad.xml", "<Document><Type>UNKNOWN</Type></Document>")
    repository = make_repository(config)

    run_id = archive_run_impl(config, repository)
    assert run_id is not None
    classify_run_impl(config, repository, run_id)

    files = {Path(record.source_path).name: record for record in repository.get_run_files(run_id)}
    assert repository.get_run(run_id).status == ProcessingRunStatus.CLASSIFIED.value
    assert files["good.xml"].status == FileStatus.CLASSIFIED_STAGING.value
    assert files["bad.xml"].status == FileStatus.QUARANTINED.value


def test_quarantined_target_does_not_block_later_fixed_source_run(tmp_path: Path) -> None:
    config = make_config(tmp_path, [invoice_rule("invoice/fixed.xml")])
    source_dir = config.directories.source_dirs[0]
    write_xml(source_dir / "one.xml", "<Document><Type>INVOICE</Type><Value>1</Value></Document>")
    write_xml(source_dir / "two.xml", "<Document><Type>INVOICE</Type><Value>2</Value></Document>")
    repository = make_repository(config)

    failed_run_id = archive_run_impl(config, repository)
    assert failed_run_id is not None
    classify_run_impl(config, repository, failed_run_id)
    publish_run(config, repository, failed_run_id)
    assert repository.summarize_run(failed_run_id)["success"] == 1

    (source_dir / "two.xml").unlink()
    unique_config = make_config(tmp_path, [invoice_rule("invoice/{source_name}")])
    write_xml(source_dir / "three.xml", "<Document><Type>INVOICE</Type><Value>3</Value></Document>")
    fixed_run_id = archive_run_impl(unique_config, repository)
    assert fixed_run_id is not None
    classify_run_impl(unique_config, repository, fixed_run_id)

    files = repository.get_run_files(fixed_run_id)
    assert len(files) == 1
    assert files[0].status == FileStatus.CLASSIFIED_STAGING.value


def test_replace_publish_overwrites_existing_formal_target(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    source_dir = config.directories.source_dirs[0]
    write_xml(source_dir / "first.xml", "<Document><Type>INVOICE</Type></Document>")
    write_xml(source_dir / "second.xml", "<Document><Type>INVOICE</Type></Document>")
    repository = make_repository(config)

    run_id = archive_run_impl(config, repository)
    assert run_id is not None
    classify_run_impl(config, repository, run_id)
    target = config.directories.output_root / "invoice" / "first.xml"
    target.parent.mkdir(parents=True)
    target.write_text("pre-existing", encoding="utf-8")

    publish_run(config, repository, run_id)

    files = {Path(record.source_path).name: record for record in repository.get_run_files(run_id)}
    summary = repository.summarize_run(run_id)
    published_target = config.directories.output_root / "invoice" / "second.xml"

    assert repository.get_run(run_id).status == ProcessingRunStatus.PUBLISHED.value
    assert summary["success"] == 2
    assert summary["failed"] == 0
    assert files["first.xml"].status == FileStatus.PUBLISHED.value
    assert files["second.xml"].status == FileStatus.PUBLISHED.value
    assert target.read_text(encoding="utf-8") != "pre-existing"
    assert published_target.exists()
    assert files["first.xml"].staging_path is not None
    assert not Path(files["first.xml"].staging_path).exists()


def test_same_hash_retry_after_quarantine(tmp_path: Path) -> None:
    bad_rules = [
        {
            "rule_id": "other",
            "target_path_template": "other/{source_name}",
            "conditions": {"all": [{"xpath": "/Document/Type", "op": "eq", "value": "OTHER"}]},
        }
    ]
    config = make_config(tmp_path, bad_rules)
    source_dir = config.directories.source_dirs[0]
    source_path = source_dir / "invoice.xml"
    write_xml(source_path, "<Document><Type>INVOICE</Type></Document>")
    repository = make_repository(config)

    first_run_id = archive_run_impl(config, repository)
    assert first_run_id is not None
    classify_run_impl(config, repository, first_run_id)
    assert repository.get_run_files(first_run_id)[0].status == FileStatus.QUARANTINED.value

    fixed_config = make_config(tmp_path)
    retry_run_id = archive_run_impl(fixed_config, repository)
    assert retry_run_id is not None
    classify_run_impl(fixed_config, repository, retry_run_id)
    publish_run(fixed_config, repository, retry_run_id)

    summary = repository.summarize_run(retry_run_id)
    assert summary["success"] == 1
    assert (fixed_config.directories.output_root / "invoice" / "invoice.xml").exists()


def test_file_stability_window_skips_recent_files(tmp_path: Path) -> None:
    config = make_config(tmp_path, file_stability_seconds=3600)
    source_dir = config.directories.source_dirs[0]
    write_xml(source_dir / "invoice.xml", "<Document><Type>INVOICE</Type></Document>")
    repository = make_repository(config)

    assert archive_run_impl(config, repository) is None
