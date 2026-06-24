from __future__ import annotations

import os
import time
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import func, select

from data_integration.storage.models import FileStatus, ProcessingRunStatus
from data_integration.tasks.archive import archive_run_impl
from data_integration.tasks.classify import classify_run_impl
from data_integration.tasks.retention import retention_run_impl
from tests.helpers import make_config, make_repository, publish_run, write_xml


def test_processes_120_incremental_files_in_one_source_run(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    source_dir = config.directories.source_dirs[0]
    for index in range(120):
        write_xml(
            source_dir / f"invoice_{index:03}.xml",
            f"<Document><Type>INVOICE</Type><Amount>{index}</Amount></Document>",
        )
    repository = make_repository(config)

    run_id = archive_run_impl(config, repository)
    assert run_id is not None
    classify_run_impl(config, repository, run_id)
    publish_run(config, repository, run_id)

    files = repository.get_run_files(run_id)
    assert repository.get_run(run_id).status == ProcessingRunStatus.PUBLISHED.value
    assert len(files) == 120
    assert all(record.status == FileStatus.PUBLISHED.value for record in files)
    assert len(list((config.directories.output_root / "invoice").glob("*.xml"))) == 120


def test_archive_retention_deletes_only_terminal_old_archives(tmp_path: Path) -> None:
    config = make_config(tmp_path, archive_retention_days=0)
    source_dir = config.directories.source_dirs[0]
    write_xml(source_dir / "invoice.xml", "<Document><Type>INVOICE</Type></Document>")
    repository = make_repository(config)

    run_id = archive_run_impl(config, repository)
    assert run_id is not None
    classify_run_impl(config, repository, run_id)
    publish_run(config, repository, run_id)
    archive_path = Path(repository.get_run_files(run_id)[0].archive_path)
    old_timestamp = time.time() - 24 * 60 * 60
    os.utime(archive_path, (old_timestamp, old_timestamp))

    with repository._session_factory.begin() as session:
        run = repository._get_run(session, run_id)
        run.created_at = datetime.now() - timedelta(days=1)

    deleted = retention_run_impl(config, repository)

    assert deleted >= 1
    assert not archive_path.exists()


def test_archive_retention_skips_recent_runs(tmp_path: Path) -> None:
    config = make_config(tmp_path, archive_retention_days=30)
    source_dir = config.directories.source_dirs[0]
    write_xml(source_dir / "old.xml", "<Document><Type>INVOICE</Type><Run>old</Run></Document>")
    repository = make_repository(config)

    old_run_id = archive_run_impl(config, repository)
    assert old_run_id is not None
    classify_run_impl(config, repository, old_run_id)
    publish_run(config, repository, old_run_id)
    old_archive_path = Path(repository.get_run_files(old_run_id)[0].archive_path)
    old_timestamp = time.time() - 60 * 24 * 60 * 60
    os.utime(old_archive_path, (old_timestamp, old_timestamp))

    with repository._session_factory.begin() as session:
        run = repository._get_run(session, old_run_id)
        run.created_at = datetime.now() - timedelta(days=60)

    write_xml(source_dir / "new.xml", "<Document><Type>INVOICE</Type><Run>new</Run></Document>")
    new_run_id = archive_run_impl(config, repository)
    assert new_run_id is not None
    classify_run_impl(config, repository, new_run_id)
    publish_run(config, repository, new_run_id)
    new_archive_path = Path(repository.get_run_files(new_run_id)[0].archive_path)
    new_timestamp = time.time() - 60 * 24 * 60 * 60
    os.utime(new_archive_path, (new_timestamp, new_timestamp))

    deleted = retention_run_impl(config, repository)

    assert deleted >= 1
    assert not old_archive_path.exists()
    assert new_archive_path.exists()


def test_detail_retention_removes_rule_and_validation_details_but_keeps_file_identity(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    source_dir = config.directories.source_dirs[0]
    write_xml(source_dir / "invoice.xml", "<Document><Type>INVOICE</Type></Document>")
    repository = make_repository(config)

    run_id = archive_run_impl(config, repository)
    assert run_id is not None
    classify_run_impl(config, repository, run_id)
    publish_run(config, repository, run_id)

    from data_integration.storage.models import FileRecord, RuleMatch, ValidationEvent

    with repository._session_factory.begin() as session:
        run = repository._get_run(session, run_id)
        run.created_at = datetime.now() - timedelta(days=100)
        for event in session.scalars(select(ValidationEvent).where(ValidationEvent.run_id == run_id)):
            event.created_at = datetime.now() - timedelta(days=100)

    deleted = repository.delete_old_detail_records(datetime.now() - timedelta(days=90))

    with repository._session_factory() as session:
        assert deleted >= 2
        assert session.scalar(select(func.count()).select_from(RuleMatch)) == 0
        assert session.scalar(select(func.count()).select_from(ValidationEvent)) == 0
        assert session.scalar(select(func.count()).select_from(FileRecord)) == 1
