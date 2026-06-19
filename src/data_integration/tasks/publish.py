from __future__ import annotations

import logging
from pathlib import Path

from prefect import get_run_logger, task

from data_integration.config.schema import IntegrationConfig
from data_integration.files.safety import move_to_quarantine, promote_file, sha256_file
from data_integration.storage.db import build_engine, build_session_factory, init_db
from data_integration.storage.models import FileRecord, FileStatus, ProcessingRunStatus
from data_integration.storage.repository import IntegrationRepository


@task(name="Publish Prepublished Files", retries=0)
def publish_prepublished_files(config: IntegrationConfig, run_id: str | None = None) -> int:
    engine = build_engine(config.directories.sqlite_path)
    init_db(engine)
    repository = IntegrationRepository(build_session_factory(engine))
    return publish_prepublished_run_impl(config, repository, run_id=run_id)


def publish_prepublished_run_impl(
    config: IntegrationConfig,
    repository: IntegrationRepository,
    *,
    run_id: str | None = None,
) -> int:
    logger = _logger()
    prepublished_files = _get_prepublished_files(repository, run_id=run_id)
    if not prepublished_files:
        return 0

    failures: list[str] = []
    published_count = 0
    touched_run_ids: set[str] = set()

    for record in sorted(prepublished_files, key=_publish_sort_key):
        touched_run_ids.add(record.run_id)

        superseding_record = _get_superseding_replace_record(repository, record)
        if superseding_record is not None:
            repository.mark_file_superseded(
                record.id,
                superseded_by_file_id=superseding_record.id,
                message=(
                    "skipped formal publish; superseded by newer revision "
                    f"{superseding_record.config_revision} for {superseding_record.logical_target_path}"
                ),
            )
            continue

        file_errors = _validate_prepublished_file(record)
        if file_errors:
            failures.extend(file_errors)
            _quarantine_prepublished_file(config, repository, record, "; ".join(file_errors))
            continue

        prepublish_path = Path(record.prepublish_path or "")
        target_path = Path(record.target_path or "")
        try:
            repository.mark_file_validated(record.id)
            promote_file(
                prepublish_path,
                target_path,
                record.sha256,
                allow_replace=record.publish_mode == "replace",
            )
            repository.mark_file_published(record.id, target_path)
            published_count += 1
        except Exception as exc:
            message = f"formal publish failed: {exc}"
            failures.append(f"{Path(record.source_path).name}: {message}")
            _quarantine_prepublished_file(config, repository, record, message)

    for current_run_id in touched_run_ids:
        _finalize_run_after_publish(repository, current_run_id)

    if failures:
        logger.warning(
            "Published %s of %s prepublished file(s); %s failed.",
            published_count,
            len(prepublished_files),
            len(failures),
        )
    else:
        logger.info("Published %s prepublished file(s).", published_count)
    return published_count


def _get_prepublished_files(
    repository: IntegrationRepository,
    *,
    run_id: str | None,
) -> list[FileRecord]:
    if run_id is not None:
        return repository.get_run_files(run_id, [FileStatus.PREPUBLISHED])
    return repository.get_files_for_status(FileStatus.PREPUBLISHED)


def _publish_sort_key(record: FileRecord) -> tuple[str, int, int]:
    logical_target = record.logical_target_path or record.final_target_path or record.target_path or ""
    return (logical_target, -record.config_revision, -record.id)


def _validate_prepublished_file(record: FileRecord) -> list[str]:
    errors: list[str] = []
    name = Path(record.source_path).name
    if not record.prepublish_path:
        errors.append(f"{name}: file {record.id} has no prepublish path")
        return errors
    if not record.target_path:
        errors.append(f"{name}: file {record.id} has no target path")
        return errors

    prepublish_path = Path(record.prepublish_path)
    target_path = Path(record.target_path)
    if not prepublish_path.exists():
        errors.append(f"{name}: prepublish file missing: {prepublish_path}")
        return errors
    if sha256_file(prepublish_path) != record.sha256:
        errors.append(f"{name}: prepublish hash mismatch: {prepublish_path}")
    if record.publish_mode != "replace" and target_path.exists():
        errors.append(f"{name}: target already exists: {target_path}")
    return errors


def _quarantine_prepublished_file(
    config: IntegrationConfig,
    repository: IntegrationRepository,
    record: FileRecord,
    message: str,
) -> None:
    if not record.prepublish_path:
        repository.mark_file_failed(record.id, message)
        return
    prepublish_path = Path(record.prepublish_path)
    if prepublish_path.exists():
        quarantine_path = move_to_quarantine(
            prepublish_path,
            config.directories.quarantine_dir,
            record.run_id,
            "publish_failed",
        )
        repository.mark_file_quarantined(record.id, quarantine_path, message)
        return
    repository.mark_file_failed(record.id, message)


def _get_superseding_replace_record(
    repository: IntegrationRepository,
    record: FileRecord,
) -> FileRecord | None:
    if record.publish_mode != "replace" or not record.logical_target_path:
        return None
    latest = repository.get_latest_replace_file(
        source_path=Path(record.source_path),
        sha256=record.sha256,
        logical_target_path=Path(record.logical_target_path),
    )
    if latest is None or latest.id == record.id:
        return None
    return latest


def _finalize_run_after_publish(repository: IntegrationRepository, run_id: str) -> None:
    files = repository.get_run_files(run_id)
    statuses = {record.status for record in files}
    if FileStatus.PREPUBLISHED.value in statuses or FileStatus.CLASSIFIED_STAGING.value in statuses:
        return
    if FileStatus.FAILED.value in statuses or FileStatus.QUARANTINED.value in statuses:
        repository.mark_run(run_id, ProcessingRunStatus.COMPLETED_WITH_ERRORS)
        return
    if FileStatus.PUBLISHED.value in statuses:
        repository.mark_run(run_id, ProcessingRunStatus.PUBLISHED)
        return
    if statuses and statuses <= {FileStatus.SUPERSEDED.value}:
        repository.mark_run(
            run_id,
            ProcessingRunStatus.COMPLETED_WITH_ERRORS,
            "All prepublished files were superseded by newer revisions.",
        )


def _logger() -> logging.Logger:
    try:
        return get_run_logger()
    except Exception:
        return logging.getLogger(__name__)
