from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from prefect import task

from data_integration.config.schema import IntegrationConfig
from data_integration.files.safety import move_to_quarantine, promote_file, sha256_file
from data_integration.logging_setup import log_fields, task_logger
from data_integration.prefect_ui import TASK_FORMAL_PUBLISH, TASK_FORMAL_PUBLISH_DESC
from data_integration.storage.db import open_repository
from data_integration.storage.models import FileRecord, FileStatus, ProcessingRunStatus
from data_integration.storage.repository import IntegrationRepository

_LOGGER = task_logger(__name__)

_BACKLOG_WARN_AT = 1000


@task(name=TASK_FORMAL_PUBLISH, description=TASK_FORMAL_PUBLISH_DESC, retries=0)
def publish_files(config: IntegrationConfig, run_id: str | None = None) -> int:
    repository = open_repository(config.directories.sqlite_path)
    return publish_run_impl(config, repository, run_id=run_id)


def publish_run_impl(
    config: IntegrationConfig,
    repository: IntegrationRepository,
    *,
    run_id: str | None = None,
) -> int:
    logger = _LOGGER
    scope = run_id or "all"
    pending = repository.count_files_by_status(FileStatus.PREPUBLISHED)
    if pending > _BACKLOG_WARN_AT:
        logger.warning(
            "Prepublish backlog high | %s",
            log_fields(scope=scope, pending=pending),
        )

    prepublished_files = _list_prepublished(repository, run_id=run_id)
    if not prepublished_files:
        logger.info(
            "No prepublished files pending formal publish | %s",
            log_fields(scope=scope, pending=pending),
        )
        return 0

    logger.info(
        "Starting formal publish | %s",
        log_fields(
            scope=scope,
            pending=pending,
            **({"scope_count": len(prepublished_files)} if run_id is not None else {}),
        ),
    )

    failures: list[str] = []
    published_count = 0
    superseded_count = 0
    deferred_count = 0
    touched_run_ids: set[str] = set()
    observation_seconds = config.runtime.prepublish_observation_seconds

    for record in sorted(prepublished_files, key=_publish_sort_key):
        touched_run_ids.add(record.run_id)
        source_name = Path(record.source_path).name

        superseding_record = repository.find_superseding_record(record)
        if superseding_record is not None:
            repository.mark_file_superseded(
                record.id,
                superseded_by_file_id=superseding_record.id,
                message=(
                    "skipped formal publish; superseded by newer revision "
                    f"{superseding_record.config_revision} for {superseding_record.logical_target_path}"
                ),
            )
            superseded_count += 1
            logger.debug(
                "Skipped publish; file superseded | %s",
                log_fields(
                    run_id=record.run_id,
                    source=source_name,
                    superseded_by=superseding_record.id,
                ),
            )
            continue

        if not _observation_satisfied(record, observation_seconds):
            deferred_count += 1
            logger.debug(
                "Deferred formal publish; observation window not satisfied | %s",
                log_fields(
                    run_id=record.run_id,
                    source=source_name,
                    observation_seconds=observation_seconds,
                ),
            )
            continue

        file_errors = _check_for_publish(record)
        if file_errors:
            failures.extend(file_errors)
            _quarantine_failed(config, repository, record, "; ".join(file_errors))
            logger.warning(
                "Prepublish validation failed before formal publish | %s",
                log_fields(run_id=record.run_id, source=source_name, errors="; ".join(file_errors)),
            )
            continue

        prepublish_path = Path(record.prepublish_path or "")
        target_path = Path(record.target_path or "")
        try:
            promote_file(
                prepublish_path,
                target_path,
                record.sha256,
                allow_replace=record.publish_mode == "replace",
            )
            repository.mark_file_published(record.id, target_path)
            published_count += 1
            logger.debug(
                "File formally published | %s",
                log_fields(
                    run_id=record.run_id,
                    source=source_name,
                    target=target_path,
                    publish_mode=record.publish_mode,
                ),
            )
        except Exception as exc:
            message = f"formal publish failed: {exc}"
            failures.append(f"{source_name}: {message}")
            _quarantine_failed(config, repository, record, message)
            logger.warning(
                "Formal publish failed | %s",
                log_fields(run_id=record.run_id, source=source_name, error=str(exc)),
            )

    for current_run_id in touched_run_ids:
        _finalize_run(repository, current_run_id, logger)

    if failures:
        logger.warning(
            "Formal publish complete with failures | %s",
            log_fields(
                scope=scope,
                published=published_count,
                pending=len(prepublished_files),
                failed=len(failures),
                superseded=superseded_count,
                deferred=deferred_count,
            ),
        )
    else:
        logger.info(
            "Formal publish complete | %s",
            log_fields(
                scope=scope,
                published=published_count,
                superseded=superseded_count,
                deferred=deferred_count,
            ),
        )
    return published_count


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _observation_satisfied(record: FileRecord, observation_seconds: int, *, now: datetime | None = None) -> bool:
    if observation_seconds <= 0:
        return True
    current = now or _utc_now()
    updated_at = record.updated_at
    if updated_at.tzinfo is not None:
        updated_at = updated_at.replace(tzinfo=None)
    return (current - updated_at).total_seconds() >= observation_seconds


def _list_prepublished(
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


def _check_for_publish(record: FileRecord) -> list[str]:
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


def _quarantine_failed(
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


def _finalize_run(repository: IntegrationRepository, run_id: str, logger: Any) -> None:
    files = repository.get_run_files(run_id)
    statuses = {record.status for record in files}
    if FileStatus.PREPUBLISHED.value in statuses or FileStatus.CLASSIFIED_STAGING.value in statuses:
        logger.debug(
            "Run still has pending files after publish | %s",
            log_fields(run_id=run_id, statuses=sorted(statuses)),
        )
        return
    if FileStatus.FAILED.value in statuses or FileStatus.QUARANTINED.value in statuses:
        repository.mark_run(run_id, ProcessingRunStatus.COMPLETED_WITH_ERRORS)
        logger.info(
            "Run finalized with errors | %s",
            log_fields(run_id=run_id, status=ProcessingRunStatus.COMPLETED_WITH_ERRORS.value),
        )
        return
    if FileStatus.PUBLISHED.value in statuses:
        repository.mark_run(run_id, ProcessingRunStatus.PUBLISHED)
        logger.info(
            "Run finalized as published | %s",
            log_fields(run_id=run_id, status=ProcessingRunStatus.PUBLISHED.value),
        )
        return
    if statuses and statuses <= {FileStatus.SUPERSEDED.value}:
        repository.mark_run(
            run_id,
            ProcessingRunStatus.COMPLETED_WITH_ERRORS,
            "All prepublished files were superseded by newer revisions.",
        )
        logger.info(
            "Run finalized; all files superseded | %s",
            log_fields(run_id=run_id, status=ProcessingRunStatus.COMPLETED_WITH_ERRORS.value),
        )
