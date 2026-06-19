from __future__ import annotations

import logging
from pathlib import Path

from prefect import get_run_logger, task

from data_integration.config.schema import IntegrationConfig
from data_integration.files.safety import move_to_quarantine, promote_file, sha256_file
from data_integration.storage.db import build_engine, build_session_factory, init_db
from data_integration.storage.models import FileRecord, FileStatus, ProcessingRunStatus, ValidationStatus
from data_integration.storage.repository import IntegrationRepository


@task(name="Step 3: Validate and Prepublish Files", retries=0)
def validate_and_prepublish_files(config: IntegrationConfig, run_id: str) -> str:
    engine = build_engine(config.directories.sqlite_path)
    init_db(engine)
    repository = IntegrationRepository(build_session_factory(engine))
    return validate_and_prepublish_run_impl(config, repository, run_id)


def validate_and_prepublish_run_impl(
    config: IntegrationConfig,
    repository: IntegrationRepository,
    run_id: str,
) -> str:
    logger = _logger()
    staged_files = repository.get_run_files(run_id, [FileStatus.CLASSIFIED_STAGING])
    if not staged_files:
        run = repository.get_run(run_id)
        if run.status == ProcessingRunStatus.CLASSIFIED.value:
            repository.mark_run(run_id, ProcessingRunStatus.COMPLETED_WITH_ERRORS, "No staged files to prepublish.")
        return run_id

    failures: list[str] = []
    targets_seen: set[str] = set()
    prepublished_count = 0
    superseded_count = 0

    for record in staged_files:
        superseding_record = _get_superseding_replace_record(repository, record)
        if superseding_record is not None:
            quarantine_path = _quarantine_superseded_staging_file(config, record)
            repository.mark_file_superseded(
                record.id,
                superseded_by_file_id=superseding_record.id,
                quarantine_path=quarantine_path,
                message=(
                    "superseded by newer revision "
                    f"{superseding_record.config_revision} for {superseding_record.logical_target_path}"
                ),
            )
            superseded_count += 1
            continue

        file_errors = _validate_staged_file(record, targets_seen)
        if file_errors:
            failures.extend(file_errors)
            _quarantine_staging_file(config, repository, record, "; ".join(file_errors))
            continue

        staging_path = Path(record.staging_path or "")
        prepublish_path = Path(record.prepublish_path or "")
        try:
            promote_file(
                staging_path,
                prepublish_path,
                record.sha256,
                allow_replace=record.publish_mode == "replace",
            )
            repository.mark_file_prepublished(record.id, prepublish_path)
            prepublished_count += 1
        except Exception as exc:
            message = str(exc)
            failures.append(f"{Path(record.source_path).name}: {message}")
            if prepublish_path.exists():
                quarantine_path = move_to_quarantine(
                    prepublish_path,
                    config.directories.quarantine_dir,
                    run_id,
                    "prepublish_failed",
                )
                repository.mark_file_quarantined(record.id, quarantine_path, f"prepublish failed: {message}")
            elif staging_path.exists():
                _quarantine_staging_file(config, repository, record, f"prepublish failed: {message}")
            else:
                repository.mark_file_failed(record.id, f"prepublish failed: {message}")

    if failures:
        message = "; ".join(failures)
        repository.mark_run(run_id, ProcessingRunStatus.COMPLETED_WITH_ERRORS, message)
        repository.add_validation_event(
            run_id=run_id,
            status=ValidationStatus.FAIL,
            message=message,
            details={
                "errors": failures,
                "prepublished_count": prepublished_count,
                "superseded_count": superseded_count,
            },
        )
        logger.warning(
            "Prepublished %s of %s staged file(s) for source run %s; %s failed.",
            prepublished_count,
            len(staged_files),
            run_id,
            len(failures),
        )
    elif prepublished_count == 0 and superseded_count > 0:
        message = "All staged files were superseded by newer revisions."
        repository.mark_run(run_id, ProcessingRunStatus.COMPLETED_WITH_ERRORS, message)
        logger.info("Skipped prepublish for source run %s because newer revisions already won.", run_id)
    elif repository.summarize_run(run_id)["failed"] > 0:
        repository.mark_run(run_id, ProcessingRunStatus.COMPLETED_WITH_ERRORS)
        logger.warning(
            "Prepublished %s staged file(s) for source run %s with prior file failures.",
            prepublished_count,
            run_id,
        )
    else:
        repository.mark_run(run_id, ProcessingRunStatus.VALIDATED)
        repository.add_validation_event(
            run_id=run_id,
            status=ValidationStatus.PASS,
            message="staging validation passed; files prepublished",
            details={
                "file_count": len(staged_files),
                "prepublished_count": prepublished_count,
                "superseded_count": superseded_count,
            },
        )
        logger.info("Prepublished %s file(s) for source run %s.", prepublished_count, run_id)
    return run_id


def _validate_staged_file(record: FileRecord, targets_seen: set[str]) -> list[str]:
    errors: list[str] = []
    name = Path(record.source_path).name
    if not record.staging_path:
        errors.append(f"{name}: file {record.id} has no staging path")
        return errors
    if not record.target_path:
        errors.append(f"{name}: file {record.id} has no target path")
        return errors
    if not record.prepublish_path:
        errors.append(f"{name}: file {record.id} has no prepublish path")
        return errors

    staging_path = Path(record.staging_path)
    target_path = Path(record.target_path)
    prepublish_path = Path(record.prepublish_path)
    if not staging_path.exists():
        errors.append(f"{name}: staging file missing: {staging_path}")
        return errors
    if sha256_file(staging_path) != record.sha256:
        errors.append(f"{name}: staging hash mismatch: {staging_path}")
    if record.publish_mode != "replace" and target_path.exists():
        errors.append(f"{name}: target already exists: {target_path}")
    if record.publish_mode != "replace" and prepublish_path.exists():
        errors.append(f"{name}: prepublish target already exists: {prepublish_path}")
    if str(prepublish_path) in targets_seen:
        errors.append(f"{name}: duplicate prepublish target in source run: {prepublish_path}")
    targets_seen.add(str(prepublish_path))
    return errors


def _quarantine_staging_file(
    config: IntegrationConfig,
    repository: IntegrationRepository,
    record: FileRecord,
    message: str,
) -> None:
    if not record.staging_path:
        repository.mark_file_failed(record.id, message)
        return
    staging_path = Path(record.staging_path)
    if staging_path.exists():
        quarantine_path = move_to_quarantine(
            staging_path,
            config.directories.quarantine_dir,
            record.run_id,
            "validation_failed",
        )
        repository.mark_file_quarantined(record.id, quarantine_path, message)
    else:
        repository.mark_file_quarantined(record.id, staging_path, message)


def _quarantine_superseded_staging_file(config: IntegrationConfig, record: FileRecord) -> Path | None:
    if not record.staging_path:
        return None
    staging_path = Path(record.staging_path)
    if not staging_path.exists():
        return None
    return move_to_quarantine(staging_path, config.directories.quarantine_dir, record.run_id, "superseded")


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


def validate_and_publish_run_impl(
    config: IntegrationConfig,
    repository: IntegrationRepository,
    run_id: str,
) -> str:
    from data_integration.tasks.publish import publish_prepublished_run_impl

    validate_and_prepublish_run_impl(config, repository, run_id)
    publish_prepublished_run_impl(config, repository, run_id=run_id)
    return run_id


validate_and_publish_files = validate_and_prepublish_files


def _logger() -> logging.Logger:
    try:
        return get_run_logger()
    except Exception:
        return logging.getLogger(__name__)
