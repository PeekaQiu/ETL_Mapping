from __future__ import annotations

from pathlib import Path

from prefect import task

from data_integration.config.schema import IntegrationConfig
from data_integration.files.safety import move_to_quarantine, promote_file, sha256_file
from data_integration.logging_setup import log_fields, task_logger
from data_integration.prefect_ui import TASK_PREPUBLISH, TASK_PREPUBLISH_DESC
from data_integration.storage.db import build_engine, build_session_factory, init_db
from data_integration.storage.models import FileRecord, FileStatus, ProcessingRunStatus, ValidationStatus
from data_integration.storage.repository import IntegrationRepository

_LOGGER = task_logger(__name__)


@task(name=TASK_PREPUBLISH, description=TASK_PREPUBLISH_DESC, retries=0)
def prepublish_files(config: IntegrationConfig, run_id: str) -> str:
    engine = build_engine(config.directories.sqlite_path)
    init_db(engine)
    repository = IntegrationRepository(build_session_factory(engine))
    return prepublish_run_impl(config, repository, run_id)


def prepublish_run_impl(
    config: IntegrationConfig,
    repository: IntegrationRepository,
    run_id: str,
) -> str:
    logger = _LOGGER
    staged_files = repository.get_run_files(run_id, [FileStatus.CLASSIFIED_STAGING])
    if not staged_files:
        run = repository.get_run(run_id)
        if run.status == ProcessingRunStatus.CLASSIFIED.value:
            repository.mark_run(run_id, ProcessingRunStatus.COMPLETED_WITH_ERRORS, "No staged files to prepublish.")
        logger.info("No staged files to prepublish | %s", log_fields(run_id=run_id, run_status=run.status))
        return run_id

    logger.info(
        "Starting prepublish validation | %s",
        log_fields(run_id=run_id, staged_count=len(staged_files)),
    )

    failures: list[str] = []
    targets_seen: set[str] = set()
    prepublished_count = 0
    superseded_count = 0

    for record in staged_files:
        source_name = Path(record.source_path).name
        superseding_record = repository.find_superseding_record(record)
        if superseding_record is not None:
            quarantine_path = _quarantine_staging(config, record)
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
            logger.debug(
                "File superseded before prepublish | %s",
                log_fields(
                    run_id=run_id,
                    source=source_name,
                    superseded_by=superseding_record.id,
                    revision=superseding_record.config_revision,
                ),
            )
            continue

        file_errors = _validate_staged_file(record, targets_seen)
        if file_errors:
            failures.extend(file_errors)
            _quarantine_failed_staging(config, repository, record, "; ".join(file_errors))
            logger.warning(
                "Staging validation failed; file quarantined | %s",
                log_fields(run_id=run_id, source=source_name, errors="; ".join(file_errors)),
            )
            continue

        staging_path = Path(record.staging_path or "")
        prepublish_path = Path(record.prepublish_path or "")
        older_records = repository.find_older_replace_records(record)
        if older_records:
            quarantine_paths = {
                older.id: (
                    _quarantine_staging(config, older)
                    or _quarantine_prepublish(config, older)
                )
                for older in older_records
            }
            superseded_count += repository.supersede_older_records(
                record,
                quarantine_paths=quarantine_paths,
            )
            for older in older_records:
                _finalize_superseded_run(repository, older.run_id)
            logger.debug(
                "Superseded older inflight replace records before prepublish | %s",
                log_fields(
                    run_id=run_id,
                    source=source_name,
                    superseded=len(older_records),
                    revision=record.config_revision,
                ),
            )
        try:
            promote_file(
                staging_path,
                prepublish_path,
                record.sha256,
                allow_replace=record.publish_mode == "replace",
            )
            repository.mark_file_prepublished(record.id, prepublish_path)
            prepublished_count += 1
            logger.debug(
                "File prepublished | %s",
                log_fields(
                    run_id=run_id,
                    source=source_name,
                    prepublish=prepublish_path,
                    publish_mode=record.publish_mode,
                ),
            )
        except Exception as exc:
            message = str(exc)
            failures.append(f"{source_name}: {message}")
            if prepublish_path.exists():
                quarantine_path = move_to_quarantine(
                    prepublish_path,
                    config.directories.quarantine_dir,
                    run_id,
                    "prepublish_failed",
                )
                repository.mark_file_quarantined(record.id, quarantine_path, f"prepublish failed: {message}")
            elif staging_path.exists():
                _quarantine_failed_staging(config, repository, record, f"prepublish failed: {message}")
            else:
                repository.mark_file_failed(record.id, f"prepublish failed: {message}")
            logger.warning(
                "Prepublish failed | %s",
                log_fields(run_id=run_id, source=source_name, error=message),
            )

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
            "Prepublish complete with failures | %s",
            log_fields(
                run_id=run_id,
                prepublished=prepublished_count,
                staged=len(staged_files),
                failed=len(failures),
                superseded=superseded_count,
            ),
        )
    elif prepublished_count == 0 and superseded_count > 0:
        message = "All staged files were superseded by newer revisions."
        repository.mark_run(run_id, ProcessingRunStatus.COMPLETED_WITH_ERRORS, message)
        logger.info(
            "Prepublish skipped; all files superseded | %s",
            log_fields(run_id=run_id, superseded=superseded_count),
        )
    elif repository.summarize_run(run_id)["failed"] > 0:
        repository.mark_run(run_id, ProcessingRunStatus.COMPLETED_WITH_ERRORS)
        logger.warning(
            "Prepublish complete with prior file failures | %s",
            log_fields(run_id=run_id, prepublished=prepublished_count),
        )
    else:
        repository.mark_run(run_id, ProcessingRunStatus.PREPUBLISHED)
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
        logger.info(
            "Prepublish validation passed | %s",
            log_fields(
                run_id=run_id,
                prepublished=prepublished_count,
                superseded=superseded_count,
            ),
        )
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


def _quarantine_failed_staging(
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


def _quarantine_staging(config: IntegrationConfig, record: FileRecord) -> Path | None:
    if not record.staging_path:
        return None
    staging_path = Path(record.staging_path)
    if not staging_path.exists():
        return None
    return move_to_quarantine(staging_path, config.directories.quarantine_dir, record.run_id, "superseded")


def _quarantine_prepublish(config: IntegrationConfig, record: FileRecord) -> Path | None:
    if not record.prepublish_path:
        return None
    prepublish_path = Path(record.prepublish_path)
    if not prepublish_path.exists():
        return None
    return move_to_quarantine(
        prepublish_path,
        config.directories.quarantine_dir,
        record.run_id,
        "superseded",
    )


def _finalize_superseded_run(repository: IntegrationRepository, run_id: str) -> None:
    files = repository.get_run_files(run_id)
    statuses = {record.status for record in files}
    if statuses and statuses <= {FileStatus.SUPERSEDED.value}:
        repository.mark_run(
            run_id,
            ProcessingRunStatus.COMPLETED_WITH_ERRORS,
            "All prepublished files were superseded by newer revisions.",
        )
