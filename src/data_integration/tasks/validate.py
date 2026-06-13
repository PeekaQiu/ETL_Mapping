from __future__ import annotations

import logging
from pathlib import Path

from prefect import get_run_logger, task

from data_integration.config.schema import IntegrationConfig
from data_integration.files.safety import move_to_quarantine, promote_file, sha256_file
from data_integration.storage.db import build_engine, build_session_factory, init_db
from data_integration.storage.models import FileStatus, ProcessingRunStatus, ValidationStatus
from data_integration.storage.repository import IntegrationRepository


class ValidationRunError(RuntimeError):
    pass


@task(name="Step 3: Validate and Publish Files", retries=0)
def validate_and_publish_files(config: IntegrationConfig, run_id: str) -> str:
    engine = build_engine(config.directories.sqlite_path)
    init_db(engine)
    repository = IntegrationRepository(build_session_factory(engine))
    return validate_and_publish_run_impl(config, repository, run_id)


def validate_and_publish_run_impl(
    config: IntegrationConfig,
    repository: IntegrationRepository,
    run_id: str,
) -> str:
    logger = _logger()
    all_files = repository.get_run_files(run_id)
    staged_files = repository.get_run_files(run_id, [FileStatus.CLASSIFIED_STAGING])
    errors = validate_staging_manifest(all_files, staged_files)
    errors.extend(validate_staged_files(staged_files))

    if errors:
        _fail_validation(config, repository, run_id, errors)
        raise ValidationRunError("; ".join(errors))

    repository.mark_run(run_id, ProcessingRunStatus.VALIDATED)
    repository.add_validation_event(
        run_id=run_id,
        status=ValidationStatus.PASS,
        message="staging validation passed",
        details={"file_count": len(staged_files)},
    )

    promoted: list[tuple[int, Path]] = []
    try:
        for record in staged_files:
            staging_path = Path(record.staging_path or "")
            target_path = Path(record.target_path or "")
            repository.mark_file_validated(record.id)
            promote_file(staging_path, target_path, record.sha256)
            promoted.append((record.id, target_path))
            repository.mark_file_published(record.id, target_path)
    except Exception as exc:
        rollback_promoted(config, repository, run_id, promoted, str(exc))
        quarantine_remaining_staging(config, repository, run_id, str(exc))
        repository.mark_run(run_id, ProcessingRunStatus.FAILED, str(exc))
        repository.add_validation_event(
            run_id=run_id,
            status=ValidationStatus.FAIL,
            message="promotion failed and rollback was attempted",
            details={"error": str(exc)},
        )
        raise

    repository.mark_run(run_id, ProcessingRunStatus.PUBLISHED)
    logger.info("Published %s file(s) for source run %s.", len(staged_files), run_id)
    return run_id


def validate_staging_manifest(all_files: list, staged_files: list) -> list[str]:
    errors: list[str] = []
    if not all_files:
        errors.append("source run has no files")
    if len(all_files) != len(staged_files):
        errors.append(f"staged file count mismatch: expected {len(all_files)}, actual {len(staged_files)}")
    return errors


def validate_staged_files(staged_files: list) -> list[str]:
    errors: list[str] = []
    targets_seen: set[str] = set()
    for record in staged_files:
        if not record.staging_path:
            errors.append(f"file {record.id} has no staging path")
            continue
        if not record.target_path:
            errors.append(f"file {record.id} has no target path")
            continue

        staging_path = Path(record.staging_path)
        target_path = Path(record.target_path)
        if not staging_path.exists():
            errors.append(f"staging file missing: {staging_path}")
            continue
        if sha256_file(staging_path) != record.sha256:
            errors.append(f"staging hash mismatch: {staging_path}")
        if target_path.exists():
            errors.append(f"target already exists: {target_path}")
        if str(target_path) in targets_seen:
            errors.append(f"duplicate target in source run: {target_path}")
        targets_seen.add(str(target_path))
    return errors


def rollback_promoted(
    config: IntegrationConfig,
    repository: IntegrationRepository,
    run_id: str,
    promoted: list[tuple[int, Path]],
    message: str,
) -> None:
    for file_id, target_path in promoted:
        quarantine_path = move_to_quarantine(target_path, config.directories.quarantine_dir, run_id, "promotion_failed")
        repository.mark_file_quarantined(file_id, quarantine_path, f"promotion rollback: {message}")


def quarantine_remaining_staging(
    config: IntegrationConfig,
    repository: IntegrationRepository,
    run_id: str,
    message: str,
) -> None:
    for status in [FileStatus.CLASSIFIED_STAGING, FileStatus.VALIDATED]:
        for record in repository.get_run_files(run_id, [status]):
            if record.staging_path and Path(record.staging_path).exists():
                quarantine_path = move_to_quarantine(
                    Path(record.staging_path),
                    config.directories.quarantine_dir,
                    run_id,
                    "promotion_failed",
                )
                repository.mark_file_quarantined(record.id, quarantine_path, f"promotion failed: {message}")


def _fail_validation(
    config: IntegrationConfig,
    repository: IntegrationRepository,
    run_id: str,
    errors: list[str],
) -> None:
    for record in repository.get_run_files(run_id, [FileStatus.CLASSIFIED_STAGING]):
        if record.staging_path:
            quarantine_path = move_to_quarantine(
                Path(record.staging_path),
                config.directories.quarantine_dir,
                run_id,
                "validation_failed",
            )
            repository.mark_file_quarantined(record.id, quarantine_path, "; ".join(errors))
    message = "; ".join(errors)
    repository.mark_run(run_id, ProcessingRunStatus.FAILED, message)
    repository.add_validation_event(
        run_id=run_id,
        status=ValidationStatus.FAIL,
        message=message,
        details={"errors": errors},
    )


def _logger() -> logging.Logger:
    try:
        return get_run_logger()
    except Exception:
        return logging.getLogger(__name__)
