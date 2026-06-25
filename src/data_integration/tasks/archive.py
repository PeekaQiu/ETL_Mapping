from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from prefect import task

from data_integration.config.schema import IntegrationConfig
from data_integration.files.safety import (
    archive_destination,
    copy_to_quarantine,
    copy_verify,
    is_file_stable,
    is_xml_file,
    sha256_file,
)
from data_integration.logging_setup import log_fields, task_logger
from data_integration.prefect_ui import TASK_ARCHIVE, TASK_ARCHIVE_DESC
from data_integration.storage.db import open_repository
from data_integration.storage.models import ProcessingRunStatus
from data_integration.storage.repository import DuplicateFileError, IntegrationRepository
from data_integration.storage.repository import REPLACEMENT_WITHOUT_REVISION_PREFIX

_LOGGER = task_logger(__name__)

REPLACEMENT_WITHOUT_REVISION_MESSAGE = REPLACEMENT_WITHOUT_REVISION_PREFIX


@dataclass(frozen=True)
class ArchiveCandidate:
    source_path: Path
    sha256: str
    size_bytes: int


@dataclass(frozen=True)
class RejectedCandidate:
    source_path: Path
    sha256: str
    size_bytes: int
    message: str
    conflicting_file_id: int | None = None


@task(name=TASK_ARCHIVE, description=TASK_ARCHIVE_DESC, retries=2, retry_delay_seconds=10)
def archive_files(config: IntegrationConfig) -> str | None:
    repository = open_repository(config.directories.sqlite_path)
    return archive_run_impl(config, repository)


def archive_run_impl(config: IntegrationConfig, repository: IntegrationRepository) -> str | None:
    logger = _LOGGER
    logger.info(
        "Scanning source directories for stable XML files | %s",
        log_fields(
            source_dirs=len(config.directories.source_dirs),
            stability_seconds=config.runtime.file_stability_seconds,
            config_revision=config.runtime.config_revision,
        ),
    )
    candidates, rejections, scan_stats = discover_archive_candidates(config, repository)
    logger.debug(
        "Archive scan stats | %s",
        log_fields(**scan_stats),
    )
    if not candidates and not rejections:
        logger.info("No new stable XML files found.")
        return None

    logger.info(
        "Discovered archive candidates | %s",
        log_fields(count=len(candidates), rejected=len(rejections)),
    )
    run = repository.create_run(config.snapshot())
    logger.info("Created processing run | %s", log_fields(run_id=run.id))
    archived_count = 0
    rejected_count = 0
    try:
        for rejection in rejections:
            rejected_count += _quarantine_rejected_candidate(config, repository, run.id, rejection)
            logger.warning(
                "Rejected source replacement | %s",
                log_fields(
                    run_id=run.id,
                    source=rejection.source_path,
                    sha256=rejection.sha256[:12],
                    reason=rejection.message,
                ),
            )

        for candidate in candidates:
            destination = archive_destination(
                config.directories.archive_dir,
                run.id,
                candidate.source_path,
            )
            try:
                record = repository.reserve_discovered_file(
                    run_id=run.id,
                    source_path=candidate.source_path,
                    archive_path=destination,
                    sha256=candidate.sha256,
                    size_bytes=candidate.size_bytes,
                    config_revision=config.runtime.config_revision,
                )
                copy_verify(candidate.source_path, destination, candidate.sha256)
                repository.mark_file_archived(record.id, destination)
                archived_count += 1
                logger.debug(
                    "Archived file | %s",
                    log_fields(
                        run_id=run.id,
                        file_id=record.id,
                        source=candidate.source_path.name,
                        destination=destination,
                        sha256=candidate.sha256[:12],
                        size_bytes=candidate.size_bytes,
                    ),
                )
            except DuplicateFileError:
                destination.unlink(missing_ok=True)
                logger.warning(
                    "Skipped duplicate source file | %s",
                    log_fields(source=candidate.source_path, sha256=candidate.sha256[:12]),
                )

        if archived_count == 0 and rejected_count == 0:
            repository.mark_run(run.id, ProcessingRunStatus.EMPTY, "No unique files archived.")
            logger.info("Archive run empty after deduplication | %s", log_fields(run_id=run.id))
            return None

        if archived_count == 0 and rejected_count > 0:
            repository.mark_run(
                run.id,
                ProcessingRunStatus.COMPLETED_WITH_ERRORS,
                f"{rejected_count} source replacement(s) rejected without config_revision bump.",
            )
            logger.warning(
                "Archive run rejected replacements only | %s",
                log_fields(run_id=run.id, rejected=rejected_count),
            )
            return run.id

        repository.mark_run(run.id, ProcessingRunStatus.ARCHIVED)
        logger.info(
            "Archive step complete | %s",
            log_fields(
                run_id=run.id,
                archived=archived_count,
                rejected=rejected_count,
                candidates=len(candidates),
            ),
        )
        return run.id
    except Exception as exc:
        repository.mark_run(run.id, ProcessingRunStatus.FAILED, str(exc))
        logger.exception("Archive step failed | %s", log_fields(run_id=run.id, error=str(exc)))
        raise


def discover_archive_candidates(
    config: IntegrationConfig,
    repository: IntegrationRepository,
) -> tuple[list[ArchiveCandidate], list[RejectedCandidate], dict[str, int]]:
    candidates: list[ArchiveCandidate] = []
    rejections: list[RejectedCandidate] = []
    stats = {
        "scanned": 0,
        "skipped_non_xml": 0,
        "skipped_unstable": 0,
        "skipped_duplicate": 0,
        "rejected_replacement": 0,
    }
    for source_dir in config.directories.source_dirs:
        if not source_dir.exists():
            raise FileNotFoundError(f"source directory does not exist: {source_dir}")
        _LOGGER.debug("Scanning source directory | %s", log_fields(path=source_dir))
        for source_path in sorted(source_dir.rglob("*.xml")):
            stats["scanned"] += 1
            if not is_xml_file(source_path):
                stats["skipped_non_xml"] += 1
                _LOGGER.debug("Skipped non-XML file | %s", log_fields(path=source_path))
                continue
            if not is_file_stable(source_path, config.runtime.file_stability_seconds):
                stats["skipped_unstable"] += 1
                _LOGGER.debug(
                    "Skipped unstable file | %s",
                    log_fields(path=source_path, stability_seconds=config.runtime.file_stability_seconds),
                )
                continue
            file_hash = sha256_file(source_path)
            if repository.has_seen_source_hash(
                source_path,
                file_hash,
                config_revision=config.runtime.config_revision,
            ):
                stats["skipped_duplicate"] += 1
                _LOGGER.debug(
                    "Skipped already-seen file | %s",
                    log_fields(path=source_path, sha256=file_hash[:12]),
                )
                continue

            conflicting = repository.find_conflicting_source_revision(
                source_path,
                file_hash,
                config_revision=config.runtime.config_revision,
            )
            if conflicting is not None:
                stats["rejected_replacement"] += 1
                rejections.append(
                    RejectedCandidate(
                        source_path=source_path,
                        sha256=file_hash,
                        size_bytes=source_path.stat().st_size,
                        message=REPLACEMENT_WITHOUT_REVISION_MESSAGE,
                        conflicting_file_id=conflicting.id,
                    )
                )
                _LOGGER.warning(
                    "Rejected source replacement without revision bump | %s",
                    log_fields(
                        path=source_path,
                        sha256=file_hash[:12],
                        config_revision=config.runtime.config_revision,
                        conflicting_file_id=conflicting.id,
                    ),
                )
                continue

            candidates.append(
                ArchiveCandidate(
                    source_path=source_path,
                    sha256=file_hash,
                    size_bytes=source_path.stat().st_size,
                )
            )
    return candidates, rejections, stats


def _quarantine_rejected_candidate(
    config: IntegrationConfig,
    repository: IntegrationRepository,
    run_id: str,
    rejection: RejectedCandidate,
) -> int:
    """Copy a rejected readonly-source file into quarantine and persist the rejection receipt."""
    quarantine_path = copy_to_quarantine(
        rejection.source_path,
        config.directories.quarantine_dir,
        run_id,
        "revision_policy_rejected",
    )
    archive_path = archive_destination(
        config.directories.archive_dir,
        run_id,
        rejection.source_path,
    )
    repository.record_rejected_replacement(
        run_id=run_id,
        source_path=rejection.source_path,
        archive_path=archive_path,
        sha256=rejection.sha256,
        size_bytes=rejection.size_bytes,
        config_revision=config.runtime.config_revision,
        quarantine_path=quarantine_path,
        message=rejection.message,
        conflicting_file_id=rejection.conflicting_file_id,
    )
    return 1
