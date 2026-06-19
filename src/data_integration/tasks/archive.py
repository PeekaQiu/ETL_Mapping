from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from prefect import task

from data_integration.config.schema import IntegrationConfig
from data_integration.files.safety import (
    archive_destination,
    copy_verify,
    is_file_stable,
    is_xml_file,
    sha256_file,
)
from data_integration.logging_setup import log_fields, task_logger
from data_integration.prefect_ui import TASK_ARCHIVE, TASK_ARCHIVE_DESC
from data_integration.storage.db import build_engine, build_session_factory, init_db
from data_integration.storage.models import ProcessingRunStatus
from data_integration.storage.repository import DuplicateFileError, IntegrationRepository

_LOGGER = task_logger(__name__)


@dataclass(frozen=True)
class ArchiveCandidate:
    source_path: Path
    sha256: str
    size_bytes: int


@task(name=TASK_ARCHIVE, description=TASK_ARCHIVE_DESC, retries=2, retry_delay_seconds=10)
def archive_files(config: IntegrationConfig) -> str | None:
    engine = build_engine(config.directories.sqlite_path)
    init_db(engine)
    repository = IntegrationRepository(build_session_factory(engine))
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
    candidates, scan_stats = discover_archive_candidates(config, repository)
    logger.debug(
        "Archive scan stats | %s",
        log_fields(**scan_stats),
    )
    if not candidates:
        logger.info("No new stable XML files found.")
        return None

    logger.info("Discovered archive candidates | %s", log_fields(count=len(candidates)))
    run = repository.create_run(config.snapshot())
    logger.info("Created processing run | %s", log_fields(run_id=run.id))
    archived_count = 0
    try:
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

        if archived_count == 0:
            repository.mark_run(run.id, ProcessingRunStatus.EMPTY, "No unique files archived.")
            logger.info("Archive run empty after deduplication | %s", log_fields(run_id=run.id))
            return None

        repository.mark_run(run.id, ProcessingRunStatus.ARCHIVED)
        logger.info(
            "Archive step complete | %s",
            log_fields(run_id=run.id, archived=archived_count, candidates=len(candidates)),
        )
        return run.id
    except Exception as exc:
        repository.mark_run(run.id, ProcessingRunStatus.FAILED, str(exc))
        logger.exception("Archive step failed | %s", log_fields(run_id=run.id, error=str(exc)))
        raise


def discover_archive_candidates(
    config: IntegrationConfig,
    repository: IntegrationRepository,
) -> tuple[list[ArchiveCandidate], dict[str, int]]:
    candidates: list[ArchiveCandidate] = []
    stats = {
        "scanned": 0,
        "skipped_non_xml": 0,
        "skipped_unstable": 0,
        "skipped_duplicate": 0,
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
            candidates.append(
                ArchiveCandidate(
                    source_path=source_path,
                    sha256=file_hash,
                    size_bytes=source_path.stat().st_size,
                )
            )
    return candidates, stats
