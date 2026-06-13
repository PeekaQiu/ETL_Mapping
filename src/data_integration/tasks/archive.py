from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from prefect import get_run_logger, task

from data_integration.config.schema import IntegrationConfig
from data_integration.files.safety import (
    archive_destination,
    copy_verify,
    is_file_stable,
    is_xml_file,
    sha256_file,
)
from data_integration.storage.db import build_engine, build_session_factory, init_db
from data_integration.storage.models import ProcessingRunStatus
from data_integration.storage.repository import DuplicateFileError, IntegrationRepository


@dataclass(frozen=True)
class ArchiveCandidate:
    source_path: Path
    sha256: str
    size_bytes: int


@task(name="Step 1: Archive Files", retries=2, retry_delay_seconds=10)
def archive_files(config: IntegrationConfig) -> str | None:
    engine = build_engine(config.directories.sqlite_path)
    init_db(engine)
    repository = IntegrationRepository(build_session_factory(engine))
    return archive_run_impl(config, repository)


def archive_run_impl(config: IntegrationConfig, repository: IntegrationRepository) -> str | None:
    logger = _logger()
    candidates = discover_archive_candidates(config, repository)
    if not candidates:
        logger.info("No new stable XML files found.")
        return None

    run = repository.create_run(config.snapshot())
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
                )
                copy_verify(candidate.source_path, destination, candidate.sha256)
                repository.mark_file_archived(record.id, destination)
                archived_count += 1
            except DuplicateFileError:
                destination.unlink(missing_ok=True)
                logger.warning("Skipped duplicate source file: %s", candidate.source_path)

        if archived_count == 0:
            repository.mark_run(run.id, ProcessingRunStatus.EMPTY, "No unique files archived.")
            return None

        repository.mark_run(run.id, ProcessingRunStatus.ARCHIVED)
        logger.info("Archived %s file(s) into source run %s.", archived_count, run.id)
        return run.id
    except Exception as exc:
        repository.mark_run(run.id, ProcessingRunStatus.FAILED, str(exc))
        raise


def discover_archive_candidates(
    config: IntegrationConfig,
    repository: IntegrationRepository,
) -> list[ArchiveCandidate]:
    candidates: list[ArchiveCandidate] = []
    for source_dir in config.directories.source_dirs:
        if not source_dir.exists():
            raise FileNotFoundError(f"source directory does not exist: {source_dir}")
        for source_path in sorted(source_dir.rglob("*.xml")):
            if not is_xml_file(source_path):
                continue
            if not is_file_stable(source_path, config.runtime.file_stability_seconds):
                continue
            file_hash = sha256_file(source_path)
            if repository.has_seen_source_hash(source_path, file_hash):
                continue
            candidates.append(
                ArchiveCandidate(
                    source_path=source_path,
                    sha256=file_hash,
                    size_bytes=source_path.stat().st_size,
                )
            )
    return candidates


def _logger() -> logging.Logger:
    try:
        return get_run_logger()
    except Exception:
        return logging.getLogger(__name__)
