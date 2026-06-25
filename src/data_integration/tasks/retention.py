from __future__ import annotations

import time
from datetime import datetime, timedelta
from pathlib import Path

from prefect import task

from data_integration.config.schema import IntegrationConfig
from data_integration.logging_setup import log_fields, task_logger
from data_integration.prefect_ui import TASK_RETENTION, TASK_RETENTION_DESC
from data_integration.storage.db import open_repository
from data_integration.storage.repository import IntegrationRepository

_LOGGER = task_logger(__name__)


@task(name=TASK_RETENTION, description=TASK_RETENTION_DESC, retries=0)
def apply_retention(config: IntegrationConfig) -> int:
    repository = open_repository(config.directories.sqlite_path)
    return retention_run_impl(config, repository)


def retention_run_impl(config: IntegrationConfig, repository: IntegrationRepository) -> int:
    logger = _LOGGER
    logger.info(
        "Starting archive retention cleanup | %s",
        log_fields(
            retention_days=config.runtime.archive_retention_days,
            cleanup_empty_dirs=config.runtime.cleanup_empty_dirs,
            detail_retention_days=config.runtime.detail_retention_days,
        ),
    )

    run_cutoff = datetime.now() - timedelta(days=config.runtime.archive_retention_days)
    archive_mtime_cutoff = time.time() - (config.runtime.archive_retention_days * 24 * 60 * 60)
    deleted_archives = 0
    for record in repository.list_retention_files(run_cutoff):
        archive_path = Path(record.archive_path)
        if archive_path.exists() and archive_path.stat().st_mtime <= archive_mtime_cutoff:
            archive_path.unlink()
            deleted_archives += 1
            logger.debug(
                "Deleted expired archive file | %s",
                log_fields(path=archive_path, status=record.status, run_id=record.run_id),
            )

    deleted_dirs = 0
    if config.runtime.cleanup_empty_dirs:
        for root in [
            config.directories.archive_dir,
            config.directories.staging_dir,
            config.directories.quarantine_dir,
        ]:
            removed = remove_empty_dirs(root)
            deleted_dirs += removed
            if removed:
                logger.debug("Removed empty directories | %s", log_fields(root=root, count=removed))

    detail_records_deleted = 0
    if config.runtime.detail_retention_days is not None:
        detail_cutoff = datetime.now() - timedelta(days=config.runtime.detail_retention_days)
        detail_records_deleted = repository.delete_old_detail_records(detail_cutoff)
        logger.debug(
            "Purged old detail records | %s",
            log_fields(cutoff=detail_cutoff.isoformat(), deleted=detail_records_deleted),
        )

    total_deleted = deleted_archives + deleted_dirs
    logger.info(
        "Retention cleanup complete | %s",
        log_fields(
            deleted_archives=deleted_archives,
            removed_empty_dirs=deleted_dirs,
            purged_detail_records=detail_records_deleted,
            total=total_deleted,
        ),
    )
    return total_deleted


def remove_empty_dirs(root: Path) -> int:
    if not root.exists():
        return 0
    removed = 0
    for path in sorted([path for path in root.rglob("*") if path.is_dir()], key=lambda item: len(item.parts), reverse=True):
        try:
            path.rmdir()
            removed += 1
        except OSError:
            continue
    return removed
