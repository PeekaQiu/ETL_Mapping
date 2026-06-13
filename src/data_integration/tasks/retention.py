from __future__ import annotations

import time
from datetime import datetime, timedelta
from pathlib import Path

from prefect import task

from data_integration.config.schema import IntegrationConfig
from data_integration.storage.db import build_engine, build_session_factory, init_db
from data_integration.storage.models import FileStatus
from data_integration.storage.repository import IntegrationRepository


TERMINAL_STATUSES = [
    FileStatus.PUBLISHED,
    FileStatus.QUARANTINED,
    FileStatus.FAILED,
]


@task(name="Step 4: Cleanup Archive Retention", retries=0)
def cleanup_archive_retention(config: IntegrationConfig) -> int:
    engine = build_engine(config.directories.sqlite_path)
    init_db(engine)
    repository = IntegrationRepository(build_session_factory(engine))
    return cleanup_archive_retention_impl(config, repository)


def cleanup_archive_retention_impl(config: IntegrationConfig, repository: IntegrationRepository) -> int:
    cutoff = time.time() - (config.runtime.archive_retention_days * 24 * 60 * 60)
    deleted = 0
    for status in TERMINAL_STATUSES:
        for record in repository.get_files_for_status(status):
            archive_path = Path(record.archive_path)
            if archive_path.exists() and archive_path.stat().st_mtime <= cutoff:
                archive_path.unlink()
                deleted += 1
    if config.runtime.cleanup_empty_dirs:
        for root in [
            config.directories.archive_dir,
            config.directories.staging_dir,
            config.directories.quarantine_dir,
        ]:
            deleted += remove_empty_dirs(root)
    if config.runtime.detail_retention_days is not None:
        detail_cutoff = datetime.now() - timedelta(days=config.runtime.detail_retention_days)
        repository.delete_old_detail_records(detail_cutoff)
    return deleted


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
