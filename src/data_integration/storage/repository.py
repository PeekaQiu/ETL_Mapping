from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from data_integration.storage.models import (
    FileRecord,
    FileStatus,
    ProcessingRun,
    ProcessingRunStatus,
    RuleMatch,
    RuleMatchStatus,
    ValidationEvent,
    ValidationStatus,
)


class DuplicateFileError(RuntimeError):
    pass


class TargetCollisionError(RuntimeError):
    pass


class SourcePathReplacementError(RuntimeError):
    """Same source path changed content without a config_revision bump."""


REPLACEMENT_WITHOUT_REVISION_PREFIX = "same source path replaced without config_revision bump"


def is_revision_policy_rejection(message: str | None) -> bool:
    return bool(message and message.startswith(REPLACEMENT_WITHOUT_REVISION_PREFIX))


_RETRYABLE_FILE_STATUSES = frozenset(
    {FileStatus.QUARANTINED.value, FileStatus.FAILED.value}
)
_SUPERSEDED_FILE_STATUSES = frozenset({FileStatus.SUPERSEDED.value})
_ACTIVE_REPLACE_STATUSES = frozenset(
    {
        FileStatus.CLASSIFIED_STAGING.value,
        FileStatus.PREPUBLISHED.value,
    }
)
_OCCUPIED_TARGET_STATUSES = frozenset(
    {
        FileStatus.CLASSIFIED_STAGING.value,
        FileStatus.PREPUBLISHED.value,
        FileStatus.PUBLISHED.value,
    }
)
_TERMINAL_RETENTION_STATUSES = (
    FileStatus.PUBLISHED,
    FileStatus.QUARANTINED,
    FileStatus.FAILED,
)
_NON_CONFLICTING_STATUSES = frozenset(
    _RETRYABLE_FILE_STATUSES | _SUPERSEDED_FILE_STATUSES | {FileStatus.DISCOVERED.value}
)


class IntegrationRepository:
    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def create_run(self, config_snapshot: dict) -> ProcessingRun:
        run = ProcessingRun(id=self.new_run_id(), config_snapshot=config_snapshot)
        with self._session_factory.begin() as session:
            session.add(run)
        return run

    def mark_run(self, run_id: str, status: ProcessingRunStatus, error_message: str | None = None) -> None:
        with self._session_factory.begin() as session:
            run = self._get_run(session, run_id)
            run.status = status.value
            run.error_message = error_message

    def get_run(self, run_id: str) -> ProcessingRun:
        with self._session_factory() as session:
            run = self._get_run(session, run_id)
            session.expunge(run)
            return run

    def get_latest_run_config_snapshot(self) -> dict | None:
        with self._session_factory() as session:
            run = session.scalar(
                select(ProcessingRun)
                .where(ProcessingRun.config_snapshot.is_not(None))
                .order_by(ProcessingRun.created_at.desc(), ProcessingRun.id.desc())
            )
            if run is None or not run.config_snapshot:
                return None
            return dict(run.config_snapshot)

    def find_conflicting_source_revision(
        self,
        source_path: Path,
        sha256: str,
        *,
        config_revision: int,
    ) -> FileRecord | None:
        """Return an existing record when the same path already has different content at this revision."""
        with self._session_factory() as session:
            stmt = (
                select(FileRecord)
                .where(
                    FileRecord.source_path == str(source_path),
                    FileRecord.config_revision == config_revision,
                    FileRecord.sha256 != sha256,
                    ~FileRecord.status.in_(_NON_CONFLICTING_STATUSES),
                )
                .order_by(FileRecord.id.desc())
            )
            record = session.scalar(stmt)
            if record is None:
                return None
            session.expunge(record)
            return record

    def record_rejected_replacement(
        self,
        *,
        run_id: str,
        source_path: Path,
        archive_path: Path,
        sha256: str,
        size_bytes: int,
        config_revision: int,
        quarantine_path: Path,
        message: str,
        conflicting_file_id: int | None = None,
    ) -> FileRecord:
        if conflicting_file_id is not None:
            message = f"{message} (conflicts with file_id={conflicting_file_id})"
        with self._session_factory.begin() as session:
            record = FileRecord(
                run_id=run_id,
                source_path=str(source_path),
                archive_path=str(archive_path),
                sha256=sha256,
                size_bytes=size_bytes,
                config_revision=config_revision,
                quarantine_path=str(quarantine_path),
                status=FileStatus.QUARANTINED.value,
                error_message=message,
            )
            session.add(record)
            session.flush()
            return record

    def has_seen_source_hash(self, source_path: Path, sha256: str, *, config_revision: int) -> bool:
        with self._session_factory() as session:
            stmt = select(FileRecord).where(
                FileRecord.source_path == str(source_path),
                FileRecord.sha256 == sha256,
                FileRecord.config_revision == config_revision,
            )
            record = session.scalar(stmt)
            if record is None:
                return False
            if record.status in _RETRYABLE_FILE_STATUSES:
                if is_revision_policy_rejection(record.error_message):
                    return True
                return False
            return record.status != FileStatus.DISCOVERED.value

    def reserve_discovered_file(
        self,
        *,
        run_id: str,
        source_path: Path,
        archive_path: Path,
        sha256: str,
        size_bytes: int,
        config_revision: int,
    ) -> FileRecord:
        with self._session_factory.begin() as session:
            existing = session.scalar(
                select(FileRecord).where(
                    FileRecord.source_path == str(source_path),
                    FileRecord.sha256 == sha256,
                    FileRecord.config_revision == config_revision,
                )
            )
            if existing is not None:
                if existing.status in _RETRYABLE_FILE_STATUSES:
                    existing.run_id = run_id
                    existing.archive_path = str(archive_path)
                    existing.size_bytes = size_bytes
                    existing.staging_path = None
                    existing.target_path = None
                    existing.logical_target_path = None
                    existing.final_target_path = None
                    existing.prepublish_path = None
                    existing.quarantine_path = None
                    existing.publish_mode = None
                    existing.superseded_by_file_id = None
                    existing.status = FileStatus.DISCOVERED.value
                    existing.error_message = None
                    return existing
                if existing.status != FileStatus.DISCOVERED.value:
                    raise DuplicateFileError(f"file already processed: {source_path}")
                existing.run_id = run_id
                existing.archive_path = str(archive_path)
                existing.size_bytes = size_bytes
                existing.error_message = None
                return existing

            record = FileRecord(
                run_id=run_id,
                source_path=str(source_path),
                archive_path=str(archive_path),
                sha256=sha256,
                size_bytes=size_bytes,
                config_revision=config_revision,
                status=FileStatus.DISCOVERED.value,
            )
            try:
                session.add(record)
                session.flush()
                return record
            except IntegrityError as exc:
                raise DuplicateFileError(f"file already reserved: {source_path}") from exc

    def mark_file_archived(self, file_id: int, archive_path: Path) -> None:
        with self._session_factory.begin() as session:
            record = self._get_file(session, file_id)
            record.archive_path = str(archive_path)
            record.status = FileStatus.ARCHIVED.value
            record.error_message = None

    def get_run_files(self, run_id: str, statuses: Iterable[FileStatus] | None = None) -> list[FileRecord]:
        with self._session_factory() as session:
            stmt = select(FileRecord).where(FileRecord.run_id == run_id)
            if statuses is not None:
                stmt = stmt.where(FileRecord.status.in_([status.value for status in statuses]))
            records = list(session.scalars(stmt).all())
            for record in records:
                session.expunge(record)
            return records

    def get_files_for_status(self, status: FileStatus) -> list[FileRecord]:
        with self._session_factory() as session:
            stmt = select(FileRecord).where(FileRecord.status == status.value)
            records = list(session.scalars(stmt).all())
            for record in records:
                session.expunge(record)
            return records

    def count_files_by_status(self, status: FileStatus) -> int:
        with self._session_factory() as session:
            stmt = select(func.count()).select_from(FileRecord).where(FileRecord.status == status.value)
            return int(session.scalar(stmt) or 0)

    def list_retention_files(self, cutoff: datetime) -> list[FileRecord]:
        with self._session_factory() as session:
            stmt = (
                select(FileRecord)
                .join(ProcessingRun, FileRecord.run_id == ProcessingRun.id)
                .where(
                    FileRecord.status.in_([status.value for status in _TERMINAL_RETENTION_STATUSES]),
                    ProcessingRun.created_at < cutoff,
                )
            )
            records = list(session.scalars(stmt).all())
            for record in records:
                session.expunge(record)
            return records

    def record_rule_match(
        self,
        *,
        file_id: int,
        status: RuleMatchStatus,
        rule_id: str | None,
        details: dict,
    ) -> None:
        with self._session_factory.begin() as session:
            session.add(
                RuleMatch(
                    file_id=file_id,
                    status=status.value,
                    rule_id=rule_id,
                    details=details,
                )
            )

    def mark_file_classified(
        self,
        *,
        file_id: int,
        staging_path: Path,
        logical_target_path: Path,
        final_target_path: Path,
        prepublish_path: Path,
        publish_mode: str,
    ) -> None:
        with self._session_factory.begin() as session:
            record = self._get_file(session, file_id)
            if self._target_exists(
                session,
                logical_target_path=logical_target_path,
                final_target_path=final_target_path,
                publish_mode=publish_mode,
                source_path=Path(record.source_path),
                sha256=record.sha256,
                config_revision=record.config_revision,
                file_id=file_id,
            ):
                raise TargetCollisionError(f"target already exists in state store: {final_target_path}")
            record.staging_path = str(staging_path)
            record.target_path = str(final_target_path)
            record.logical_target_path = str(logical_target_path)
            record.final_target_path = str(final_target_path)
            record.prepublish_path = str(prepublish_path)
            record.publish_mode = publish_mode
            record.status = FileStatus.CLASSIFIED_STAGING.value
            record.error_message = None

    def mark_file_prepublished(self, file_id: int, prepublish_path: Path) -> None:
        with self._session_factory.begin() as session:
            record = self._get_file(session, file_id)
            record.prepublish_path = str(prepublish_path)
            record.status = FileStatus.PREPUBLISHED.value
            record.error_message = None

    def mark_file_published(self, file_id: int, target_path: Path) -> None:
        with self._session_factory.begin() as session:
            record = self._get_file(session, file_id)
            record.target_path = str(target_path)
            record.final_target_path = str(target_path)
            record.status = FileStatus.PUBLISHED.value

    def mark_file_superseded(
        self,
        file_id: int,
        *,
        superseded_by_file_id: int,
        quarantine_path: Path | None = None,
        message: str | None = None,
    ) -> None:
        with self._session_factory.begin() as session:
            record = self._get_file(session, file_id)
            record.superseded_by_file_id = superseded_by_file_id
            if quarantine_path is not None:
                record.quarantine_path = str(quarantine_path)
            record.status = FileStatus.SUPERSEDED.value
            record.error_message = message

    def mark_file_failed(self, file_id: int, message: str) -> None:
        with self._session_factory.begin() as session:
            record = self._get_file(session, file_id)
            record.status = FileStatus.FAILED.value
            record.error_message = message

    def mark_file_quarantined(self, file_id: int, quarantine_path: Path, message: str) -> None:
        with self._session_factory.begin() as session:
            record = self._get_file(session, file_id)
            record.quarantine_path = str(quarantine_path)
            record.status = FileStatus.QUARANTINED.value
            record.error_message = message

    def add_validation_event(
        self,
        *,
        run_id: str,
        status: ValidationStatus,
        message: str,
        details: dict | None = None,
    ) -> None:
        with self._session_factory.begin() as session:
            session.add(
                ValidationEvent(
                    run_id=run_id,
                    status=status.value,
                    message=message,
                    details=details or {},
                )
            )

    def find_latest_replace(
        self,
        *,
        source_path: Path,
        logical_target_path: Path,
    ) -> FileRecord | None:
        with self._session_factory() as session:
            stmt = (
                select(FileRecord)
                .where(
                    FileRecord.source_path == str(source_path),
                    FileRecord.logical_target_path == str(logical_target_path),
                    FileRecord.publish_mode == "replace",
                    ~FileRecord.status.in_(_RETRYABLE_FILE_STATUSES | _SUPERSEDED_FILE_STATUSES),
                )
                .order_by(FileRecord.config_revision.desc(), FileRecord.id.desc())
            )
            record = session.scalar(stmt)
            if record is None:
                return None
            session.expunge(record)
            return record

    def find_superseding_record(self, record: FileRecord) -> FileRecord | None:
        if record.publish_mode != "replace" or not record.logical_target_path:
            return None
        latest = self.find_latest_replace(
            source_path=Path(record.source_path),
            logical_target_path=Path(record.logical_target_path),
        )
        if latest is None or latest.id == record.id:
            return None
        if (latest.config_revision, latest.id) > (record.config_revision, record.id):
            return latest
        return None

    def find_older_replace_records(self, record: FileRecord) -> list[FileRecord]:
        if record.publish_mode != "replace" or not record.logical_target_path:
            return []
        with self._session_factory() as session:
            stmt = select(FileRecord).where(
                FileRecord.source_path == record.source_path,
                FileRecord.logical_target_path == record.logical_target_path,
                FileRecord.publish_mode == "replace",
                FileRecord.status.in_(_ACTIVE_REPLACE_STATUSES),
                FileRecord.id != record.id,
            )
            candidates = list(session.scalars(stmt).all())
            older: list[FileRecord] = []
            for existing in candidates:
                if existing.config_revision < record.config_revision:
                    older.append(existing)
                elif existing.config_revision == record.config_revision and existing.id < record.id:
                    older.append(existing)
            for item in older:
                session.expunge(item)
            return older

    def supersede_older_records(
        self,
        record: FileRecord,
        *,
        quarantine_paths: dict[int, Path | None] | None = None,
    ) -> int:
        paths = quarantine_paths or {}
        older_records = self.find_older_replace_records(record)
        for older in older_records:
            self.mark_file_superseded(
                older.id,
                superseded_by_file_id=record.id,
                quarantine_path=paths.get(older.id),
                message=(
                    f"superseded by newer revision {record.config_revision} "
                    f"for {record.logical_target_path}"
                ),
            )
        return len(older_records)

    def summarize_run(self, run_id: str) -> dict:
        files = self.get_run_files(run_id)
        by_status: dict[str, int] = {}
        success = 0
        ready_to_publish = 0
        failed = 0
        failed_files: list[dict[str, str | None]] = []
        for record in files:
            by_status[record.status] = by_status.get(record.status, 0) + 1
            if record.status == FileStatus.PUBLISHED.value:
                success += 1
            elif record.status == FileStatus.PREPUBLISHED.value:
                ready_to_publish += 1
            elif record.status in _RETRYABLE_FILE_STATUSES:
                failed += 1
                failed_files.append(
                    {
                        "name": Path(record.source_path).name,
                        "source_path": record.source_path,
                        "quarantine_path": record.quarantine_path,
                        "reason": record.error_message,
                    }
                )
        return {
            "total": len(files),
            "success": success,
            "ready_to_publish": ready_to_publish,
            "failed": failed,
            "failed_files": failed_files,
            "by_status": by_status,
        }

    def delete_old_detail_records(self, cutoff: datetime) -> int:
        with self._session_factory.begin() as session:
            old_file_ids = select(FileRecord.id).join(ProcessingRun).where(ProcessingRun.created_at < cutoff)
            rule_result = session.execute(delete(RuleMatch).where(RuleMatch.file_id.in_(old_file_ids)))
            validation_result = session.execute(delete(ValidationEvent).where(ValidationEvent.created_at < cutoff))
            return (rule_result.rowcount or 0) + (validation_result.rowcount or 0)

    @staticmethod
    def new_run_id() -> str:
        return uuid4().hex

    @staticmethod
    def _get_run(session: Session, run_id: str) -> ProcessingRun:
        run = session.get(ProcessingRun, run_id)
        if run is None:
            raise KeyError(f"processing run not found: {run_id}")
        return run

    @staticmethod
    def _get_file(session: Session, file_id: int) -> FileRecord:
        record = session.get(FileRecord, file_id)
        if record is None:
            raise KeyError(f"file not found: {file_id}")
        return record

    @staticmethod
    def _target_exists(
        session: Session,
        *,
        logical_target_path: Path,
        final_target_path: Path,
        publish_mode: str,
        source_path: Path,
        sha256: str,
        config_revision: int,
        file_id: int,
    ) -> bool:
        if publish_mode == "replace":
            stmt = select(FileRecord).where(
                FileRecord.logical_target_path == str(logical_target_path),
                FileRecord.publish_mode == "replace",
                FileRecord.status.in_(_ACTIVE_REPLACE_STATUSES),
                FileRecord.id != file_id,
            )
            for existing in session.scalars(stmt):
                if existing.source_path != str(source_path):
                    return True
                if existing.config_revision < config_revision:
                    continue
                if existing.config_revision == config_revision and existing.sha256 == sha256:
                    continue
                return True
            return False

        stmt = select(FileRecord.id).where(
            FileRecord.final_target_path == str(final_target_path),
            FileRecord.status.in_(_OCCUPIED_TARGET_STATUSES),
            FileRecord.id != file_id,
        )
        return session.execute(stmt).first() is not None
