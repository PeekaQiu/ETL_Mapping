from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from sqlalchemy import delete, select
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

    def has_seen_source_hash(self, source_path: Path, sha256: str) -> bool:
        with self._session_factory() as session:
            stmt = select(FileRecord.status).where(
                FileRecord.source_path == str(source_path),
                FileRecord.sha256 == sha256,
            )
            row = session.execute(stmt).first()
            if row is None:
                return False
            return row[0] != FileStatus.DISCOVERED.value

    def reserve_discovered_file(
        self,
        *,
        run_id: str,
        source_path: Path,
        archive_path: Path,
        sha256: str,
        size_bytes: int,
    ) -> FileRecord:
        with self._session_factory.begin() as session:
            existing = session.scalar(
                select(FileRecord).where(
                    FileRecord.source_path == str(source_path),
                    FileRecord.sha256 == sha256,
                )
            )
            if existing is not None:
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
            record.status = FileStatus.ARCHIVED_B.value
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
        target_path: Path,
    ) -> None:
        with self._session_factory.begin() as session:
            if self._target_exists(session, target_path):
                raise TargetCollisionError(f"target already exists in state store: {target_path}")
            record = self._get_file(session, file_id)
            record.staging_path = str(staging_path)
            record.target_path = str(target_path)
            record.status = FileStatus.CLASSIFIED_STAGING.value
            record.error_message = None

    def mark_file_validated(self, file_id: int) -> None:
        self._mark_file_status(file_id, FileStatus.VALIDATED)

    def mark_file_published(self, file_id: int, target_path: Path) -> None:
        with self._session_factory.begin() as session:
            record = self._get_file(session, file_id)
            record.target_path = str(target_path)
            record.status = FileStatus.PUBLISHED.value

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

    def summarize_run(self, run_id: str) -> dict[str, int]:
        files = self.get_run_files(run_id)
        summary: dict[str, int] = {}
        for record in files:
            summary[record.status] = summary.get(record.status, 0) + 1
        return summary

    def delete_old_detail_records(self, cutoff: datetime) -> int:
        with self._session_factory.begin() as session:
            old_file_ids = select(FileRecord.id).join(ProcessingRun).where(ProcessingRun.created_at < cutoff)
            rule_result = session.execute(delete(RuleMatch).where(RuleMatch.file_id.in_(old_file_ids)))
            validation_result = session.execute(delete(ValidationEvent).where(ValidationEvent.created_at < cutoff))
            return (rule_result.rowcount or 0) + (validation_result.rowcount or 0)

    def _mark_file_status(self, file_id: int, status: FileStatus) -> None:
        with self._session_factory.begin() as session:
            record = self._get_file(session, file_id)
            record.status = status.value

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
    def _target_exists(session: Session, target_path: Path) -> bool:
        stmt = select(FileRecord.id).where(
            FileRecord.target_path == str(target_path),
            FileRecord.status.in_(
                [
                    FileStatus.CLASSIFIED_STAGING.value,
                    FileStatus.VALIDATED.value,
                    FileStatus.PUBLISHED.value,
                ]
            ),
        )
        return session.execute(stmt).first() is not None
