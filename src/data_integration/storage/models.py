from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class ProcessingRunStatus(StrEnum):
    """Run-level lifecycle; advances with the pipeline, not per-file."""

    CREATED = "CREATED"
    EMPTY = "EMPTY"
    ARCHIVED = "ARCHIVED"
    CLASSIFIED = "CLASSIFIED"
    PREPUBLISHED = "PREPUBLISHED"
    PUBLISHED = "PUBLISHED"
    COMPLETED_WITH_ERRORS = "COMPLETED_WITH_ERRORS"
    FAILED = "FAILED"


class FileStatus(StrEnum):
    """Per-file lifecycle from discovery through publish or quarantine."""

    DISCOVERED = "DISCOVERED"
    ARCHIVED = "ARCHIVED"
    CLASSIFIED_STAGING = "CLASSIFIED_STAGING"
    PREPUBLISHED = "PREPUBLISHED"
    PUBLISHED = "PUBLISHED"
    SUPERSEDED = "SUPERSEDED"
    FAILED = "FAILED"
    QUARANTINED = "QUARANTINED"


class RuleMatchStatus(StrEnum):
    MATCHED = "MATCHED"
    NO_MATCH = "NO_MATCH"
    MULTIPLE_MATCH = "MULTIPLE_MATCH"
    RULE_ERROR = "RULE_ERROR"


class ValidationStatus(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"


class ProcessingRun(Base):
    __tablename__ = "processing_runs"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    status: Mapped[str] = mapped_column(String(32), default=ProcessingRunStatus.CREATED.value, index=True)
    config_snapshot: Mapped[dict] = mapped_column(JSON, default=dict)
    error_message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        server_default=func.now(),
        onupdate=func.now(),
    )

    files: Mapped[list["FileRecord"]] = relationship(back_populates="processing_run")
    validation_events: Mapped[list["ValidationEvent"]] = relationship(back_populates="processing_run")


class FileRecord(Base):
    __tablename__ = "files"
    __table_args__ = (
        UniqueConstraint("source_path", "sha256", "config_revision", name="uq_file_source_hash_revision"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("processing_runs.id"), index=True)
    source_path: Mapped[str] = mapped_column(Text)
    archive_path: Mapped[str] = mapped_column(Text)
    staging_path: Mapped[str | None] = mapped_column(Text)
    target_path: Mapped[str | None] = mapped_column(Text)
    logical_target_path: Mapped[str | None] = mapped_column(Text, index=True)
    final_target_path: Mapped[str | None] = mapped_column(Text, index=True)
    prepublish_path: Mapped[str | None] = mapped_column(Text)
    quarantine_path: Mapped[str | None] = mapped_column(Text)
    sha256: Mapped[str] = mapped_column(String(64), index=True)
    size_bytes: Mapped[int] = mapped_column(Integer)
    config_revision: Mapped[int] = mapped_column(Integer, default=1, index=True)
    publish_mode: Mapped[str | None] = mapped_column(String(16), index=True)
    superseded_by_file_id: Mapped[int | None] = mapped_column(ForeignKey("files.id"), index=True)
    status: Mapped[str] = mapped_column(String(32), default=FileStatus.DISCOVERED.value, index=True)
    error_message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        server_default=func.now(),
        onupdate=func.now(),
    )

    processing_run: Mapped[ProcessingRun] = relationship(back_populates="files")
    rule_matches: Mapped[list["RuleMatch"]] = relationship(back_populates="file")


class RuleMatch(Base):
    __tablename__ = "rule_matches"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    file_id: Mapped[int] = mapped_column(ForeignKey("files.id"), index=True)
    rule_id: Mapped[str | None] = mapped_column(String(128), index=True)
    status: Mapped[str] = mapped_column(String(32), index=True)
    details: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    file: Mapped[FileRecord] = relationship(back_populates="rule_matches")


class ValidationEvent(Base):
    __tablename__ = "validation_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("processing_runs.id"), index=True)
    status: Mapped[str] = mapped_column(String(16), index=True)
    message: Mapped[str] = mapped_column(Text)
    details: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    processing_run: Mapped[ProcessingRun] = relationship(back_populates="validation_events")
