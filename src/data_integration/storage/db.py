from __future__ import annotations

from pathlib import Path

from sqlalchemy import Engine, create_engine, inspect
from sqlalchemy.orm import Session, sessionmaker

from data_integration.storage.models import Base


def build_engine(sqlite_path: Path) -> Engine:
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(
        f"sqlite:///{sqlite_path}",
        connect_args={"check_same_thread": False},
        future=True,
    )


def init_db(engine: Engine) -> None:
    Base.metadata.create_all(engine)
    _ensure_files_schema(engine)
    _migrate_legacy_statuses(engine)


def build_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False, future=True)


def _ensure_files_schema(engine: Engine) -> None:
    inspector = inspect(engine)
    if "files" not in inspector.get_table_names():
        return

    columns = {column["name"] for column in inspector.get_columns("files")}
    required_columns = {
        "config_revision",
        "publish_mode",
        "logical_target_path",
        "final_target_path",
        "prepublish_path",
        "superseded_by_file_id",
    }
    if required_columns.issubset(columns):
        return

    with engine.begin() as connection:
        connection.exec_driver_sql("PRAGMA foreign_keys=OFF")
        connection.exec_driver_sql(
            """
            CREATE TABLE files__migration (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                run_id VARCHAR(64) NOT NULL,
                source_path TEXT NOT NULL,
                archive_path TEXT NOT NULL,
                staging_path TEXT,
                target_path TEXT,
                logical_target_path TEXT,
                final_target_path TEXT,
                prepublish_path TEXT,
                quarantine_path TEXT,
                sha256 VARCHAR(64) NOT NULL,
                size_bytes INTEGER NOT NULL,
                config_revision INTEGER NOT NULL DEFAULT 1,
                publish_mode VARCHAR(16),
                superseded_by_file_id INTEGER,
                status VARCHAR(32) NOT NULL,
                error_message TEXT,
                created_at DATETIME,
                updated_at DATETIME,
                CONSTRAINT uq_file_source_hash_revision UNIQUE (source_path, sha256, config_revision),
                FOREIGN KEY(run_id) REFERENCES processing_runs (id),
                FOREIGN KEY(superseded_by_file_id) REFERENCES files__migration (id)
            )
            """
        )
        connection.exec_driver_sql(
            """
            INSERT INTO files__migration (
                id,
                run_id,
                source_path,
                archive_path,
                staging_path,
                target_path,
                logical_target_path,
                final_target_path,
                prepublish_path,
                quarantine_path,
                sha256,
                size_bytes,
                config_revision,
                publish_mode,
                superseded_by_file_id,
                status,
                error_message,
                created_at,
                updated_at
            )
            SELECT
                id,
                run_id,
                source_path,
                archive_path,
                staging_path,
                target_path,
                target_path,
                target_path,
                NULL,
                quarantine_path,
                sha256,
                size_bytes,
                1,
                'replace',
                NULL,
                status,
                error_message,
                created_at,
                updated_at
            FROM files
            """
        )
        connection.exec_driver_sql("DROP TABLE files")
        connection.exec_driver_sql("ALTER TABLE files__migration RENAME TO files")
        connection.exec_driver_sql("CREATE INDEX ix_files_run_id ON files (run_id)")
        connection.exec_driver_sql("CREATE INDEX ix_files_sha256 ON files (sha256)")
        connection.exec_driver_sql("CREATE INDEX ix_files_status ON files (status)")
        connection.exec_driver_sql("CREATE INDEX ix_files_config_revision ON files (config_revision)")
        connection.exec_driver_sql("CREATE INDEX ix_files_publish_mode ON files (publish_mode)")
        connection.exec_driver_sql("CREATE INDEX ix_files_logical_target_path ON files (logical_target_path)")
        connection.exec_driver_sql("CREATE INDEX ix_files_final_target_path ON files (final_target_path)")
        connection.exec_driver_sql("CREATE INDEX ix_files_superseded_by_file_id ON files (superseded_by_file_id)")
        connection.exec_driver_sql("PRAGMA foreign_keys=ON")


def _migrate_legacy_statuses(engine: Engine) -> None:
    # One-shot SQL renames; safe to re-run (idempotent WHERE clauses)
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "UPDATE files SET status = 'ARCHIVED' WHERE status = 'ARCHIVED_B'"
        )
        connection.exec_driver_sql(
            "UPDATE files SET status = 'PREPUBLISHED' WHERE status = 'VALIDATED'"
        )
        connection.exec_driver_sql(
            "UPDATE processing_runs SET status = 'PREPUBLISHED' WHERE status = 'VALIDATED'"
        )
