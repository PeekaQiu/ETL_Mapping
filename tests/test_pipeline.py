from pathlib import Path

from data_integration.config.schema import IntegrationConfig
from data_integration.storage.db import build_engine, build_session_factory, init_db
from data_integration.storage.models import FileStatus, ProcessingRunStatus
from data_integration.storage.repository import IntegrationRepository
from data_integration.files.safety import sha256_file
from data_integration.tasks.publish import publish_prepublished_run_impl
from data_integration.tasks.archive import archive_run_impl
from data_integration.tasks.classify import classify_run_impl
from data_integration.tasks.validate import validate_and_prepublish_run_impl, validate_and_publish_run_impl


def test_pipeline_archives_classifies_and_publishes(tmp_path: Path) -> None:
    config = _config(tmp_path)
    source_dir = config.directories.source_dirs[0]
    source_dir.mkdir(parents=True)
    (source_dir / "invoice.xml").write_text(
        "<Document><Type>INVOICE</Type><Amount>42.50</Amount></Document>",
        encoding="utf-8",
    )
    repository = _repository(config)

    run_id = archive_run_impl(config, repository)
    assert run_id is not None
    classify_run_impl(config, repository, run_id)
    validate_and_publish_run_impl(config, repository, run_id)

    run = repository.get_run(run_id)
    files = repository.get_run_files(run_id)
    summary = repository.summarize_run(run_id)
    assert run.status == ProcessingRunStatus.PUBLISHED.value
    assert summary["total"] == 1
    assert summary["success"] == 1
    assert summary["failed"] == 0
    assert summary["failed_files"] == []
    assert files[0].status == FileStatus.PUBLISHED.value
    assert (config.directories.output_root / "invoice" / "invoice.xml").exists()
    assert files[0].staging_path is not None
    assert not Path(files[0].staging_path).exists()


def test_pipeline_prepublishes_before_scheduled_publish(tmp_path: Path) -> None:
    config = _config(tmp_path)
    source_dir = config.directories.source_dirs[0]
    source_dir.mkdir(parents=True)
    (source_dir / "invoice.xml").write_text(
        "<Document><Type>INVOICE</Type><Amount>42.50</Amount></Document>",
        encoding="utf-8",
    )
    repository = _repository(config)

    run_id = archive_run_impl(config, repository)
    assert run_id is not None
    classify_run_impl(config, repository, run_id)
    validate_and_prepublish_run_impl(config, repository, run_id)

    prepublished_file = repository.get_run_files(run_id)[0]
    assert repository.get_run(run_id).status == ProcessingRunStatus.VALIDATED.value
    assert prepublished_file.status == FileStatus.PREPUBLISHED.value
    assert prepublished_file.prepublish_path is not None
    assert Path(prepublished_file.prepublish_path).exists()
    assert not (config.directories.output_root / "invoice" / "invoice.xml").exists()

    published_count = publish_prepublished_run_impl(config, repository, run_id=run_id)

    published_file = repository.get_run_files(run_id)[0]
    assert published_count == 1
    assert repository.get_run(run_id).status == ProcessingRunStatus.PUBLISHED.value
    assert published_file.status == FileStatus.PUBLISHED.value
    assert (config.directories.output_root / "invoice" / "invoice.xml").exists()
    assert published_file.prepublish_path is not None
    assert not Path(published_file.prepublish_path).exists()


def test_newer_replace_run_wins_formal_publish(tmp_path: Path) -> None:
    config_v1 = _config(tmp_path, config_revision=1)
    source_dir = config_v1.directories.source_dirs[0]
    source_dir.mkdir(parents=True)
    source_path = source_dir / "invoice.xml"
    source_path.write_text("<Document><Type>INVOICE</Type></Document>", encoding="utf-8")
    repository = _repository(config_v1)

    first_run_id = archive_run_impl(config_v1, repository)
    assert first_run_id is not None
    classify_run_impl(config_v1, repository, first_run_id)
    validate_and_prepublish_run_impl(config_v1, repository, first_run_id)

    config_v2 = _config(tmp_path, config_revision=2)
    second_run_id = archive_run_impl(config_v2, repository)
    assert second_run_id is not None
    classify_run_impl(config_v2, repository, second_run_id)
    validate_and_prepublish_run_impl(config_v2, repository, second_run_id)

    published_count = publish_prepublished_run_impl(config_v2, repository)

    first_file = repository.get_run_files(first_run_id)[0]
    second_file = repository.get_run_files(second_run_id)[0]
    assert published_count == 1
    assert first_file.status == FileStatus.SUPERSEDED.value
    assert first_file.superseded_by_file_id == second_file.id
    assert second_file.status == FileStatus.PUBLISHED.value
    assert repository.get_run(first_run_id).status == ProcessingRunStatus.COMPLETED_WITH_ERRORS.value
    assert repository.get_run(second_run_id).status == ProcessingRunStatus.PUBLISHED.value
    assert (config_v2.directories.output_root / "invoice" / "invoice.xml").exists()


def test_specific_publish_run_skips_superseded_replace_prepublish(tmp_path: Path) -> None:
    config_v1 = _config(tmp_path, config_revision=1)
    source_dir = config_v1.directories.source_dirs[0]
    source_dir.mkdir(parents=True)
    source_path = source_dir / "invoice.xml"
    source_path.write_text("<Document><Type>INVOICE</Type></Document>", encoding="utf-8")
    repository = _repository(config_v1)

    first_run_id = archive_run_impl(config_v1, repository)
    assert first_run_id is not None
    classify_run_impl(config_v1, repository, first_run_id)
    validate_and_prepublish_run_impl(config_v1, repository, first_run_id)

    config_v2 = _config(tmp_path, config_revision=2)
    second_run_id = archive_run_impl(config_v2, repository)
    assert second_run_id is not None
    classify_run_impl(config_v2, repository, second_run_id)
    validate_and_prepublish_run_impl(config_v2, repository, second_run_id)

    published_count = publish_prepublished_run_impl(config_v1, repository, run_id=first_run_id)

    first_file = repository.get_run_files(first_run_id)[0]
    second_file = repository.get_run_files(second_run_id)[0]
    assert published_count == 0
    assert first_file.status == FileStatus.SUPERSEDED.value
    assert first_file.superseded_by_file_id == second_file.id
    assert repository.get_run(first_run_id).status == ProcessingRunStatus.COMPLETED_WITH_ERRORS.value
    assert second_file.status == FileStatus.PREPUBLISHED.value
    assert not (config_v1.directories.output_root / "invoice" / "invoice.xml").exists()

    assert publish_prepublished_run_impl(config_v2, repository, run_id=second_run_id) == 1
    assert repository.get_run(second_run_id).status == ProcessingRunStatus.PUBLISHED.value
    assert (config_v2.directories.output_root / "invoice" / "invoice.xml").exists()


def test_archive_skips_same_source_hash_on_rerun(tmp_path: Path) -> None:
    config = _config(tmp_path)
    source_dir = config.directories.source_dirs[0]
    source_dir.mkdir(parents=True)
    (source_dir / "invoice.xml").write_text("<Document><Type>INVOICE</Type></Document>", encoding="utf-8")
    repository = _repository(config)

    first_run_id = archive_run_impl(config, repository)
    second_run_id = archive_run_impl(config, repository)

    assert first_run_id is not None
    assert second_run_id is None


def test_archive_allows_same_source_hash_for_new_config_revision(tmp_path: Path) -> None:
    config_v1 = _config(tmp_path, config_revision=1)
    source_dir = config_v1.directories.source_dirs[0]
    source_dir.mkdir(parents=True)
    source_path = source_dir / "invoice.xml"
    source_path.write_text("<Document><Type>INVOICE</Type></Document>", encoding="utf-8")
    repository = _repository(config_v1)

    first_run_id = archive_run_impl(config_v1, repository)
    second_run_id = archive_run_impl(_config(tmp_path, config_revision=2), repository)

    assert first_run_id is not None
    assert second_run_id is not None
    first_file = repository.get_run_files(first_run_id)[0]
    second_file = repository.get_run_files(second_run_id)[0]
    assert first_file.config_revision == 1
    assert second_file.config_revision == 2


def test_repository_returns_latest_replace_file_for_logical_target(tmp_path: Path) -> None:
    config_v1 = _config(tmp_path, config_revision=1)
    source_dir = config_v1.directories.source_dirs[0]
    source_dir.mkdir(parents=True)
    source_path = source_dir / "invoice.xml"
    source_path.write_text("<Document><Type>INVOICE</Type></Document>", encoding="utf-8")
    repository = _repository(config_v1)

    first_run_id = archive_run_impl(config_v1, repository)
    assert first_run_id is not None
    classify_run_impl(config_v1, repository, first_run_id)
    validate_and_publish_run_impl(config_v1, repository, first_run_id)

    config_v2 = _config(tmp_path, config_revision=2)
    second_run_id = archive_run_impl(config_v2, repository)
    assert second_run_id is not None
    classify_run_impl(config_v2, repository, second_run_id)

    latest = repository.get_latest_replace_file(
        source_path=source_path,
        sha256=sha256_file(source_path),
        logical_target_path=config_v2.directories.output_root / "invoice" / "invoice.xml",
    )
    assert latest is not None
    assert latest.run_id == second_run_id
    assert latest.config_revision == 2


def test_archive_collects_all_incremental_files_in_one_run(tmp_path: Path) -> None:
    config = _config(tmp_path)
    source_dir = config.directories.source_dirs[0]
    source_dir.mkdir(parents=True)
    for index in range(5):
        (source_dir / f"invoice_{index}.xml").write_text(
            f"<Document><Type>INVOICE</Type><Index>{index}</Index></Document>",
            encoding="utf-8",
        )
    repository = _repository(config)

    first_run_id = archive_run_impl(config, repository)
    second_run_id = archive_run_impl(config, repository)

    assert first_run_id is not None
    assert second_run_id is None
    assert len(repository.get_run_files(first_run_id)) == 5


def test_archive_reuses_discovered_record_left_by_interrupted_copy(tmp_path: Path) -> None:
    config = _config(tmp_path)
    source_dir = config.directories.source_dirs[0]
    source_dir.mkdir(parents=True)
    source_path = source_dir / "invoice.xml"
    source_path.write_text("<Document><Type>INVOICE</Type></Document>", encoding="utf-8")
    repository = _repository(config)
    interrupted_run = repository.create_run(config.snapshot())

    repository.reserve_discovered_file(
        run_id=interrupted_run.id,
        source_path=source_path,
        archive_path=config.directories.archive_dir / interrupted_run.id / source_path.name,
        sha256=sha256_file(source_path),
        size_bytes=source_path.stat().st_size,
        config_revision=config.runtime.config_revision,
    )

    recovered_run_id = archive_run_impl(config, repository)

    assert recovered_run_id is not None
    files = repository.get_run_files(recovered_run_id)
    assert len(files) == 1
    assert files[0].status == FileStatus.ARCHIVED_B.value


def test_classification_persists_versioned_paths_for_supplement_mode(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        config_revision=2,
        rules=[
            {
                "rule_id": "invoice",
                "target_path_template": "invoice/{source_name}",
                "publish_mode": "supplement",
                "conditions": {"all": [{"xpath": "/Document/Type", "op": "eq", "value": "INVOICE"}]},
            }
        ],
    )
    source_dir = config.directories.source_dirs[0]
    source_dir.mkdir(parents=True)
    (source_dir / "invoice.xml").write_text("<Document><Type>INVOICE</Type></Document>", encoding="utf-8")
    repository = _repository(config)

    run_id = archive_run_impl(config, repository)
    assert run_id is not None
    classify_run_impl(config, repository, run_id)

    record = repository.get_run_files(run_id)[0]
    assert record.publish_mode == "supplement"
    assert record.logical_target_path == str(config.directories.output_root / "invoice" / "invoice.xml")
    assert record.final_target_path == str(config.directories.output_root / "invoice" / "invoice__v2.xml")
    assert record.prepublish_path == str(config.directories.prepublish_dir / "invoice" / "invoice__v2.xml")
    assert record.target_path == record.final_target_path


def test_supplement_mode_publishes_multiple_revisions_side_by_side(tmp_path: Path) -> None:
    rules = [
        {
            "rule_id": "invoice",
            "target_path_template": "invoice/{source_name}",
            "publish_mode": "supplement",
            "conditions": {"all": [{"xpath": "/Document/Type", "op": "eq", "value": "INVOICE"}]},
        }
    ]
    config_v1 = _config(tmp_path, config_revision=1, rules=rules)
    source_dir = config_v1.directories.source_dirs[0]
    source_dir.mkdir(parents=True)
    source_path = source_dir / "invoice.xml"
    source_path.write_text("<Document><Type>INVOICE</Type></Document>", encoding="utf-8")
    repository = _repository(config_v1)

    first_run_id = archive_run_impl(config_v1, repository)
    assert first_run_id is not None
    classify_run_impl(config_v1, repository, first_run_id)
    validate_and_publish_run_impl(config_v1, repository, first_run_id)

    config_v2 = _config(tmp_path, config_revision=2, rules=rules)
    second_run_id = archive_run_impl(config_v2, repository)
    assert second_run_id is not None
    classify_run_impl(config_v2, repository, second_run_id)
    validate_and_publish_run_impl(config_v2, repository, second_run_id)

    first_file = repository.get_run_files(first_run_id)[0]
    second_file = repository.get_run_files(second_run_id)[0]
    first_target = config_v1.directories.output_root / "invoice" / "invoice__v1.xml"
    second_target = config_v2.directories.output_root / "invoice" / "invoice__v2.xml"

    assert first_file.status == FileStatus.PUBLISHED.value
    assert second_file.status == FileStatus.PUBLISHED.value
    assert repository.get_run(first_run_id).status == ProcessingRunStatus.PUBLISHED.value
    assert repository.get_run(second_run_id).status == ProcessingRunStatus.PUBLISHED.value
    assert first_target.exists()
    assert second_target.exists()
    assert sha256_file(first_target) == sha256_file(second_target) == sha256_file(source_path)


def test_init_db_migrates_legacy_files_table(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "legacy.sqlite3"
    engine = build_engine(sqlite_path)
    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            CREATE TABLE processing_runs (
                id VARCHAR(64) NOT NULL PRIMARY KEY,
                status VARCHAR(32),
                config_snapshot JSON,
                error_message TEXT,
                created_at DATETIME,
                updated_at DATETIME
            )
            """
        )
        connection.exec_driver_sql(
            """
            CREATE TABLE files (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                run_id VARCHAR(64),
                source_path TEXT,
                archive_path TEXT,
                staging_path TEXT,
                target_path TEXT,
                quarantine_path TEXT,
                sha256 VARCHAR(64),
                size_bytes INTEGER,
                status VARCHAR(32),
                error_message TEXT,
                created_at DATETIME,
                updated_at DATETIME,
                CONSTRAINT uq_file_source_hash UNIQUE (source_path, sha256)
            )
            """
        )
        connection.exec_driver_sql(
            """
            INSERT INTO processing_runs (id, status, config_snapshot)
            VALUES ('legacy-run', 'ARCHIVED', '{}')
            """
        )
        connection.exec_driver_sql(
            """
            INSERT INTO files (
                run_id,
                source_path,
                archive_path,
                target_path,
                sha256,
                size_bytes,
                status
            )
            VALUES (
                'legacy-run',
                '/tmp/source.xml',
                '/tmp/archive/source.xml',
                '/tmp/output/source.xml',
                'abc123',
                10,
                'PUBLISHED'
            )
            """
        )

    init_db(engine)
    repository = IntegrationRepository(build_session_factory(engine))

    record = repository.get_run_files("legacy-run")[0]
    assert record.config_revision == 1
    assert record.publish_mode == "replace"
    assert record.logical_target_path == "/tmp/output/source.xml"
    assert record.final_target_path == "/tmp/output/source.xml"


def test_classify_resolves_prepublish_path_with_relative_output_root(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "A").mkdir()
    (tmp_path / "A" / "invoice.xml").write_text(
        "<Document><Type>INVOICE</Type></Document>",
        encoding="utf-8",
    )
    config = IntegrationConfig.model_validate(
        {
            "directories": {
                "source_dirs": ["A"],
                "archive_dir": "B",
                "staging_dir": "C_staging",
                "output_root": "C",
                "prepublish_dir": "C_prepublish",
                "quarantine_dir": "quarantine",
                "sqlite_path": "integration.sqlite3",
                "lock_file": "integration.lock",
            },
            "runtime": {"file_stability_seconds": 0},
            "rules": [
                {
                    "rule_id": "invoice",
                    "target_path_template": "invoice/{source_name}",
                    "conditions": {"all": [{"xpath": "/Document/Type", "op": "eq", "value": "INVOICE"}]},
                }
            ],
        }
    )
    repository = _repository(config)

    run_id = archive_run_impl(config, repository)
    assert run_id is not None
    classify_run_impl(config, repository, run_id)

    record = repository.get_run_files(run_id)[0]
    assert record.prepublish_path == str((tmp_path / "C_prepublish" / "invoice" / "invoice.xml").resolve())


def _repository(config: IntegrationConfig) -> IntegrationRepository:
    engine = build_engine(config.directories.sqlite_path)
    init_db(engine)
    return IntegrationRepository(build_session_factory(engine))


def _config(tmp_path: Path, rules: list[dict] | None = None, **runtime_overrides) -> IntegrationConfig:
    runtime = {"file_stability_seconds": 0}
    runtime.update(runtime_overrides)
    return IntegrationConfig.model_validate(
        {
            "directories": {
                "source_dirs": [tmp_path / "A"],
                "archive_dir": tmp_path / "B",
                "staging_dir": tmp_path / "C_staging",
                "output_root": tmp_path / "C",
                "prepublish_dir": tmp_path / "C_prepublish",
                "quarantine_dir": tmp_path / "quarantine",
                "sqlite_path": tmp_path / "integration.sqlite3",
                "lock_file": tmp_path / "integration.lock",
            },
            "runtime": runtime,
            "rules": rules
            or [
                {
                    "rule_id": "invoice",
                    "target_path_template": "invoice/{source_name}",
                    "conditions": {"all": [{"xpath": "/Document/Type", "op": "eq", "value": "INVOICE"}]},
                }
            ],
        }
    )
