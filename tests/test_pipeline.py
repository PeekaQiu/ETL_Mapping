from pathlib import Path

from data_integration.config.schema import IntegrationConfig
from data_integration.storage.db import build_engine, build_session_factory, init_db
from data_integration.storage.models import FileStatus, ProcessingRunStatus
from data_integration.storage.repository import IntegrationRepository
from data_integration.files.safety import sha256_file
from data_integration.tasks.archive import archive_run_impl
from data_integration.tasks.classify import classify_run_impl
from data_integration.tasks.validate import validate_and_publish_run_impl


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
    assert run.status == ProcessingRunStatus.PUBLISHED.value
    assert files[0].status == FileStatus.PUBLISHED.value
    assert (config.directories.output_root / "invoice" / "invoice.xml").exists()
    assert files[0].staging_path is not None
    assert not Path(files[0].staging_path).exists()


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
    )

    recovered_run_id = archive_run_impl(config, repository)

    assert recovered_run_id is not None
    files = repository.get_run_files(recovered_run_id)
    assert len(files) == 1
    assert files[0].status == FileStatus.ARCHIVED_B.value


def _repository(config: IntegrationConfig) -> IntegrationRepository:
    engine = build_engine(config.directories.sqlite_path)
    init_db(engine)
    return IntegrationRepository(build_session_factory(engine))


def _config(tmp_path: Path, **runtime_overrides) -> IntegrationConfig:
    runtime = {"file_stability_seconds": 0}
    runtime.update(runtime_overrides)
    return IntegrationConfig.model_validate(
        {
            "directories": {
                "source_dirs": [tmp_path / "A"],
                "archive_dir": tmp_path / "B",
                "staging_dir": tmp_path / "C_staging",
                "output_root": tmp_path / "C",
                "quarantine_dir": tmp_path / "quarantine",
                "sqlite_path": tmp_path / "integration.sqlite3",
                "lock_file": tmp_path / "integration.lock",
            },
            "runtime": runtime,
            "rules": [
                {
                    "rule_id": "invoice",
                    "target_path_template": "invoice/{source_name}",
                    "conditions": {"all": [{"xpath": "/Document/Type", "op": "eq", "value": "INVOICE"}]},
                }
            ],
        }
    )
