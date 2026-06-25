from __future__ import annotations

import json
from pathlib import Path

import pytest

from data_integration.config import loader
from data_integration.config.guardrails import ConfigGuardrailError, validate_config_for_runtime
from data_integration.config.loader import update_config_variable
from data_integration.flows.controller import (
    SourceConfigRef,
    load_and_validate_sources,
)
from data_integration.flows.main import load_source_config
from data_integration.storage.models import FileStatus, ProcessingRunStatus
from data_integration.tasks.archive import archive_run_impl, discover_archive_candidates
from data_integration.tasks.classify import classify_run_impl
from data_integration.tasks.validate import prepublish_run_impl
from tests.helpers import FakeVariableStore, config_payload, make_config, make_repository, publish_run, write_xml
from tests.test_pipeline import _config, _repository


def test_same_revision_replacement_rejected_after_publish(tmp_path: Path) -> None:
    config = _config(tmp_path, config_revision=1)
    source_dir = config.directories.source_dirs[0]
    source_dir.mkdir(parents=True)
    source_path = source_dir / "invoice.xml"
    source_path.write_text("<Document><Type>INVOICE</Type><Rev>1</Rev></Document>", encoding="utf-8")
    repository = _repository(config)

    run_id = archive_run_impl(config, repository)
    assert run_id is not None
    classify_run_impl(config, repository, run_id)
    publish_run(config, repository, run_id)
    assert repository.get_run_files(run_id)[0].status == FileStatus.PUBLISHED.value

    source_path.write_text("<Document><Type>INVOICE</Type><Rev>2</Rev></Document>", encoding="utf-8")
    rejected_run_id = archive_run_impl(config, repository)

    assert rejected_run_id is not None
    rejected_files = repository.get_run_files(rejected_run_id)
    assert len(rejected_files) == 1
    assert rejected_files[0].status == FileStatus.QUARANTINED.value
    assert "without config_revision bump" in (rejected_files[0].error_message or "")
    assert repository.get_run(rejected_run_id).status == ProcessingRunStatus.COMPLETED_WITH_ERRORS.value
    assert source_path.exists()
    assert "<Rev>2</Rev>" in source_path.read_text(encoding="utf-8")


def test_same_revision_replacement_rejected_while_prepublished(tmp_path: Path) -> None:
    config = _config(tmp_path, config_revision=1)
    source_dir = config.directories.source_dirs[0]
    source_dir.mkdir(parents=True)
    source_path = source_dir / "invoice.xml"
    source_path.write_text("<Document><Type>INVOICE</Type><Rev>1</Rev></Document>", encoding="utf-8")
    repository = _repository(config)

    run_id = archive_run_impl(config, repository)
    assert run_id is not None
    classify_run_impl(config, repository, run_id)
    prepublish_run_impl(config, repository, run_id)
    assert repository.get_run_files(run_id)[0].status == FileStatus.PREPUBLISHED.value

    source_path.write_text("<Document><Type>INVOICE</Type><Rev>2</Rev></Document>", encoding="utf-8")
    rejected_run_id = archive_run_impl(config, repository)

    assert rejected_run_id is not None
    rejected_files = repository.get_run_files(rejected_run_id)
    assert len(rejected_files) == 1
    assert rejected_files[0].status == FileStatus.QUARANTINED.value
    assert not (config.directories.output_root / "invoice" / "invoice.xml").exists()


def test_same_revision_replacement_not_retried_every_scan(tmp_path: Path) -> None:
    config = _config(tmp_path, config_revision=1)
    source_dir = config.directories.source_dirs[0]
    source_dir.mkdir(parents=True)
    source_path = source_dir / "invoice.xml"
    source_path.write_text("<Document><Type>INVOICE</Type><Rev>1</Rev></Document>", encoding="utf-8")
    repository = _repository(config)

    first_run_id = archive_run_impl(config, repository)
    assert first_run_id is not None
    classify_run_impl(config, repository, first_run_id)
    publish_run(config, repository, first_run_id)

    source_path.write_text("<Document><Type>INVOICE</Type><Rev>2</Rev></Document>", encoding="utf-8")
    assert archive_run_impl(config, repository) is not None
    assert archive_run_impl(config, repository) is None

    candidates, rejections, stats = discover_archive_candidates(config, repository)
    assert candidates == []
    assert rejections == []
    assert stats["skipped_duplicate"] == 1


def test_revision_bump_allows_same_path_replacement(tmp_path: Path) -> None:
    config_v1 = _config(tmp_path, config_revision=1)
    source_dir = config_v1.directories.source_dirs[0]
    source_dir.mkdir(parents=True)
    source_path = source_dir / "invoice.xml"
    source_path.write_text("<Document><Type>INVOICE</Type><Rev>1</Rev></Document>", encoding="utf-8")
    repository = _repository(config_v1)

    first_run_id = archive_run_impl(config_v1, repository)
    assert first_run_id is not None
    classify_run_impl(config_v1, repository, first_run_id)
    publish_run(config_v1, repository, first_run_id)

    config_v2 = _config(tmp_path, config_revision=2)
    source_path.write_text("<Document><Type>INVOICE</Type><Rev>2</Rev></Document>", encoding="utf-8")
    second_run_id = archive_run_impl(config_v2, repository)

    assert second_run_id is not None
    second_file = repository.get_run_files(second_run_id)[0]
    assert second_file.status == FileStatus.ARCHIVED.value
    assert second_file.config_revision == 2


def test_archive_leaves_readonly_source_in_place(tmp_path: Path) -> None:
    config = _config(tmp_path)
    source_dir = config.directories.source_dirs[0]
    source_dir.mkdir(parents=True)
    source_path = source_dir / "invoice.xml"
    body = "<Document><Type>INVOICE</Type></Document>"
    source_path.write_text(body, encoding="utf-8")
    repository = _repository(config)

    run_id = archive_run_impl(config, repository)
    assert run_id is not None
    assert source_path.exists()
    assert source_path.read_text(encoding="utf-8") == body


def test_update_config_variable_rejects_rules_change_without_revision_bump(
    tmp_path: Path,
    monkeypatch,
) -> None:
    store = FakeVariableStore()
    monkeypatch.setattr(loader, "Variable", store.variable_class())
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config_payload(tmp_path)), encoding="utf-8")
    loader.bootstrap_config_variable("etl_config", config_path)

    with pytest.raises(ConfigGuardrailError, match="config_revision"):
        update_config_variable(
            {
                "rules": [
                    {
                        "rule_id": "invoice",
                        "target_path_template": "invoice/new_{source_name}",
                        "conditions": {"all": [{"xpath": "/Document/Type", "op": "eq", "value": "INVOICE"}]},
                    }
                ]
            },
            variable_name="etl_config",
        )


def test_load_and_validate_sources_rejects_storage_change_with_backlog(tmp_path: Path, monkeypatch) -> None:
    store = FakeVariableStore()
    monkeypatch.setattr(loader, "Variable", store.variable_class())
    source_path = tmp_path / "source.json"
    source_path.write_text(json.dumps(config_payload(tmp_path)), encoding="utf-8")
    (tmp_path / "A").mkdir(parents=True)
    loader.bootstrap_config_variable("source_config", source_path)

    config = make_config(tmp_path)
    write_xml(config.directories.source_dirs[0] / "invoice.xml", "<Document><Type>INVOICE</Type></Document>")
    repository = make_repository(config)
    run_id = archive_run_impl(config, repository)
    assert run_id is not None
    classify_run_impl(config, repository, run_id)
    prepublish_run_impl(config, repository, run_id)
    assert repository.count_files_by_status(FileStatus.PREPUBLISHED) == 1

    patched = config_payload(tmp_path, config_revision=2)
    patched["directories"]["sqlite_path"] = str(tmp_path / "other.sqlite3")
    store.values["source_config"] = patched

    refs = [
        SourceConfigRef(
            source_name="source-1",
            config_variable="source_config",
            bootstrap_path=source_path,
        )
    ]

    with pytest.raises(ConfigGuardrailError, match="backlog"):
        load_and_validate_sources(refs)


def test_validate_config_for_runtime_allows_revision_bump_with_rule_change(tmp_path: Path) -> None:
    previous = make_config(tmp_path)
    updated = make_config(
        tmp_path,
        rules=[
            {
                "rule_id": "invoice",
                "target_path_template": "invoice/new_{source_name}",
                "conditions": {"all": [{"xpath": "/Document/Type", "op": "eq", "value": "INVOICE"}]},
            }
        ],
        config_revision=2,
    )

    validate_config_for_runtime(updated, bootstrap_config=previous, repository=None)


def test_run_integration_freezes_config_across_steps(tmp_path: Path, monkeypatch) -> None:
    store = FakeVariableStore()
    monkeypatch.setattr(loader, "Variable", store.variable_class())
    config_path = tmp_path / "config.json"
    (tmp_path / "A").mkdir(parents=True)
    config_path.write_text(json.dumps(config_payload(tmp_path)), encoding="utf-8")
    loader.bootstrap_config_variable("source_config", config_path)
    write_xml(tmp_path / "A" / "invoice.xml", "<Document><Type>INVOICE</Type></Document>")

    revisions: list[int] = []
    import data_integration.flows.main as main_module
    import data_integration.tasks.archive as archive_module
    import data_integration.tasks.classify as classify_module
    import data_integration.tasks.validate as validate_module
    import data_integration.tasks.retention as retention_module

    def repository_for(config):
        from data_integration.storage.db import build_engine, build_session_factory, init_db
        from data_integration.storage.repository import IntegrationRepository

        engine = build_engine(config.directories.sqlite_path)
        init_db(engine)
        return IntegrationRepository(build_session_factory(engine))

    def archive_proxy(config):
        revisions.append(config.runtime.config_revision)
        if len(revisions) == 1:
            store.values["source_config"]["runtime"]["config_revision"] = 42
        return archive_module.archive_run_impl(config, repository_for(config))

    def classify_proxy(config, run_id):
        revisions.append(config.runtime.config_revision)
        return classify_module.classify_run_impl(config, repository_for(config), run_id)

    def prepublish_proxy(config, run_id):
        revisions.append(config.runtime.config_revision)
        return validate_module.prepublish_run_impl(config, repository_for(config), run_id)

    def retention_proxy(config):
        return retention_module.retention_run_impl(config, repository_for(config))

    monkeypatch.setattr(main_module, "archive_files", archive_proxy)
    monkeypatch.setattr(main_module, "classify_files", classify_proxy)
    monkeypatch.setattr(main_module, "prepublish_files", prepublish_proxy)
    monkeypatch.setattr(main_module, "apply_retention", retention_proxy)
    monkeypatch.setattr(main_module, "_source_task", lambda task_fn, *_args: task_fn)

    main_module.run_integration(
        config_variable="source_config",
        bootstrap_path=str(config_path),
        source_name="source-1",
    )

    assert revisions == [1, 1, 1]


def test_integration_cycle_reads_config_per_source_run_not_per_cycle(tmp_path: Path, monkeypatch) -> None:
    """Each integrate_source task reads Variable at task start; freeze applies within one source run only."""
    store = FakeVariableStore()
    monkeypatch.setattr(loader, "Variable", store.variable_class())

    source_1_path = tmp_path / "source_1.json"
    source_2_path = tmp_path / "source_2.json"
    controller_path = tmp_path / "controller.json"
    source_1_payload = config_payload(tmp_path / "source1")
    source_2_payload = config_payload(tmp_path / "source2")
    (tmp_path / "source1" / "A").mkdir(parents=True)
    (tmp_path / "source2" / "A").mkdir(parents=True)
    source_1_path.write_text(json.dumps(source_1_payload), encoding="utf-8")
    source_2_path.write_text(json.dumps(source_2_payload), encoding="utf-8")
    controller_path.write_text(
        json.dumps(
            {
                "flow_configs": [
                    {"name": "source-1", "config_variable": "source_1_config", "enabled": True},
                    {"name": "source-2", "config_variable": "source_2_config", "enabled": True},
                ],
                "loop": {"delay_seconds": 0},
            }
        ),
        encoding="utf-8",
    )
    loader.bootstrap_bulk_config_variable("bulk_sources_controller", controller_path)
    loader.bootstrap_config_variable("source_1_config", source_1_path)
    loader.bootstrap_config_variable("source_2_config", source_2_path)

    sources = [
        SourceConfigRef(
            source_name="source-1",
            config_variable="source_1_config",
            bootstrap_path=source_1_path,
        ),
        SourceConfigRef(
            source_name="source-2",
            config_variable="source_2_config",
            bootstrap_path=source_2_path,
        ),
    ]
    load_and_validate_sources(sources)

    seen_revisions: list[int] = []
    for source_ref in sources:
        config = load_source_config(
            config_variable=source_ref.config_variable,
            bootstrap_path=source_ref.bootstrap_path,
        )
        seen_revisions.append(config.runtime.config_revision)
        if source_ref.source_name == "source-1":
            store.values["source_2_config"]["runtime"]["config_revision"] = 99

    assert seen_revisions == [1, 99]


def test_publish_cycle_validation_fails_when_storage_paths_change_with_backlog(
    tmp_path: Path,
    monkeypatch,
) -> None:
    store = FakeVariableStore()
    monkeypatch.setattr(loader, "Variable", store.variable_class())

    source_path = tmp_path / "source.json"
    payload = config_payload(tmp_path)
    (tmp_path / "A").mkdir(parents=True)
    source_path.write_text(json.dumps(payload), encoding="utf-8")
    loader.bootstrap_config_variable("source_config", source_path)

    config = make_config(tmp_path)
    write_xml(config.directories.source_dirs[0] / "invoice.xml", "<Document><Type>INVOICE</Type></Document>")
    repository = make_repository(config)
    run_id = archive_run_impl(config, repository)
    assert run_id is not None
    classify_run_impl(config, repository, run_id)
    prepublish_run_impl(config, repository, run_id)

    patched = dict(payload)
    patched["directories"] = dict(payload["directories"])
    patched["directories"]["output_root"] = str(tmp_path / "D")
    patched["runtime"] = dict(payload["runtime"])
    patched["runtime"]["config_revision"] = 2
    store.values["source_config"] = patched

    refs = [
        SourceConfigRef(
            source_name="source-1",
            config_variable="source_config",
            bootstrap_path=source_path,
        )
    ]

    with pytest.raises(ConfigGuardrailError, match="backlog"):
        load_and_validate_sources(refs)
