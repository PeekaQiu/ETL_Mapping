import json
from pathlib import Path

from data_integration.config import loader
from data_integration.config.loader import (
    bootstrap_config_variable,
    ensure_bulk_config_variable,
    ensure_config_variable,
    update_config_variable,
    validate_config_payload,
)
from data_integration.flows.main import load_source_config
from tests.helpers import FakeVariableStore, config_payload


def test_bootstrap_and_patch_prefect_variable(tmp_path: Path, monkeypatch) -> None:
    store = FakeVariableStore()
    monkeypatch.setattr(loader, "Variable", store.variable_class())
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config_payload(tmp_path)), encoding="utf-8")

    initialized = bootstrap_config_variable("etl_config", config_path)
    loaded = validate_config_payload(store.values["etl_config"])
    updated = update_config_variable(
        {"runtime": {"archive_retention_days": 60, "config_revision": 2}},
        variable_name="etl_config",
    )

    assert initialized.runtime.archive_retention_days == 30
    assert initialized.directories.prepublish_dir == tmp_path / "C_prepublish"
    assert initialized.runtime.config_revision == 1
    assert loaded.rules[0].rule_id == "invoice"
    assert loaded.rules[0].publish_mode == "replace"
    assert updated.runtime.archive_retention_days == 60
    assert updated.runtime.config_revision == 2
    assert store.values["etl_config"]["runtime"]["archive_retention_days"] == 60
    assert store.values["etl_config"]["runtime"]["config_revision"] == 2


def test_ensure_config_variable_bootstraps_when_missing(tmp_path: Path, monkeypatch) -> None:
    store = FakeVariableStore()
    monkeypatch.setattr(loader, "Variable", store.variable_class())
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config_payload(tmp_path)), encoding="utf-8")

    config = ensure_config_variable("etl_config", config_path)

    assert config.rules[0].rule_id == "invoice"
    assert store.values["etl_config"]["rules"][0]["rule_id"] == "invoice"


def test_ensure_config_variable_does_not_overwrite_existing(tmp_path: Path, monkeypatch) -> None:
    store = FakeVariableStore()
    monkeypatch.setattr(loader, "Variable", store.variable_class())
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config_payload(tmp_path)), encoding="utf-8")
    store.values["etl_config"] = config_payload(tmp_path, config_revision=99)

    config = ensure_config_variable("etl_config", config_path)

    assert config.runtime.config_revision == 99
    assert store.values["etl_config"]["runtime"]["config_revision"] == 99


def test_load_source_config_uses_updated_variable(tmp_path: Path, monkeypatch) -> None:
    store = FakeVariableStore()
    monkeypatch.setattr(loader, "Variable", store.variable_class())
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config_payload(tmp_path)), encoding="utf-8")
    bootstrap_config_variable("etl_config", config_path)

    store.values["etl_config"]["runtime"]["config_revision"] = 5
    config = load_source_config(
        config_variable="etl_config",
        bootstrap_path=config_path,
    )

    assert config.runtime.config_revision == 5


def test_ensure_bulk_config_variable_bootstraps_controller(tmp_path: Path, monkeypatch) -> None:
    store = FakeVariableStore()
    monkeypatch.setattr(loader, "Variable", store.variable_class())
    source_path = tmp_path / "source.json"
    controller_path = tmp_path / "controller.json"
    source_path.write_text(json.dumps(config_payload(tmp_path)), encoding="utf-8")
    controller_path.write_text(
        json.dumps(
            {
                "flow_configs": [{"name": "source-1", "config_variable": "source_config"}],
                "loop": {"delay_seconds": 0, "cycles": 1},
            }
        ),
        encoding="utf-8",
    )

    controller = ensure_bulk_config_variable("bulk_sources_controller", controller_path)

    assert controller.flow_configs[0].name == "source-1"
    assert store.values["bulk_sources_controller"]["flow_configs"][0]["name"] == "source-1"


def test_update_config_variable_allows_prepublish_observation_without_revision_bump(
    tmp_path: Path,
    monkeypatch,
) -> None:
    store = FakeVariableStore()
    monkeypatch.setattr(loader, "Variable", store.variable_class())
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config_payload(tmp_path)), encoding="utf-8")
    bootstrap_config_variable("etl_config", config_path)

    updated = update_config_variable(
        {"runtime": {"prepublish_observation_seconds": 120}},
        variable_name="etl_config",
    )

    assert updated.runtime.prepublish_observation_seconds == 120
    assert updated.runtime.config_revision == 1
    assert store.values["etl_config"]["runtime"]["prepublish_observation_seconds"] == 120
    assert store.values["etl_config"]["runtime"]["config_revision"] == 1
