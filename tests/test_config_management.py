import json
from pathlib import Path

from data_integration.config import loader
from data_integration.config.loader import (
    initialize_bulk_sources_from_files,
    initialize_config_variable_from_file,
    update_config_variable,
    validate_config_payload,
)
from tests.helpers import FakeVariableStore, config_payload


def test_initialize_load_and_patch_prefect_variable(tmp_path: Path, monkeypatch) -> None:
    store = FakeVariableStore()
    monkeypatch.setattr(loader, "Variable", store.variable_class())
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config_payload(tmp_path)), encoding="utf-8")

    initialized = initialize_config_variable_from_file(config_path, variable_name="etl_config")
    loaded = validate_config_payload(store.values["etl_config"])
    updated = update_config_variable(
        {"runtime": {"archive_retention_days": 60}},
        variable_name="etl_config",
    )

    assert initialized.runtime.archive_retention_days == 30
    assert loaded.rules[0].rule_id == "invoice"
    assert updated.runtime.archive_retention_days == 60
    assert store.values["etl_config"]["runtime"]["archive_retention_days"] == 60


def test_initialize_bulk_sources_from_files_requires_explicit_overwrite(tmp_path: Path, monkeypatch) -> None:
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
    store.values["source_config"] = {"existing": True}

    try:
        initialize_bulk_sources_from_files(
            controller_config_path=controller_path,
            source_config_files={"source_config": source_path},
        )
    except ValueError as exc:
        assert str(exc) == "variable exists"
    else:
        raise AssertionError("existing variables should require overwrite=True")

    initialize_bulk_sources_from_files(
        controller_config_path=controller_path,
        source_config_files={"source_config": source_path},
        overwrite=True,
    )

    assert store.values["source_config"]["rules"][0]["rule_id"] == "invoice"
    assert store.values["bulk_sources_controller"]["flow_configs"][0]["name"] == "source-1"

