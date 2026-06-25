import json
from pathlib import Path

import pytest

from data_integration.config import loader
from data_integration.flows.controller import (
    SourceConfigRef,
    load_controller_variable,
    enabled_sources,
    load_and_validate_sources,
)
from data_integration.flows.main import load_source_config
from tests.helpers import FakeVariableStore, config_payload


def test_load_controller_variable_bootstraps_from_config(tmp_path: Path, monkeypatch) -> None:
    store = FakeVariableStore()
    monkeypatch.setattr(loader, "Variable", store.variable_class())
    controller_path = tmp_path / "controller.json"
    controller_path.write_text(
        json.dumps(
            {
                "flow_configs": [
                    {"name": "source-1", "config_variable": "source_config", "enabled": True}
                ],
                "loop": {"delay_seconds": 15, "cycles": 2},
            }
        ),
        encoding="utf-8",
    )

    controller = load_controller_variable(
        variable_name="bulk_sources_controller",
        bootstrap_path=controller_path,
        project_root_path=tmp_path,
    )

    assert controller.loop.delay_seconds == 15
    assert store.values["bulk_sources_controller"]["loop"]["delay_seconds"] == 15


def test_integration_cycle_reads_updated_source_variable(tmp_path: Path, monkeypatch) -> None:
    store = FakeVariableStore()
    monkeypatch.setattr(loader, "Variable", store.variable_class())
    source_path = tmp_path / "source.json"
    controller_path = tmp_path / "controller.json"
    source_path.write_text(json.dumps(config_payload(tmp_path)), encoding="utf-8")
    controller_path.write_text(
        json.dumps(
            {
                "flow_configs": [
                    {"name": "source-1", "config_variable": "source_config", "enabled": True}
                ],
                "loop": {"delay_seconds": 0},
            }
        ),
        encoding="utf-8",
    )
    loader.bootstrap_bulk_config_variable("bulk_sources_controller", controller_path)
    loader.bootstrap_config_variable("source_config", source_path)

    store.values["source_config"]["runtime"]["config_revision"] = 7
    config = load_source_config(
        config_variable="source_config",
        bootstrap_path=source_path,
    )

    assert config.runtime.config_revision == 7


def test_load_and_validate_sources_bootstraps_missing_source_variable(tmp_path: Path, monkeypatch) -> None:
    store = FakeVariableStore()
    monkeypatch.setattr(loader, "Variable", store.variable_class())
    source_path = tmp_path / "source.json"
    source_path.write_text(json.dumps(config_payload(tmp_path)), encoding="utf-8")
    (tmp_path / "A").mkdir(parents=True)

    refs = [
        SourceConfigRef(
            source_name="source-1",
            config_variable="source_config",
            bootstrap_path=source_path,
        )
    ]

    validated = load_and_validate_sources(refs)

    assert validated["source-1"].rules[0].rule_id == "invoice"
    assert store.values["source_config"]["rules"][0]["rule_id"] == "invoice"


def test_enabled_sources_requires_config_variable(tmp_path: Path) -> None:
    controller = loader.validate_bulk_config_payload(
        {
            "flow_configs": [{"name": "bad", "config_path": "config/foo.json", "enabled": True}],
            "loop": {"delay_seconds": 0},
        }
    )

    with pytest.raises(ValueError, match="requires config_variable"):
        enabled_sources(controller, project_root_path=tmp_path)
