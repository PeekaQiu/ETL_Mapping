import pytest
from pydantic import ValidationError

from data_integration.config.schema import BulkIntegrationConfig
from data_integration.flows import bulk


def test_bulk_controller_runs_enabled_sources_in_order(monkeypatch) -> None:
    controller = BulkIntegrationConfig.model_validate(
        {
            "flow_configs": [
                {"name": "source-1", "config_variable": "source_1_config", "enabled": True},
                {"name": "source-2", "config_variable": "source_2_config", "enabled": True},
                {"name": "source-3", "config_variable": "source_3_config", "enabled": False},
            ],
            "loop": {"delay_seconds": 0, "cycles": 1, "stop_on_failure": False},
        }
    )
    calls: list[str] = []

    def fake_run_single_source(flow_config):
        calls.append(flow_config.name)
        return f"run-{flow_config.name}"

    monkeypatch.setattr(bulk, "_run_single_source", fake_run_single_source)

    result = bulk.run_bulk_sources_once_impl(controller)

    assert calls == ["source-1", "source-2"]
    assert result == {"source-1": "run-source-1", "source-2": "run-source-2"}


def test_bulk_controller_continues_after_source_failure_by_default(monkeypatch) -> None:
    controller = BulkIntegrationConfig.model_validate(
        {
            "flow_configs": [
                {"name": "source-1", "config_variable": "source_1_config"},
                {"name": "source-2", "config_variable": "source_2_config"},
            ],
            "loop": {"delay_seconds": 0, "cycles": 1, "stop_on_failure": False},
        }
    )
    calls: list[str] = []

    def fake_run_single_source(flow_config):
        calls.append(flow_config.name)
        if flow_config.name == "source-1":
            raise RuntimeError("boom")
        return "run-source-2"

    monkeypatch.setattr(bulk, "_run_single_source", fake_run_single_source)

    result = bulk.run_bulk_sources_once_impl(controller)

    assert calls == ["source-1", "source-2"]
    assert result == {"source-1": None, "source-2": "run-source-2"}


def test_bulk_controller_can_stop_on_source_failure(monkeypatch) -> None:
    controller = BulkIntegrationConfig.model_validate(
        {
            "flow_configs": [
                {"name": "source-1", "config_variable": "source_1_config"},
                {"name": "source-2", "config_variable": "source_2_config"},
            ],
            "loop": {"delay_seconds": 0, "cycles": 1, "stop_on_failure": True},
        }
    )
    calls: list[str] = []

    def fake_run_single_source(flow_config):
        calls.append(flow_config.name)
        raise RuntimeError("boom")

    monkeypatch.setattr(bulk, "_run_single_source", fake_run_single_source)

    with pytest.raises(RuntimeError):
        bulk.run_bulk_sources_once_impl(controller)

    assert calls == ["source-1"]


def test_bulk_controller_rejects_ref_with_both_variable_and_path() -> None:
    with pytest.raises(ValidationError):
        BulkIntegrationConfig.model_validate(
            {
                "flow_configs": [
                    {
                        "name": "bad",
                        "config_variable": "source_config",
                        "config_path": "config/source.json",
                    }
                ]
            }
        )


def test_single_source_runs_inside_controller_flow_context(monkeypatch) -> None:
    calls: list[dict] = []

    class FakeSourceTask:
        def __call__(self, **kwargs):
            calls.append(kwargs)
            return "run-source-1"

    monkeypatch.setattr(bulk.integrate_source, "with_options", lambda **kwargs: FakeSourceTask())
    flow_config = BulkIntegrationConfig.model_validate(
        {"flow_configs": [{"name": "source-1", "config_variable": "source_1_config"}]}
    ).flow_configs[0]

    result = bulk._run_single_source(flow_config)

    assert result == "run-source-1"
    assert calls == [
        {
            "config_variable": "source_1_config",
            "config_path": None,
            "source_name": "source-1",
        }
    ]


def test_external_bulk_loop_triggers_one_flow_run_per_cycle(monkeypatch) -> None:
    controller = BulkIntegrationConfig.model_validate(
        {
            "flow_configs": [{"name": "source-1", "config_variable": "source_1_config"}],
            "loop": {"delay_seconds": 10, "cycles": 2, "stop_on_failure": False},
        }
    )
    calls: list[BulkIntegrationConfig] = []
    sleeps: list[int] = []

    monkeypatch.setattr(bulk, "load_bulk_config", lambda **kwargs: controller)
    monkeypatch.setattr(bulk, "run_bulk_sources_once", lambda **kwargs: calls.append(kwargs["controller"]))
    monkeypatch.setattr(bulk.time, "sleep", lambda seconds: sleeps.append(seconds))

    bulk.run_bulk_sources_loop()

    assert calls == [controller, controller]
    assert sleeps == [10]
