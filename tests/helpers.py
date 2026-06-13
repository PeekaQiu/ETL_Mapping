from __future__ import annotations

from pathlib import Path
from typing import Any

from data_integration.config.schema import IntegrationConfig
from data_integration.storage.db import build_engine, build_session_factory, init_db
from data_integration.storage.repository import IntegrationRepository


class FakeVariableStore:
    def __init__(self) -> None:
        self.values: dict[str, Any] = {}

    def variable_class(self) -> type:
        values = self.values

        class FakeVariable:
            @staticmethod
            def get(name: str, default: Any = None) -> Any:
                return values.get(name, default)

            @staticmethod
            def set(name: str, value: Any, tags: list[str] | None = None, overwrite: bool = False) -> None:
                if name in values and not overwrite:
                    raise ValueError("variable exists")
                values[name] = value

        return FakeVariable


def config_payload(tmp_path: Path, rules: list[dict[str, Any]] | None = None, **runtime_overrides: Any) -> dict[str, Any]:
    runtime = {"file_stability_seconds": 0, "archive_retention_days": 30}
    runtime.update(runtime_overrides)
    return {
        "directories": {
            "source_dirs": [str(tmp_path / "A")],
            "archive_dir": str(tmp_path / "B"),
            "staging_dir": str(tmp_path / "C_staging"),
            "output_root": str(tmp_path / "C"),
            "quarantine_dir": str(tmp_path / "quarantine"),
            "sqlite_path": str(tmp_path / "integration.sqlite3"),
            "lock_file": str(tmp_path / "integration.lock"),
        },
        "runtime": runtime,
        "rules": rules or [invoice_rule()],
    }


def make_config(tmp_path: Path, rules: list[dict[str, Any]] | None = None, **runtime_overrides: Any) -> IntegrationConfig:
    return IntegrationConfig.model_validate(config_payload(tmp_path, rules, **runtime_overrides))


def make_repository(config: IntegrationConfig) -> IntegrationRepository:
    engine = build_engine(config.directories.sqlite_path)
    init_db(engine)
    return IntegrationRepository(build_session_factory(engine))


def invoice_rule(target_path_template: str = "invoice/{source_name}") -> dict[str, Any]:
    return {
        "rule_id": "invoice",
        "target_path_template": target_path_template,
        "conditions": {"all": [{"xpath": "/Document/Type", "op": "eq", "value": "INVOICE"}]},
    }


def write_xml(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
