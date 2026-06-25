from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import ValidationError
from prefect.variables import Variable

from data_integration.config.guardrails import validate_config_patch
from data_integration.config.schema import BulkIntegrationConfig, IntegrationConfig
from data_integration.storage.db import open_repository


DEFAULT_CONTROLLER_VAR = "bulk_sources_controller"
DEFAULT_CONTROLLER_PATH = "config/bulk_sources_controller.json"
DEFAULT_SOURCE_CONFIGS = {
    "bulk_source_1_config": "config/bulk_source_1_flow.json",
    "bulk_source_2_config": "config/bulk_source_2_flow.json",
    "bulk_source_3_config": "config/bulk_source_3_flow.json",
}
CONFIG_VARIABLE_TAGS = ["data-integration", "configuration"]


def validate_config_payload(payload: Any) -> IntegrationConfig:
    try:
        return IntegrationConfig.model_validate(payload)
    except ValidationError as exc:
        raise ValueError(f"Invalid data integration config: {exc}") from exc


def validate_bulk_config_payload(payload: Any) -> BulkIntegrationConfig:
    try:
        return BulkIntegrationConfig.model_validate(payload)
    except ValidationError as exc:
        raise ValueError(f"Invalid bulk data integration config: {exc}") from exc


def read_config_file(config_path: str | Path) -> IntegrationConfig:
    with Path(config_path).open("r", encoding="utf-8") as handle:
        return validate_config_payload(json.load(handle))


def read_bulk_config_file(config_path: str | Path) -> BulkIntegrationConfig:
    with Path(config_path).open("r", encoding="utf-8") as handle:
        return validate_bulk_config_payload(json.load(handle))


def set_config_variable(
    config: IntegrationConfig,
    *,
    variable_name: str,
    overwrite: bool = True,
) -> None:
    Variable.set(
        variable_name,
        config.snapshot(),
        tags=CONFIG_VARIABLE_TAGS,
        overwrite=overwrite,
    )


def set_bulk_config_variable(
    config: BulkIntegrationConfig,
    *,
    variable_name: str = DEFAULT_CONTROLLER_VAR,
    overwrite: bool = True,
) -> None:
    Variable.set(
        variable_name,
        config.snapshot(),
        tags=CONFIG_VARIABLE_TAGS,
        overwrite=overwrite,
    )


def bootstrap_config_variable(
    variable_name: str,
    bootstrap_path: str | Path,
    *,
    bootstrap_config: IntegrationConfig | None = None,
) -> IntegrationConfig:
    config = bootstrap_config or read_config_file(bootstrap_path)
    set_config_variable(config, variable_name=variable_name, overwrite=False)
    return config


def bootstrap_bulk_config_variable(
    variable_name: str,
    bootstrap_path: str | Path,
) -> BulkIntegrationConfig:
    config = read_bulk_config_file(bootstrap_path)
    set_bulk_config_variable(config, variable_name=variable_name, overwrite=False)
    return config


def ensure_config_variable(
    variable_name: str,
    bootstrap_path: str | Path,
    *,
    bootstrap_config: IntegrationConfig | None = None,
) -> IntegrationConfig:
    raw_config = Variable.get(variable_name, default=None)
    if raw_config is None:
        return bootstrap_config_variable(
            variable_name,
            bootstrap_path,
            bootstrap_config=bootstrap_config,
        )
    return validate_config_payload(_decode_variable(raw_config))


def ensure_bulk_config_variable(
    variable_name: str,
    bootstrap_path: str | Path,
) -> BulkIntegrationConfig:
    raw_config = Variable.get(variable_name, default=None)
    if raw_config is None:
        return bootstrap_bulk_config_variable(variable_name, bootstrap_path)
    return validate_bulk_config_payload(_decode_variable(raw_config))


def update_config_variable(
    patch: dict[str, Any],
    *,
    variable_name: str,
) -> IntegrationConfig:
    raw_config = Variable.get(variable_name, default=None)
    if raw_config is None:
        raise ValueError(f"Prefect Variable not found: {variable_name}")
    current = validate_config_payload(_decode_variable(raw_config))
    updated_payload = _deep_merge(current.snapshot(), patch)
    updated = validate_config_payload(updated_payload)
    repository = open_repository(current.directories.sqlite_path)
    validate_config_patch(current, updated, repository=repository)
    set_config_variable(updated, variable_name=variable_name, overwrite=True)
    return updated


def _decode_variable(raw_config: Any) -> Any:
    if isinstance(raw_config, str):
        return json.loads(raw_config)
    return raw_config


def _deep_merge(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged
