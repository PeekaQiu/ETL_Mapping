from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import ValidationError
from prefect.variables import Variable

from data_integration.config.schema import BulkIntegrationConfig, IntegrationConfig


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


def load_config_var(
    config_path: str | Path,
    *,
    variable_name: str,
    overwrite: bool = False,
) -> IntegrationConfig:
    config = read_config_file(config_path)
    set_config_variable(config, variable_name=variable_name, overwrite=overwrite)
    return config


def load_bulk_config_var(
    config_path: str | Path,
    *,
    variable_name: str = DEFAULT_CONTROLLER_VAR,
    overwrite: bool = False,
) -> BulkIntegrationConfig:
    config = read_bulk_config_file(config_path)
    set_bulk_config_variable(config, variable_name=variable_name, overwrite=overwrite)
    return config


def load_source_config_vars(
    mappings: dict[str, str | Path],
    *,
    overwrite: bool = False,
) -> dict[str, IntegrationConfig]:
    initialized: dict[str, IntegrationConfig] = {}
    for variable_name, config_path in mappings.items():
        initialized[variable_name] = load_config_var(
            config_path,
            variable_name=variable_name,
            overwrite=overwrite,
        )
    return initialized


def load_bulk_sources(
    *,
    controller_config_path: str | Path = DEFAULT_CONTROLLER_PATH,
    controller_variable_name: str = DEFAULT_CONTROLLER_VAR,
    source_config_files: dict[str, str | Path] | None = None,
    overwrite: bool = False,
) -> tuple[dict[str, IntegrationConfig], BulkIntegrationConfig]:
    source_mappings = source_config_files or DEFAULT_SOURCE_CONFIGS
    initialized_sources = load_source_config_vars(source_mappings, overwrite=overwrite)
    controller = load_bulk_config_var(
        controller_config_path,
        variable_name=controller_variable_name,
        overwrite=overwrite,
    )
    return initialized_sources, controller


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
