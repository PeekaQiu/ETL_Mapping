from __future__ import annotations

from pathlib import Path

from data_integration.config.schema import IntegrationConfig
from data_integration.storage.db import open_repository
from data_integration.storage.models import FileStatus
from data_integration.storage.repository import IntegrationRepository


class ConfigGuardrailError(ValueError):
    """Raised when a configuration change violates runtime safety rules."""


_BACKLOG_STATUSES = (FileStatus.PREPUBLISHED, FileStatus.CLASSIFIED_STAGING)


def validate_config_for_runtime(
    config: IntegrationConfig,
    *,
    bootstrap_config: IntegrationConfig | None = None,
    repository: IntegrationRepository | None = None,
) -> None:
    """Validate configuration against bootstrap defaults and the latest persisted run."""
    if bootstrap_config is not None:
        _validate_revision_policy(bootstrap_config, config, context="bootstrap")
        _validate_storage_path_change(bootstrap_config, config)

    if repository is not None:
        snapshot = repository.get_latest_run_config_snapshot()
        if snapshot is not None:
            previous = IntegrationConfig.model_validate(snapshot)
            _validate_revision_policy(previous, config, context="last run")
            if _storage_paths_changed(previous.directories, config.directories):
                previous_repository = _repository_for_config(previous)
                if _has_processing_backlog(previous_repository):
                    raise ConfigGuardrailError(
                        "cannot change sqlite_path, output_root, or prepublish_dir while "
                        "PREPUBLISHED or CLASSIFIED_STAGING backlog exists in the previous database"
                    )


def validate_config_patch(
    current: IntegrationConfig,
    updated: IntegrationConfig,
    *,
    repository: IntegrationRepository | None = None,
) -> None:
    """Validate a Variable patch before it is persisted."""
    _validate_revision_policy(current, updated, context="current variable")
    if _storage_paths_changed(current.directories, updated.directories):
        repo = repository or _repository_for_config(current)
        if _has_processing_backlog(repo):
            raise ConfigGuardrailError(
                "cannot change sqlite_path, output_root, or prepublish_dir while "
                "PREPUBLISHED or CLASSIFIED_STAGING backlog exists"
            )


def _validate_revision_policy(
    previous: IntegrationConfig,
    current: IntegrationConfig,
    *,
    context: str,
) -> None:
    if not _routing_policy_changed(previous, current):
        return
    if current.runtime.config_revision > previous.runtime.config_revision:
        return
    raise ConfigGuardrailError(
        f"rules, source_dirs, or publish routing changed relative to {context} "
        f"without increasing runtime.config_revision "
        f"(previous={previous.runtime.config_revision}, current={current.runtime.config_revision})"
    )


def _validate_storage_path_change(
    previous: IntegrationConfig,
    current: IntegrationConfig,
) -> None:
    if not _storage_paths_changed(previous.directories, current.directories):
        return
    previous_repository = _repository_for_config(previous)
    if previous_repository is None:
        return
    if _has_processing_backlog(previous_repository):
        raise ConfigGuardrailError(
            "cannot change sqlite_path, output_root, or prepublish_dir while "
            "PREPUBLISHED or CLASSIFIED_STAGING backlog exists in the previous database"
        )


def _routing_policy_changed(previous: IntegrationConfig, current: IntegrationConfig) -> bool:
    if [str(path) for path in previous.directories.source_dirs] != [
        str(path) for path in current.directories.source_dirs
    ]:
        return True
    previous_rules = [_rule_signature(rule) for rule in previous.rules]
    current_rules = [_rule_signature(rule) for rule in current.rules]
    return previous_rules != current_rules


def _rule_signature(rule: object) -> tuple:
    from data_integration.config.schema import RuleConfig

    if not isinstance(rule, RuleConfig):
        raise TypeError(f"expected RuleConfig, got {type(rule)!r}")
    return (
        rule.rule_id,
        rule.target_path_template,
        rule.publish_mode,
        rule.enabled,
        rule.model_dump(mode="json")["conditions"],
    )


def _storage_paths_changed(previous_dirs: object, current_dirs: object) -> bool:
    for field in ("sqlite_path", "output_root", "prepublish_dir"):
        if str(getattr(previous_dirs, field)) != str(getattr(current_dirs, field)):
            return True
    return False


def _has_processing_backlog(repository: IntegrationRepository) -> bool:
    for status in _BACKLOG_STATUSES:
        if repository.count_files_by_status(status) > 0:
            return True
    return False


def _repository_for_config(config: IntegrationConfig) -> IntegrationRepository | None:
    sqlite_path = config.directories.sqlite_path
    if not sqlite_path.exists():
        return None
    return open_repository(sqlite_path)
