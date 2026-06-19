from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from prefect import task

from data_integration.config.schema import IntegrationConfig
from data_integration.files.safety import (
    FileSafetyError,
    copy_verify,
    move_to_quarantine,
    render_safe_target,
    staging_destination,
)
from data_integration.logging_setup import log_fields, task_logger
from data_integration.prefect_ui import TASK_CLASSIFY, TASK_CLASSIFY_DESC
from data_integration.rules.engine import RuleEvaluationError, classify_xml_file
from data_integration.storage.db import build_engine, build_session_factory, init_db
from data_integration.storage.models import FileStatus, ProcessingRunStatus, RuleMatchStatus
from data_integration.storage.repository import (
    IntegrationRepository,
    TargetCollisionError,
)

_LOGGER = task_logger(__name__)


class ClassificationRunError(RuntimeError):
    pass


@task(name=TASK_CLASSIFY, description=TASK_CLASSIFY_DESC, retries=1, retry_delay_seconds=10)
def classify_files(config: IntegrationConfig, run_id: str) -> str:
    engine = build_engine(config.directories.sqlite_path)
    init_db(engine)
    repository = IntegrationRepository(build_session_factory(engine))
    return classify_run_impl(config, repository, run_id)


def classify_run_impl(
    config: IntegrationConfig,
    repository: IntegrationRepository,
    run_id: str,
) -> str:
    logger = _LOGGER
    files = repository.get_run_files(run_id, [FileStatus.ARCHIVED])
    if not files:
        repository.mark_run(run_id, ProcessingRunStatus.FAILED, "No archived files available for classification.")
        logger.error("Classification aborted: no archived files | %s", log_fields(run_id=run_id))
        raise ClassificationRunError(f"source run {run_id} has no archived files")

    logger.info(
        "Starting classification | %s",
        log_fields(run_id=run_id, file_count=len(files), rule_count=len(config.rules)),
    )

    failures: list[str] = []
    for record in files:
        archive_path = Path(record.archive_path)
        source_name = Path(record.source_path).name
        logger.debug(
            "Classifying file | %s",
            log_fields(run_id=run_id, file_id=record.id, source=source_name, archive=archive_path),
        )
        try:
            result = classify_xml_file(archive_path, config)
        except RuleEvaluationError as exc:
            repository.record_rule_match(
                file_id=record.id,
                status=RuleMatchStatus.RULE_ERROR,
                rule_id=None,
                details={"error": str(exc)},
            )
            failures.append(_quarantine_file(repository, config, record.id, archive_path, run_id, "rule_error", str(exc)))
            logger.warning(
                "Rule evaluation error; file quarantined | %s",
                log_fields(run_id=run_id, source=source_name, error=str(exc)),
            )
            continue

        for evaluation in result.all_evaluations:
            repository.record_rule_match(
                file_id=record.id,
                status=RuleMatchStatus.MATCHED if evaluation.matched else RuleMatchStatus.NO_MATCH,
                rule_id=evaluation.rule_id,
                details={
                    "matched": evaluation.matched,
                    "values": _json_safe(evaluation.extracted_values),
                    "errors": evaluation.errors,
                },
            )
            logger.debug(
                "Rule evaluation | %s",
                log_fields(
                    source=source_name,
                    rule_id=evaluation.rule_id,
                    matched=evaluation.matched,
                    errors=len(evaluation.errors),
                ),
            )

        if len(result.matched_rules) != 1:
            reason = "multiple_match" if result.matched_rules else "no_match"
            message = f"file matched {len(result.matched_rules)} rule(s)"
            repository.record_rule_match(
                file_id=record.id,
                status=RuleMatchStatus.MULTIPLE_MATCH if result.matched_rules else RuleMatchStatus.NO_MATCH,
                rule_id=None,
                details={"matched_rule_ids": [match.rule_id for match in result.matched_rules]},
            )
            failures.append(_quarantine_file(repository, config, record.id, archive_path, run_id, reason, message))
            logger.warning(
                "Classification mismatch; file quarantined | %s",
                log_fields(
                    run_id=run_id,
                    source=source_name,
                    reason=reason,
                    matched_rules=len(result.matched_rules),
                ),
            )
            continue

        matched_rule = result.matched_rules[0]
        rule_config = next(rule for rule in config.rules if rule.rule_id == matched_rule.rule_id)
        staging_path: Path | None = None
        try:
            logical_target_path, final_target_path, prepublish_path = _resolve_target_paths(
                config,
                rule_config.target_path_template,
                rule_config.publish_mode,
                run_id,
                matched_rule.rule_id,
                archive_path,
                matched_rule.extracted_values,
            )
            if rule_config.publish_mode == "supplement" and final_target_path.exists():
                raise FileSafetyError(f"target already exists: {final_target_path}")
            staging_path = staging_destination(config.directories.staging_dir, run_id, matched_rule.rule_id, archive_path)
            copy_verify(archive_path, staging_path, record.sha256)
            repository.mark_file_classified(
                file_id=record.id,
                staging_path=staging_path,
                logical_target_path=logical_target_path,
                final_target_path=final_target_path,
                prepublish_path=prepublish_path,
                publish_mode=rule_config.publish_mode,
            )
            logger.debug(
                "File classified | %s",
                log_fields(
                    run_id=run_id,
                    source=source_name,
                    rule_id=matched_rule.rule_id,
                    publish_mode=rule_config.publish_mode,
                    logical_target=logical_target_path,
                    prepublish=prepublish_path,
                ),
            )
        except (FileSafetyError, TargetCollisionError) as exc:
            if staging_path is not None and staging_path.exists():
                move_to_quarantine(staging_path, config.directories.quarantine_dir, run_id, "classification_failed")
            failures.append(_quarantine_file(repository, config, record.id, archive_path, run_id, "classification_failed", str(exc)))
            logger.warning(
                "Classification failed; file quarantined | %s",
                log_fields(run_id=run_id, source=source_name, error=str(exc)),
            )

    staged_count = len(repository.get_run_files(run_id, [FileStatus.CLASSIFIED_STAGING]))
    if staged_count:
        repository.mark_run(run_id, ProcessingRunStatus.CLASSIFIED)
        if failures:
            logger.warning(
                "Classification complete with failures | %s",
                log_fields(run_id=run_id, staged=staged_count, total=len(files), failed=len(failures)),
            )
        else:
            logger.info(
                "Classification complete | %s",
                log_fields(run_id=run_id, staged=staged_count),
            )
    else:
        message = "; ".join(failures)
        repository.mark_run(run_id, ProcessingRunStatus.COMPLETED_WITH_ERRORS, message)
        logger.error(
            "Classification failed for all files | %s",
            log_fields(run_id=run_id, failures=message),
        )
    return run_id


def _target_variables(run_id: str, rule_id: str, source_path: Path, extracted_values: dict) -> dict:
    now = datetime.now()
    return {
        "run_id": run_id,
        "rule_id": rule_id,
        "source_stem": source_path.stem,
        "source_name": source_path.name,
        "yyyy": f"{now:%Y}",
        "mm": f"{now:%m}",
        "dd": f"{now:%d}",
        **extracted_values,
    }


def _resolve_target_paths(
    config: IntegrationConfig,
    target_path_template: str,
    publish_mode: str,
    run_id: str,
    rule_id: str,
    source_path: Path,
    extracted_values: dict,
) -> tuple[Path, Path, Path]:
    logical_target_path = render_safe_target(
        config.directories.output_root,
        target_path_template,
        _target_variables(run_id, rule_id, source_path, extracted_values),
    )
    if publish_mode == "supplement":
        final_target_path = _versioned_target_path(logical_target_path, config.runtime.config_revision)
    else:
        final_target_path = logical_target_path
    output_root = config.directories.output_root.resolve()
    relative_target_path = final_target_path.relative_to(output_root)
    prepublish_path = config.directories.prepublish_dir.resolve() / relative_target_path
    return logical_target_path, final_target_path, prepublish_path


def _versioned_target_path(path: Path, config_revision: int) -> Path:
    suffix = "".join(path.suffixes)
    stem = path.name[: -len(suffix)] if suffix else path.name
    return path.with_name(f"{stem}__v{config_revision}{suffix}")


def _quarantine_file(
    repository: IntegrationRepository,
    config: IntegrationConfig,
    file_id: int,
    path: Path,
    run_id: str,
    reason: str,
    message: str,
) -> str:
    quarantine_path = move_to_quarantine(path, config.directories.quarantine_dir, run_id, reason)
    repository.mark_file_quarantined(file_id, quarantine_path, message)
    return f"{path.name}: {message}"


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime | date):
        return value.isoformat()
    return value
