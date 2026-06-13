from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from prefect import get_run_logger, task

from data_integration.config.schema import IntegrationConfig
from data_integration.files.safety import (
    FileSafetyError,
    copy_verify,
    move_to_quarantine,
    render_safe_target,
    staging_destination,
)
from data_integration.rules.engine import RuleEvaluationError, classify_xml_file
from data_integration.storage.db import build_engine, build_session_factory, init_db
from data_integration.storage.models import FileStatus, ProcessingRunStatus, RuleMatchStatus
from data_integration.storage.repository import (
    IntegrationRepository,
    TargetCollisionError,
)


class ClassificationRunError(RuntimeError):
    pass


@task(name="Step 2: Classify Files", retries=1, retry_delay_seconds=10)
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
    logger = _logger()
    files = repository.get_run_files(run_id, [FileStatus.ARCHIVED_B])
    if not files:
        repository.mark_run(run_id, ProcessingRunStatus.FAILED, "No archived files available for classification.")
        raise ClassificationRunError(f"source run {run_id} has no archived files")

    failures: list[str] = []
    for record in files:
        archive_path = Path(record.archive_path)
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
            continue

        matched_rule = result.matched_rules[0]
        rule_config = next(rule for rule in config.rules if rule.rule_id == matched_rule.rule_id)
        staging_path: Path | None = None
        try:
            target_path = render_safe_target(
                config.directories.output_root,
                rule_config.target_path_template,
                _target_variables(run_id, matched_rule.rule_id, archive_path, matched_rule.extracted_values),
            )
            if target_path.exists():
                raise FileSafetyError(f"target already exists: {target_path}")
            staging_path = staging_destination(config.directories.staging_dir, run_id, matched_rule.rule_id, archive_path)
            copy_verify(archive_path, staging_path, record.sha256)
            repository.mark_file_classified(
                file_id=record.id,
                staging_path=staging_path,
                target_path=target_path,
            )
        except (FileSafetyError, TargetCollisionError) as exc:
            if staging_path is not None and staging_path.exists():
                move_to_quarantine(staging_path, config.directories.quarantine_dir, run_id, "classification_failed")
            failures.append(_quarantine_file(repository, config, record.id, archive_path, run_id, "classification_failed", str(exc)))

    if failures:
        _quarantine_staged_files(config, repository, run_id, "run_classification_failed")
        message = "; ".join(failures)
        repository.mark_run(run_id, ProcessingRunStatus.FAILED, message)
        logger.error("Classification failed for source run %s: %s", run_id, message)
        raise ClassificationRunError(message)

    repository.mark_run(run_id, ProcessingRunStatus.CLASSIFIED)
    logger.info("Classified %s file(s) for source run %s.", len(files), run_id)
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


def _quarantine_staged_files(
    config: IntegrationConfig,
    repository: IntegrationRepository,
    run_id: str,
    reason: str,
) -> None:
    staged_files = repository.get_run_files(run_id, [FileStatus.CLASSIFIED_STAGING])
    for record in staged_files:
        if record.staging_path:
            quarantine_path = move_to_quarantine(
                Path(record.staging_path),
                config.directories.quarantine_dir,
                run_id,
                reason,
            )
            repository.mark_file_quarantined(record.id, quarantine_path, f"source run failed: {reason}")


def _logger() -> logging.Logger:
    try:
        return get_run_logger()
    except Exception:
        return logging.getLogger(__name__)


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
