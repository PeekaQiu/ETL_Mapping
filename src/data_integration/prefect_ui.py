"""Prefect UI labels — names stay stable; descriptions are employee-facing."""

from __future__ import annotations

# Flows
FLOW_INTEGRATION_LOOP = "integration-loop"
FLOW_INTEGRATION_LOOP_DESC = "Process new XML files for every enabled data source."

FLOW_PUBLISH_LOOP = "publish-loop"
FLOW_PUBLISH_LOOP_DESC = "Move staged files to their final published locations."

# Top-level source tasks
TASK_INTEGRATE_SOURCE = "Integrate Source"
TASK_INTEGRATE_SOURCE_DESC = "Archive, classify, validate, and stage files for one data source."

TASK_PUBLISH_SOURCE = "Publish Source"
TASK_PUBLISH_SOURCE_DESC = "Publish all staged files for one data source."

# Pipeline steps
TASK_ARCHIVE = "Step 1: Archive Files"
TASK_ARCHIVE_DESC = "Pick up stable XML files and copy them to the archive."

TASK_CLASSIFY = "Step 2: Classify Files"
TASK_CLASSIFY_DESC = "Match each file to a business rule and prepare staging paths."

TASK_PREPUBLISH = "Step 3: Validate and Prepublish Files"
TASK_PREPUBLISH_DESC = "Validate staged files and copy them to the prepublish area."

TASK_FORMAL_PUBLISH = "Publish Prepublished Files"
TASK_FORMAL_PUBLISH_DESC = "Promote prepublished files to their final output paths."

TASK_RETENTION = "Step 4: Cleanup Archive Retention"
TASK_RETENTION_DESC = "Remove archive files older than the retention period."

STEP_DESCRIPTIONS: dict[str, str] = {
    TASK_ARCHIVE: TASK_ARCHIVE_DESC,
    TASK_CLASSIFY: TASK_CLASSIFY_DESC,
    TASK_PREPUBLISH: TASK_PREPUBLISH_DESC,
    TASK_RETENTION: TASK_RETENTION_DESC,
}


def source_integrate_name(source: str) -> str:
    return f"{source} — Integrate Source"


def source_integrate_desc(source: str) -> str:
    return f"Ingest pipeline for data source: {source}."


def source_publish_name(source: str) -> str:
    return f"{source} — Publish Source"


def source_publish_desc(source: str) -> str:
    return f"Formal publish for data source: {source}."


def source_step_name(source: str, step_name: str) -> str:
    return f"{source} — {step_name}"


def source_step_desc(source: str, step_name: str) -> str:
    step_desc = STEP_DESCRIPTIONS.get(step_name, step_name)
    return f"{step_desc} Source: {source}."
