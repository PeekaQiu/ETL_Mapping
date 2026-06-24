from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


ValueType = Literal["str", "int", "float", "decimal", "date", "bool"]
PublishMode = Literal["replace", "supplement"]
Operator = Literal[
    "exists",
    "eq",
    "ne",
    "gt",
    "gte",
    "lt",
    "lte",
    "between",
    "in",
    "contains",
    "startswith",
    "endswith",
    "regex",
]
Transform = Literal["strip", "lower", "upper", "abs"]


class DirectoryConfig(BaseModel):
    """Directories used by the integration flow."""

    source_dirs: list[Path] = Field(min_length=1)
    archive_dir: Path
    staging_dir: Path
    output_root: Path
    prepublish_dir: Path | None = None
    quarantine_dir: Path
    sqlite_path: Path = Path("runtime/integration.sqlite3")
    lock_file: Path = Path("runtime/integration.lock")

    @model_validator(mode="after")
    def default_prepublish_dir(self) -> "DirectoryConfig":
        if self.prepublish_dir is None:
            self.prepublish_dir = self.output_root.parent / f"{self.output_root.name}_prepublish"
        return self


class RuntimeConfig(BaseModel):
    file_stability_seconds: int = Field(default=5, ge=0)
    archive_retention_days: int = Field(default=30, ge=0)
    detail_retention_days: int | None = Field(default=90, ge=0)
    cleanup_empty_dirs: bool = True
    no_overlap: bool = True
    config_revision: int = Field(default=1, ge=1)


class PredicateCondition(BaseModel):
    xpath: str
    type: ValueType = "str"
    op: Operator
    value: Any = None
    transform: Transform | None = None
    alias: str | None = None

    @field_validator("xpath")
    @classmethod
    def validate_xpath(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("xpath must not be empty")
        return value.strip()


class LogicalCondition(BaseModel):
    all: list["Condition"] | None = None
    any: list["Condition"] | None = None
    not_: "Condition | None" = Field(default=None, alias="not")

    model_config = ConfigDict(populate_by_name=True)

    @model_validator(mode="after")
    def validate_one_operator(self) -> "LogicalCondition":
        populated = [self.all is not None, self.any is not None, self.not_ is not None]
        if sum(populated) != 1:
            raise ValueError("logical condition must define exactly one of all, any, not")
        if self.all is not None and not self.all:
            raise ValueError("all condition must contain at least one child")
        if self.any is not None and not self.any:
            raise ValueError("any condition must contain at least one child")
        return self


Condition = PredicateCondition | LogicalCondition


class RuleConfig(BaseModel):
    rule_id: str
    target_path_template: str
    conditions: Condition
    publish_mode: PublishMode = "replace"
    description: str | None = None
    enabled: bool = True

    @field_validator("rule_id")
    @classmethod
    def validate_rule_id(cls, value: str) -> str:
        rule_id = value.strip()
        if not rule_id:
            raise ValueError("rule_id must not be empty")
        if any(ch in rule_id for ch in "\\/:*?\"<>|"):
            raise ValueError("rule_id contains characters unsafe for Windows paths")
        return rule_id

    @field_validator("target_path_template")
    @classmethod
    def validate_target_template(cls, value: str) -> str:
        template = value.strip()
        if not template:
            raise ValueError("target_path_template must not be empty")
        candidate = Path(template)
        if candidate.is_absolute():
            raise ValueError("target_path_template must be relative")
        if ".." in candidate.parts:
            raise ValueError("target_path_template must not escape output root")
        return template


class NotificationConfig(BaseModel):
    recipients: list[str] = Field(default_factory=list)


class FlowConfigRef(BaseModel):
    name: str
    config_variable: str | None = None
    config_path: str | None = None
    enabled: bool = True

    @model_validator(mode="after")
    def validate_ref_source(self) -> "FlowConfigRef":
        if bool(self.config_variable) == bool(self.config_path):
            raise ValueError("flow config ref must define exactly one of config_variable or config_path")
        return self


class BulkLoopConfig(BaseModel):
    delay_seconds: int = Field(default=300, ge=0)
    cycles: int | None = Field(default=None, gt=0)
    stop_on_failure: bool = False


class BulkIntegrationConfig(BaseModel):
    name: str = "bulk-sources-controller"
    flow_configs: list[FlowConfigRef] = Field(min_length=1)
    loop: BulkLoopConfig = Field(default_factory=BulkLoopConfig)

    @model_validator(mode="after")
    def validate_unique_flow_names(self) -> "BulkIntegrationConfig":
        names = [flow_config.name for flow_config in self.flow_configs]
        if len(names) != len(set(names)):
            raise ValueError("flow config names must be unique")
        return self

    def snapshot(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class IntegrationConfig(BaseModel):
    directories: DirectoryConfig
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)
    rules: list[RuleConfig] = Field(min_length=1)
    notifications: NotificationConfig = Field(default_factory=NotificationConfig)

    @model_validator(mode="after")
    def validate_unique_rules(self) -> "IntegrationConfig":
        rule_ids = [rule.rule_id for rule in self.rules]
        if len(rule_ids) != len(set(rule_ids)):
            raise ValueError("rule_id values must be unique")
        return self

    def snapshot(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


LogicalCondition.model_rebuild(_types_namespace={"Condition": Condition})
