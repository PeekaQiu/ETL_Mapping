from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any
from xml.etree.ElementTree import Element

from defusedxml import ElementTree

from data_integration.config.schema import IntegrationConfig, LogicalCondition, PredicateCondition, RuleConfig


class RuleEvaluationError(RuntimeError):
    pass


@dataclass(frozen=True)
class RuleEvaluation:
    rule_id: str
    matched: bool
    extracted_values: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ClassificationResult:
    matched_rules: list[RuleEvaluation]
    all_evaluations: list[RuleEvaluation]

    @property
    def is_unique_match(self) -> bool:
        return len(self.matched_rules) == 1

    @property
    def matched_rule(self) -> RuleEvaluation | None:
        if self.is_unique_match:
            return self.matched_rules[0]
        return None


def classify_xml_file(xml_path: Path, config: IntegrationConfig) -> ClassificationResult:
    root = parse_xml(xml_path)
    evaluations = [evaluate_rule(root, rule) for rule in config.rules if rule.enabled]
    matches = [evaluation for evaluation in evaluations if evaluation.matched]
    return ClassificationResult(matched_rules=matches, all_evaluations=evaluations)


def parse_xml(xml_path: Path) -> Element:
    try:
        return ElementTree.parse(xml_path).getroot()
    except Exception as exc:
        raise RuleEvaluationError(f"failed to parse XML {xml_path}: {exc}") from exc


def evaluate_rule(root: Element, rule: RuleConfig) -> RuleEvaluation:
    values: dict[str, Any] = {}
    try:
        matched = evaluate_condition(root, rule.conditions, values)
        return RuleEvaluation(rule_id=rule.rule_id, matched=matched, extracted_values=values)
    except Exception as exc:
        return RuleEvaluation(
            rule_id=rule.rule_id,
            matched=False,
            extracted_values=values,
            errors=[str(exc)],
        )


def evaluate_condition(root: Element, condition: PredicateCondition | LogicalCondition, values: dict[str, Any]) -> bool:
    if isinstance(condition, PredicateCondition):
        return evaluate_predicate(root, condition, values)

    if condition.all is not None:
        return all(evaluate_condition(root, child, values) for child in condition.all)
    if condition.any is not None:
        return any(evaluate_condition(root, child, values) for child in condition.any)
    if condition.not_ is not None:
        return not evaluate_condition(root, condition.not_, values)
    raise RuleEvaluationError("invalid logical condition")


def evaluate_predicate(root: Element, predicate: PredicateCondition, values: dict[str, Any]) -> bool:
    raw_value = extract_text(root, predicate.xpath)
    if predicate.op == "exists":
        matched = raw_value is not None
        if predicate.alias and raw_value is not None:
            values[predicate.alias] = raw_value
        return matched

    if raw_value is None:
        return False

    value = coerce_value(raw_value, predicate.type)
    value = apply_transform(value, predicate.transform)
    if predicate.alias:
        values[predicate.alias] = value
    return compare(value, predicate.op, predicate.value)


def extract_text(root: Element, xpath: str) -> str | None:
    normalized = normalize_xpath(root, xpath)
    element = root.find(normalized)
    if element is None:
        return None
    return (element.text or "").strip()


def normalize_xpath(root: Element, xpath: str) -> str:
    normalized = xpath.strip()
    if normalized.startswith("/"):
        parts = [part for part in normalized.split("/") if part]
        if parts and parts[0] == root.tag:
            parts = parts[1:]
        normalized = "/".join(parts)
    if not normalized:
        return "."
    if normalized.startswith("."):
        return normalized
    return normalized


def coerce_value(value: str, value_type: str) -> Any:
    if value_type == "str":
        return value
    if value_type == "int":
        return int(value)
    if value_type == "float":
        return float(value)
    if value_type == "decimal":
        return Decimal(value)
    if value_type == "date":
        return date.fromisoformat(value)
    if value_type == "bool":
        lowered = value.lower()
        if lowered in {"true", "1", "yes", "y"}:
            return True
        if lowered in {"false", "0", "no", "n"}:
            return False
        raise ValueError(f"cannot coerce bool from {value!r}")
    raise RuleEvaluationError(f"unsupported value type: {value_type}")


def apply_transform(value: Any, transform: str | None) -> Any:
    if transform is None:
        return value
    if transform == "strip":
        return str(value).strip()
    if transform == "lower":
        return str(value).lower()
    if transform == "upper":
        return str(value).upper()
    if transform == "abs":
        return abs(value)
    raise RuleEvaluationError(f"unsupported transform: {transform}")


def compare(value: Any, operator: str, expected: Any) -> bool:
    expected = normalize_expected(value, expected)
    if operator == "eq":
        return value == expected
    if operator == "ne":
        return value != expected
    if operator == "gt":
        return value > expected
    if operator == "gte":
        return value >= expected
    if operator == "lt":
        return value < expected
    if operator == "lte":
        return value <= expected
    if operator == "between":
        lower, upper = expected
        return lower <= value <= upper
    if operator == "in":
        return value in expected
    if operator == "contains":
        return str(expected) in str(value)
    if operator == "startswith":
        return str(value).startswith(str(expected))
    if operator == "endswith":
        return str(value).endswith(str(expected))
    if operator == "regex":
        return re.search(str(expected), str(value)) is not None
    raise RuleEvaluationError(f"unsupported operator: {operator}")


def normalize_expected(value: Any, expected: Any) -> Any:
    if isinstance(expected, list):
        return [normalize_expected(value, item) for item in expected]
    if isinstance(value, Decimal):
        return Decimal(str(expected))
    if isinstance(value, int) and not isinstance(value, bool):
        return int(expected)
    if isinstance(value, float):
        return float(expected)
    if isinstance(value, date):
        return date.fromisoformat(str(expected))
    if isinstance(value, bool):
        if isinstance(expected, bool):
            return expected
        return str(expected).lower() in {"true", "1", "yes", "y"}
    return expected
