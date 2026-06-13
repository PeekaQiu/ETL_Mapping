from pathlib import Path

from data_integration.config.schema import IntegrationConfig
from data_integration.rules.engine import classify_xml_file


def test_rule_engine_finds_unique_match(tmp_path: Path) -> None:
    xml_path = tmp_path / "invoice.xml"
    xml_path.write_text("<Document><Type>INVOICE</Type><Amount>42.50</Amount></Document>", encoding="utf-8")
    config = _config(tmp_path)

    result = classify_xml_file(xml_path, config)

    assert result.is_unique_match
    assert result.matched_rule is not None
    assert result.matched_rule.rule_id == "invoice"


def test_rule_engine_detects_multiple_matches(tmp_path: Path) -> None:
    xml_path = tmp_path / "invoice.xml"
    xml_path.write_text("<Document><Type>INVOICE</Type><Amount>42.50</Amount></Document>", encoding="utf-8")
    data = _config_data(tmp_path)
    data["rules"].append(
        {
            "rule_id": "invoice_copy",
            "target_path_template": "copy/{source_name}",
            "conditions": {"all": [{"xpath": "/Document/Type", "op": "eq", "value": "INVOICE"}]},
        }
    )
    config = IntegrationConfig.model_validate(data)

    result = classify_xml_file(xml_path, config)

    assert not result.is_unique_match
    assert [match.rule_id for match in result.matched_rules] == ["invoice", "invoice_copy"]


def _config(tmp_path: Path) -> IntegrationConfig:
    return IntegrationConfig.model_validate(_config_data(tmp_path))


def _config_data(tmp_path: Path) -> dict:
    return {
        "directories": {
            "source_dirs": [tmp_path / "A"],
            "archive_dir": tmp_path / "B",
            "staging_dir": tmp_path / "C_staging",
            "output_root": tmp_path / "C",
            "quarantine_dir": tmp_path / "quarantine",
            "sqlite_path": tmp_path / "integration.sqlite3",
            "lock_file": tmp_path / "integration.lock",
        },
        "runtime": {"file_stability_seconds": 0},
        "rules": [
            {
                "rule_id": "invoice",
                "target_path_template": "invoice/{source_name}",
                "conditions": {
                    "all": [
                        {"xpath": "/Document/Type", "type": "str", "op": "eq", "value": "INVOICE"},
                        {"xpath": "/Document/Amount", "type": "decimal", "op": "between", "value": ["0", "100"]},
                    ]
                },
            }
        ],
    }
