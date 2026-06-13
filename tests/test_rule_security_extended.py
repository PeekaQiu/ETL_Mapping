from pathlib import Path

import pytest
from pydantic import ValidationError

from data_integration.config.schema import IntegrationConfig
from data_integration.files.safety import FileSafetyError, render_safe_target
from data_integration.rules.engine import RuleEvaluationError, classify_xml_file, parse_xml
from tests.helpers import config_payload, make_config, write_xml


def test_complex_rule_supports_nested_logic_aliases_transforms_and_types(tmp_path: Path) -> None:
    rules = [
        {
            "rule_id": "complex_invoice",
            "target_path_template": "complex/{customer_code}/{source_name}",
            "conditions": {
                "all": [
                    {"xpath": "/Document/Type", "type": "str", "transform": "upper", "op": "eq", "value": "INVOICE"},
                    {"xpath": "/Document/Amount", "type": "decimal", "transform": "abs", "op": "between", "value": ["10", "100"]},
                    {"xpath": "/Document/Active", "type": "bool", "op": "eq", "value": True},
                    {"xpath": "/Document/Date", "type": "date", "op": "gte", "value": "2026-01-01"},
                    {"xpath": "/Document/Customer", "type": "str", "transform": "strip", "op": "regex", "value": "^ACME-\\d+$", "alias": "customer_code"},
                    {
                        "any": [
                            {"xpath": "/Document/Region", "type": "str", "transform": "lower", "op": "in", "value": ["apac", "emea"]},
                            {"xpath": "/Document/Priority", "type": "int", "op": "gte", "value": 9},
                        ]
                    },
                    {"not": {"xpath": "/Document/Status", "type": "str", "op": "eq", "value": "CANCELLED"}},
                ]
            },
        }
    ]
    config = make_config(tmp_path, rules)
    xml_path = tmp_path / "invoice.xml"
    write_xml(
        xml_path,
        """
        <Document>
          <Type> invoice </Type>
          <Amount>-42.50</Amount>
          <Active>true</Active>
          <Date>2026-06-12</Date>
          <Customer> ACME-123 </Customer>
          <Region>APAC</Region>
          <Priority>1</Priority>
          <Status>OPEN</Status>
        </Document>
        """,
    )

    result = classify_xml_file(xml_path, config)

    assert result.is_unique_match
    assert result.matched_rule is not None
    assert result.matched_rule.extracted_values["customer_code"] == "ACME-123"


def test_composite_rule_matches_deeply_nested_xml_paths(tmp_path: Path) -> None:
    rules = [
        {
            "rule_id": "deep_invoice",
            "target_path_template": "deep/{company_code}/{invoice_number}/{source_name}",
            "conditions": {
                "all": [
                    {
                        "xpath": "/Envelope/Header/Sender/CompanyCode",
                        "type": "str",
                        "transform": "upper",
                        "op": "eq",
                        "value": "TLO",
                        "alias": "company_code",
                    },
                    {
                        "xpath": "/Envelope/Body/Documents/Document/Metadata/DocumentType",
                        "type": "str",
                        "op": "eq",
                        "value": "INVOICE",
                    },
                    {
                        "xpath": "/Envelope/Body/Documents/Document/Identifiers/InvoiceNumber",
                        "type": "str",
                        "op": "regex",
                        "value": "^INV-\\d{6}$",
                        "alias": "invoice_number",
                    },
                    {
                        "any": [
                            {
                                "xpath": "/Envelope/Body/Documents/Document/Amounts/GrossAmount",
                                "type": "decimal",
                                "op": "between",
                                "value": ["100", "999.99"],
                            },
                            {
                                "xpath": "/Envelope/Body/Documents/Document/Routing/Priority",
                                "type": "int",
                                "op": "gte",
                                "value": 8,
                            },
                        ]
                    },
                    {
                        "not": {
                            "xpath": "/Envelope/Body/Documents/Document/Status/Code",
                            "type": "str",
                            "op": "in",
                            "value": ["CANCELLED", "VOID"],
                        }
                    },
                ]
            },
        }
    ]
    config = make_config(tmp_path, rules)
    xml_path = tmp_path / "deep_invoice.xml"
    write_xml(
        xml_path,
        """
        <Envelope>
          <Header>
            <Sender>
              <CompanyCode>tlo</CompanyCode>
            </Sender>
          </Header>
          <Body>
            <Documents>
              <Document>
                <Metadata>
                  <DocumentType>INVOICE</DocumentType>
                </Metadata>
                <Identifiers>
                  <InvoiceNumber>INV-123456</InvoiceNumber>
                </Identifiers>
                <Amounts>
                  <GrossAmount>888.88</GrossAmount>
                </Amounts>
                <Routing>
                  <Priority>3</Priority>
                </Routing>
                <Status>
                  <Code>OPEN</Code>
                </Status>
              </Document>
            </Documents>
          </Body>
        </Envelope>
        """,
    )

    result = classify_xml_file(xml_path, config)

    assert result.is_unique_match
    assert result.matched_rule is not None
    assert result.matched_rule.rule_id == "deep_invoice"
    assert result.matched_rule.extracted_values["company_code"] == "TLO"
    assert result.matched_rule.extracted_values["invoice_number"] == "INV-123456"


def test_xxe_payload_is_rejected(tmp_path: Path) -> None:
    xml_path = tmp_path / "xxe.xml"
    write_xml(
        xml_path,
        """<?xml version="1.0"?>
        <!DOCTYPE foo [ <!ENTITY xxe SYSTEM "file:///c:/windows/win.ini"> ]>
        <Document><Type>&xxe;</Type></Document>
        """,
    )

    with pytest.raises(RuleEvaluationError):
        parse_xml(xml_path)


def test_target_template_rejects_path_traversal_config(tmp_path: Path) -> None:
    payload = config_payload(
        tmp_path,
        rules=[
            {
                "rule_id": "bad",
                "target_path_template": "../escape/{source_name}",
                "conditions": {"all": [{"xpath": "/Document/Type", "op": "eq", "value": "INVOICE"}]},
            }
        ],
    )

    with pytest.raises(ValidationError):
        IntegrationConfig.model_validate(payload)


def test_render_safe_target_sanitizes_alias_values_and_stays_under_root(tmp_path: Path) -> None:
    target = render_safe_target(
        tmp_path / "C",
        "invoice/{customer_code}/{source_name}",
        {"customer_code": "../ACME:123", "source_name": "invoice.xml"},
    )

    assert (tmp_path / "C").resolve() in target.parents
    assert ".." not in target.parts
    assert "ACME_123" in target.parts


def test_render_safe_target_rejects_unknown_template_field(tmp_path: Path) -> None:
    with pytest.raises(FileSafetyError):
        render_safe_target(tmp_path / "C", "invoice/{missing}/file.xml", {"source_name": "file.xml"})
