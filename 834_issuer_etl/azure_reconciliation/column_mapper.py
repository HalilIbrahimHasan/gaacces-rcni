"""
Dynamic column mapping between local XML/SQLite and Azure SQL schemas.

Inspects available columns on both sides and maps equivalents with confidence scores.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Any

import pandas as pd

# Canonical business fields used for reconciliation join and comparison.
CANONICAL_FIELDS: dict[str, dict[str, list[str]]] = {
    "issuer": {
        "xml": ["issuer", "issuer_id", "hios_issuer_id", "gaa_hios_id"],
        "azure": ["hios_issuer_id", "GAA_HIOS_ID", "issuer_id", "hios_id", "carrier_id", "issuer"],
    },
    "enrollment_id": {
        "xml": [
            "policy_id",
            "exchg_assigned_policy_id",
            "enrollment_id",
            "exchange_assigned_policy_id",
        ],
        "azure": [
            "enrollment_id", "policy_id", "exchgAssignedPolicyID",
            "exchg_assigned_policy_id", "exchange_assigned_policy_id",
        ],
    },
    "enrollee_id": {
        "xml": [
            "member_id",
            "exchg_indiv_identifier",
            "exchg_assigned_enrollee_id",
            "enrollee_id",
        ],
        "azure": [
            "enrollee_id", "member_id", "exchgIndivIdentifier",
            "exchg_indiv_identifier", "exchange_member_id", "exchange_enrollee_id",
        ],
    },
    "subscriber_id": {
        "xml": ["subscriber_id", "exchg_subscriber_identifier"],
        "azure": ["subscriber_id", "exchange_subscriber_id"],
    },
    "insurance_type": {
        "xml": ["insurance_type_code", "insurance_type", "insurance_type_description"],
        "azure": ["insurance_type", "insurance_type_code", "product_type"],
    },
    "coverage_year": {
        "xml": ["year", "source_year", "coverage_year"],
        "azure": ["coverage_year", "plan_year", "benefit_year"],
    },
    "enrollment_status": {
        "xml": [
            "additional_maint_reason_code",
            "action_code_description",
            "coverage_status",
            "transaction_classification",
        ],
        "azure": [
            "enrollment_status_description",
            "enrollee_status_description",
            "enrolleeStatus",
            "enrollment_status",
            "enrollee_status",
            "actionCode",
            "action_code",
            "status_description",
            "status",
        ],
    },
    "maintenance_type_code": {
        "xml": ["maintenance_type_code"],
        "azure": ["maintenance_type_code", "maint_type_code"],
    },
    "benefit_effective_date": {
        "xml": ["benefit_effective_date", "benefit_effective_begin_date"],
        "azure": ["benefit_effective_date", "enrollee_start_date", "coverage_start_date"],
    },
    "benefit_end_date": {
        "xml": ["benefit_end_date", "benefit_effective_end_date"],
        "azure": ["benefit_end_date", "enrollee_end_date", "coverage_end_date"],
    },
    "member_maint_effective_date": {
        "xml": ["member_maint_effective_date"],
        "azure": [
            "enrollment_last_update_date",
            "enrollee_last_update_date",
            "enrollment_confirmation_date",
        ],
    },
    "request_submit_timestamp": {
        "xml": ["request_submit_timestamp"],
        "azure": ["enrollment_create_date", "enrollee_create_date", "application_create_date"],
    },
    "gross_premium": {
        "xml": ["total_premium_amount", "total_premium_amt"],
        "azure": ["gross_premium_amt", "total_premium_amt", "premium_amt"],
    },
    "net_premium": {
        "xml": ["individual_responsibility_amount", "total_indiv_responsibility_amt"],
        "azure": ["net_premium_amt", "total_indiv_responsibility_amt"],
    },
    "aptc_amount": {
        "xml": ["aptc_amount", "aptc_amt"],
        "azure": ["aptc_amt", "aptc_amount"],
    },
    "plan_id": {
        "xml": ["health_coverage_policy_no", "cms_plan_id"],
        "azure": ["cms_plan_id", "plan_id", "plan_name"],
    },
    "household_id": {
        "xml": ["household_or_employee_case_id"],
        "azure": ["household_id", "client_id", "household_or_employee_case_id"],
    },
}

JOIN_KEY_CANONICAL = ["issuer", "enrollment_id", "enrollee_id", "insurance_type"]


def _normalize_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name).lower())


def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, _normalize_name(a), _normalize_name(b)).ratio()


@dataclass
class ColumnMatch:
    canonical_field: str
    xml_column: str | None
    azure_column: str | None
    confidence: str  # exact | synonym | fuzzy | unmatched
    confidence_score: float
    notes: str = ""


@dataclass
class ColumnMappingResult:
    matches: list[ColumnMatch] = field(default_factory=list)
    xml_unmatched: list[str] = field(default_factory=list)
    azure_unmatched: list[str] = field(default_factory=list)
    join_key_xml: dict[str, str | None] = field(default_factory=dict)
    join_key_azure: dict[str, str | None] = field(default_factory=dict)

    def to_dataframe(self) -> pd.DataFrame:
        rows = []
        for m in self.matches:
            rows.append({
                "canonical_field": m.canonical_field,
                "xml_column": m.xml_column or "",
                "azure_column": m.azure_column or "",
                "confidence": m.confidence,
                "confidence_score": round(m.confidence_score, 3),
                "notes": m.notes,
            })
        return pd.DataFrame(rows)


def _best_match(
    canonical: str,
    side: str,
    available: list[str],
    used: set[str],
) -> tuple[str | None, str, float, str]:
    """Return (column, confidence_label, score, notes)."""
    synonyms = CANONICAL_FIELDS.get(canonical, {}).get(side, [])
    norm_available = {_normalize_name(c): c for c in available}

    # Exact / synonym match
    for syn in synonyms:
        norm = _normalize_name(syn)
        if norm in norm_available:
            col = norm_available[norm]
            if col not in used:
                label = "exact" if _normalize_name(col) == _normalize_name(canonical) else "synonym"
                return col, label, 1.0 if label == "exact" else 0.95, f"synonym:{syn}"

    # Fuzzy match against available columns
    best_col = None
    best_score = 0.0
    for col in available:
        if col in used:
            continue
        for syn in synonyms + [canonical]:
            score = _similarity(col, syn)
            if score > best_score:
                best_score = score
                best_col = col

    if best_col and best_score >= 0.72:
        return best_col, "fuzzy", best_score, "fuzzy match"

    return None, "unmatched", 0.0, "no match found"


def build_column_mapping(
    xml_columns: list[str],
    azure_columns: list[str],
) -> ColumnMappingResult:
    """
    Map canonical reconciliation fields to actual XML and Azure column names.
    """
    result = ColumnMappingResult()
    xml_used: set[str] = set()
    azure_used: set[str] = set()

    for canonical in CANONICAL_FIELDS:
        xml_col, xml_conf, xml_score, xml_notes = _best_match(
            canonical, "xml", xml_columns, xml_used
        )
        az_col, az_conf, az_score, az_notes = _best_match(
            canonical, "azure", azure_columns, azure_used
        )
        if xml_col:
            xml_used.add(xml_col)
        if az_col:
            azure_used.add(az_col)

        overall_conf = "unmatched"
        overall_score = min(xml_score, az_score) if xml_col and az_col else max(xml_score, az_score)
        if xml_col and az_col:
            if xml_conf == "exact" and az_conf == "exact":
                overall_conf = "exact"
            elif xml_conf in ("exact", "synonym") and az_conf in ("exact", "synonym"):
                overall_conf = "synonym"
            else:
                overall_conf = "fuzzy"
        elif xml_col or az_col:
            overall_conf = "partial"
            overall_score *= 0.5

        result.matches.append(
            ColumnMatch(
                canonical_field=canonical,
                xml_column=xml_col,
                azure_column=az_col,
                confidence=overall_conf,
                confidence_score=overall_score,
                notes=f"xml:{xml_notes}; azure:{az_notes}",
            )
        )

    result.xml_unmatched = [c for c in xml_columns if c not in xml_used]
    result.azure_unmatched = [c for c in azure_columns if c not in azure_used]

    for key in JOIN_KEY_CANONICAL:
        match = next((m for m in result.matches if m.canonical_field == key), None)
        result.join_key_xml[key] = match.xml_column if match else None
        result.join_key_azure[key] = match.azure_column if match else None

    return result


def rename_to_canonical(df: pd.DataFrame, mapping: ColumnMappingResult, side: str) -> pd.DataFrame:
    """Rename source columns to canonical names using mapping result."""
    rename: dict[str, str] = {}
    for m in mapping.matches:
        col = m.xml_column if side == "xml" else m.azure_column
        if col and col in df.columns:
            rename[col] = m.canonical_field
    out = df.rename(columns=rename)
    return out


def mapping_report_sheets(result: ColumnMappingResult) -> dict[str, pd.DataFrame]:
    """Build Excel sheet dict for column_mapping_report.xlsx."""
    mapping_df = result.to_dataframe()
    unmatched_xml = pd.DataFrame({"xml_column": result.xml_unmatched})
    unmatched_azure = pd.DataFrame({"azure_column": result.azure_unmatched})
    join_xml = pd.DataFrame([{"canonical": k, "xml_column": v} for k, v in result.join_key_xml.items()])
    join_azure = pd.DataFrame([{"canonical": k, "azure_column": v} for k, v in result.join_key_azure.items()])
    return {
        "field_mappings": mapping_df,
        "unmatched_xml": unmatched_xml,
        "unmatched_azure": unmatched_azure,
        "join_key_xml": join_xml,
        "join_key_azure": join_azure,
    }
