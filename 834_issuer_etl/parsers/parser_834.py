"""
834 XML parser — preserves full payload as JSON, maps staging columns.
Adapted from the original xml_parser with staging schema alignment.
"""

from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any

from utils.logger import get_logger

logger = get_logger(__name__)

ACTION_DESCRIPTIONS = {
    "CONFIRM": "Confirmed/Effectuated",
    "CANCEL": "Cancelled",
    "TERM": "Terminated",
    "REINSTATE": "Reinstated",
}

# Companion Guide REF*4A — Exchange Assigned Enrollee ID (camelCase XML variants).
ENROLLEE_ID_TAGS = (
    "exchgAssignedEnrolleeID",
    "exchgAssignedEnrolleeId",
    "exchgEnrolleeIdentifier",
    "exchgEnrolleeID",
)

REQUEST_SUBMIT_TAGS = (
    "requestSubmitTimestamp",
    "requestSubmitTimeStamp",
    "requestSubmitTimeStampValue",
)


def _text(el: ET.Element | None) -> str | None:
    if el is None or el.text is None:
        return None
    return el.text.strip()


def _lookup(parent: ET.Element | None) -> str | None:
    if parent is None:
        return None
    return _text(parent.find("lookupValueCode"))


def _first_text(parent: ET.Element | None, *tag_names: str) -> str | None:
    """Return the first non-empty text among candidate child tag names."""
    if parent is None:
        return None
    for tag in tag_names:
        value = _text(parent.find(tag))
        if value:
            return value
    return None


def _request_submit_timestamp(reporting: ET.Element | None) -> str | None:
    """2750 REQUEST SUBMIT TIMESTAMP — optional reporting-category field."""
    if reporting is None:
        return None
    value = _first_text(reporting, *REQUEST_SUBMIT_TAGS)
    if value:
        return value
    for child in reporting:
        if "requestsubmit" in child.tag.lower():
            found = _text(child)
            if found:
                return found
    return None


def _parse_date(raw: str | None) -> str | None:
    if not raw or len(raw) < 8:
        return None
    try:
        return f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]}"
    except (ValueError, IndexError):
        return raw


def _float_val(raw: str | None) -> float | None:
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _parse_enrollment_header(enrollment: ET.Element) -> dict[str, Any]:
    """Enrollment-level fields shared across all enrollees in the segment."""
    return {
        "enrollment_action_code": _text(enrollment.find("actionCode")),
        "insurer_tax_id_number": _text(enrollment.find("insurerTaxIdNumber")),
        "qtyn": _text(enrollment.find("QTYn")),
        "qtyy": _text(enrollment.find("QTYy")),
        "qtyt": _text(enrollment.find("QTYt")),
    }


class Parser834:
    """Parse 834 enrollment XML into staging-ready record dicts."""

    def parse_file(
        self,
        xml_bytes: bytes,
        issuer: str,
        year: str,
        month: str,
        file_name: str,
        file_path: str,
    ) -> list[dict[str, Any]]:
        root = ET.fromstring(xml_bytes)
        records: list[dict[str, Any]] = []

        for enrollment in root.findall("enrollment"):
            header = _parse_enrollment_header(enrollment)
            for enrollee in enrollment.findall("enrollee"):
                row = self._parse_enrollee(enrollee)
                row.update(header)
                row.update({
                    "issuer": issuer,
                    "year": year,
                    "month": month,
                    "file_name": file_name,
                    "raw_xml_path": file_path,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                })
                # Backward compatibility: action_code mirrors enrollee event type only.
                row["action_code"] = row.get("enrollee_event_type_code")
                row["raw_payload"] = json.dumps(row, default=str)
                records.append(row)

        logger.info("Parsed %d record(s) from %s", len(records), file_name)
        return records

    def _parse_enrollee(self, enrollee: ET.Element) -> dict[str, Any]:
        health = enrollee.find("healthCoverage")
        reporting = enrollee.find("memberReportingCategory")
        events = enrollee.find("enrollmentEvents")

        action_desc_code = None
        if reporting is not None:
            action_desc_code = _lookup(reporting.find("additionalMaintReason"))

        action_desc = ACTION_DESCRIPTIONS.get(
            action_desc_code or "", action_desc_code or "Unknown"
        )

        benefit_begin = None
        benefit_end = None
        last_premium_paid = None
        if health is not None:
            benefit_begin = _parse_date(_text(health.find("benefitEffectiveBeginDate")))
            benefit_end = _parse_date(_text(health.find("benefitEffectiveEndDate")))
            last_premium_paid = _text(health.find("lastPremiumPaidDate"))

        total_premium = _float_val(
            _text(reporting.find("totalPremiumAmt")) if reporting is not None else None
        )
        indiv_resp = _float_val(
            _text(reporting.find("totalIndivResponsibilityAmt"))
            if reporting is not None
            else None
        )
        aptc = _float_val(
            _text(reporting.find("aptcAmt")) if reporting is not None else None
        )
        user_fee = round(total_premium * 0.0325, 4) if total_premium else None

        enrollee_event_type = (
            _lookup(events.find("eventTypeLkp")) if events is not None else None
        )
        enrollee_event_reason = None
        if events is not None:
            enrollee_event_reason = _lookup(events.find("eventReasonLookUp"))
            if enrollee_event_reason is None:
                enrollee_event_reason = _lookup(events.find("eventReasonLookup"))

        return {
            "policy_id": _text(enrollee.find("exchgAssignedPolicyID")),
            "member_id": _text(enrollee.find("exchgIndivIdentifier")),
            "subscriber_id": _text(enrollee.find("exchgSubscriberIdentifier")),
            "exchg_assigned_enrollee_id": _first_text(enrollee, *ENROLLEE_ID_TAGS),
            "issuer_subscriber_identifier": _text(
                enrollee.find("issuerSubscriberIdentifier")
            ),
            "issuer_indiv_identifier": _text(enrollee.find("issuerIndivIdentifier")),
            "member_first_name": _text(enrollee.find("memberFirstName")),
            "member_last_name": _text(enrollee.find("memberLastName")),
            "relationship": _lookup(enrollee.find("relationshipLkp")),
            "subscriber_flag": _text(enrollee.find("subscriberFlag")),
            "enrollee_event_type_code": enrollee_event_type,
            "enrollee_event_reason_code": enrollee_event_reason,
            "action_code_description": action_desc,
            "maintenance_type_code": (
                _lookup(health.find("maintenanceTypeCode")) if health else None
            ),
            "additional_maint_reason_code": action_desc_code,
            "coverage_status": action_desc,
            "benefit_effective_date": benefit_begin,
            "benefit_end_date": benefit_end,
            "member_maint_effective_date": _parse_date(
                _text(enrollee.find("memberMaintEffectiveDate"))
            ),
            "last_premium_paid_date": last_premium_paid,
            "request_submit_timestamp": _request_submit_timestamp(reporting),
            "total_premium_amount": total_premium,
            "individual_responsibility_amount": indiv_resp,
            "aptc_amount": aptc,
            "user_fee_amount": user_fee,
            "insurance_type_code": (
                _lookup(health.find("insuranceTypeLkp")) if health else None
            ),
            "health_coverage_policy_no": (
                _text(health.find("healthCoveragePolicyNo")) if health else None
            ),
            "household_or_employee_case_id": (
                _text(health.find("householdOrEmployeeCaseID")) if health else None
            ),
            "rating_area": (
                _text(reporting.find("ratingArea")) if reporting is not None else None
            ),
            "source_exchg_id": (
                _text(reporting.find("sourceExchgID")) if reporting is not None else None
            ),
        }
