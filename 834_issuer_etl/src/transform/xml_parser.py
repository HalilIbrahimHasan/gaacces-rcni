"""
834 XML parser — converts enrollment XML into flat enrollee row dictionaries.

Each ``<enrollment>`` may contain multiple ``<enrollee>`` elements; this
module emits one normalized dict per enrollee with header fields repeated.
"""

import xml.etree.ElementTree as ET
from typing import Any

from utils.file_utils import parse_issuer_from_filename
from utils.logger import get_logger

logger = get_logger(__name__)


def _text(element: ET.Element | None) -> str | None:
    """Safely return stripped text from an XML element."""
    if element is None or element.text is None:
        return None
    return element.text.strip()


def _lookup_code(parent: ET.Element | None) -> str | None:
    """
    Extract ``lookupValueCode`` from a lookup wrapper element.

    Many 834 fields nest codes inside ``*Lkp`` elements rather than exposing
    plain text values at the parent tag.
    """
    if parent is None:
        return None
    code_el = parent.find("lookupValueCode")
    return _text(code_el)


def _parse_enrollment_header(enrollment: ET.Element) -> dict[str, Any]:
    """
    Parse file/enrollment-level header fields shared across all enrollees.

    Captures ISA/GS/ST segments and quantity counts needed for validation
    and traceability back to the EDI interchange.
    """
    return {
        "isa09": _text(enrollment.find("ISA09")),
        "isa10": _text(enrollment.find("ISA10")),
        "isa13": _text(enrollment.find("ISA13")),
        "gs04": _text(enrollment.find("GS04")),
        "gs05": _text(enrollment.find("GS05")),
        "gs06": _text(enrollment.find("GS06")),
        "st02": _text(enrollment.find("ST02")),
        "action_code": _text(enrollment.find("actionCode")),
        "insurer_tax_id_number": _text(enrollment.find("insurerTaxIdNumber")),
        "qtyn": _text(enrollment.find("QTYn")),
        "qtyy": _text(enrollment.find("QTYy")),
        "qtyt": _text(enrollment.find("QTYt")),
    }


def _parse_enrollee(enrollee: ET.Element) -> dict[str, Any]:
    """
    Parse a single ``<enrollee>`` element into a flat field dictionary.

    Handles nested lookups (relationship, events, gender, coverage) and
    optionally captures PII fields when export policy allows it upstream.
    """
    health = enrollee.find("healthCoverage")
    reporting = enrollee.find("memberReportingCategory")
    address = enrollee.find("memberHomeAddress")

    row: dict[str, Any] = {
        "subscriber_flag": _text(enrollee.find("subscriberFlag")),
        "relationship_code": _lookup_code(enrollee.find("relationshipLkp")),
        "event_type_code": None,
        "event_reason_code": None,
        "exchg_subscriber_identifier": _text(
            enrollee.find("exchgSubscriberIdentifier")
        ),
        "exchg_assigned_policy_id": _text(enrollee.find("exchgAssignedPolicyID")),
        "exchg_indiv_identifier": _text(enrollee.find("exchgIndivIdentifier")),
        "issuer_subscriber_identifier": _text(
            enrollee.find("issuerSubscriberIdentifier")
        ),
        "issuer_indiv_identifier": _text(enrollee.find("issuerIndivIdentifier")),
        "member_maint_effective_date": _text(
            enrollee.find("memberMaintEffectiveDate")
        ),
        "member_entity_identifier_code": _text(
            enrollee.find("memberEntityIdentifierCode")
        ),
        "member_gender_code": _lookup_code(enrollee.find("memberGenderLkp")),
        "member_marital_status_code": _lookup_code(
            enrollee.find("memberMaritalStatusLkp")
        ),
        "member_citizenship_status_code": _lookup_code(
            enrollee.find("memberCitizenshipStatusLkp")
        ),
        "member_tobacco_usage_code": _lookup_code(
            enrollee.find("memberTobaccoUsageLkp")
        ),
        "city": _text(address.find("city")) if address is not None else None,
        "state": _text(address.find("state")) if address is not None else None,
        "zip": _text(address.find("zip")) if address is not None else None,
        "member_birth_date": _text(enrollee.find("memberBirthDate")),
        # Coverage fields
        "maintenance_type_code": None,
        "insurance_type_code": None,
        "benefit_effective_begin_date": None,
        "last_premium_paid_date": None,
        "household_or_employee_case_id": None,
        "class_of_contract_code": None,
        "health_coverage_policy_no": None,
        # Reporting / premium fields
        "aptc_amt": None,
        "health_coverage_premium_amt": None,
        "rating_area": None,
        "total_indiv_responsibility_amt": None,
        "total_premium_amt": None,
        "source_exchg_id": None,
        "additional_maint_reason_code": None,
        # PII — captured for optional export; masked downstream by default
        "member_first_name": _text(enrollee.find("memberFirstName")),
        "member_last_name": _text(enrollee.find("memberLastName")),
        "member_ssn": _text(enrollee.find("memberSSN")),
        "member_primary_phone_no": _text(enrollee.find("memberPrimaryPhoneNo")),
        "member_preferred_email": _text(enrollee.find("memberPreferredEmail")),
        "member_full_address": None,
    }

    events = enrollee.find("enrollmentEvents")
    if events is not None:
        row["event_type_code"] = _lookup_code(events.find("eventTypeLkp"))
        row["event_reason_code"] = _lookup_code(events.find("eventReasonLookUp"))

    if health is not None:
        row["maintenance_type_code"] = _lookup_code(
            health.find("maintenanceTypeCode")
        )
        row["insurance_type_code"] = _lookup_code(health.find("insuranceTypeLkp"))
        row["benefit_effective_begin_date"] = _text(
            health.find("benefitEffectiveBeginDate")
        )
        row["last_premium_paid_date"] = _text(health.find("lastPremiumPaidDate"))
        row["household_or_employee_case_id"] = _text(
            health.find("householdOrEmployeeCaseID")
        )
        row["class_of_contract_code"] = _text(health.find("classOfContractCode"))
        row["health_coverage_policy_no"] = _text(
            health.find("healthCoveragePolicyNo")
        )

    if reporting is not None:
        row["aptc_amt"] = _text(reporting.find("aptcAmt"))
        row["health_coverage_premium_amt"] = _text(
            reporting.find("healthCoveragePremiumAmt")
        )
        row["rating_area"] = _text(reporting.find("ratingArea"))
        row["total_indiv_responsibility_amt"] = _text(
            reporting.find("totalIndivResponsibilityAmt")
        )
        row["total_premium_amt"] = _text(reporting.find("totalPremiumAmt"))
        row["source_exchg_id"] = _text(reporting.find("sourceExchgID"))
        row["additional_maint_reason_code"] = _lookup_code(
            reporting.find("additionalMaintReason")
        )

    if address is not None:
        parts = [
            _text(address.find("city")),
            _text(address.find("state")),
            _text(address.find("zip")),
        ]
        row["member_full_address"] = ", ".join(p for p in parts if p)

    return row


class Xml834Parser:
    """
    Parse 834 enrollment XML into a list of flat enrollee row dictionaries.

    Designed to be called once per file; malformed XML raises ``ET.ParseError``
    so the orchestrator can log and continue with remaining files.
    """

    def parse(
        self,
        xml_bytes: bytes,
        source_file: str,
        issuer_id: str,
    ) -> list[dict[str, Any]]:
        """
        Parse XML bytes into normalized enrollee rows.

        Args:
            xml_bytes: Raw XML content from the extract stage.
            source_file: Original filename for traceability.
            issuer_id: Issuer ID from folder name (fallback: filename).

        Returns:
            List of dicts — one per enrollee across all enrollments.
        """
        root = ET.fromstring(xml_bytes)
        rows: list[dict[str, Any]] = []

        resolved_issuer = issuer_id or parse_issuer_from_filename(source_file) or ""

        for enrollment in root.findall("enrollment"):
            header = _parse_enrollment_header(enrollment)
            enrollees = enrollment.findall("enrollee")

            for enrollee in enrollees:
                row = _parse_enrollee(enrollee)
                row.update(header)
                row["source_file"] = source_file
                row["issuer_id"] = resolved_issuer
                rows.append(row)

        logger.info(
            "Parsed %d enrollee row(s) from %s (issuer %s)",
            len(rows),
            source_file,
            resolved_issuer,
        )
        return rows
