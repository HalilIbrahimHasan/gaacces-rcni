"""Header-name mapping from the 19-column RCNI contract to dbo.rcni_raw.

Mapping is by header name, never by ordinal alone. SQL schema is not altered
when a new header appears.
"""

from __future__ import annotations

from rcni.constants import EXPECTED_COLUMN_COUNT, EXPECTED_HEADER

SOURCE_TO_SQL: dict[str, str] = {
    "Exchange Assigned Policy ID": "exchange_assigned_policy_id",
    "Plan ID": "plan_id",
    "Member Last Name": "member_last_name",
    "Member First Name": "member_first_name",
    "Exchange Assigned Member ID": "exchange_assigned_member_id",
    "Issuer Assigned Member ID": "issuer_assigned_member_id",
    "Subscriber Last Name": "subscriber_last_name",
    "Subscriber First Name": "subscriber_first_name",
    "Exchange Assigned Subscriber ID": "exchange_assigned_subscriber_id",
    "Issuer Assigned Subscriber ID": "issuer_assigned_subscriber_id",
    "Discrepancy Reason Code": "discrepancy_reason_code",
    "Discrepancy Reason Text": "discrepancy_reason_text",
    "HIX Value": "hix_value",
    "Issuer Value": "issuer_value",
    "Date of Discrepancy": "date_of_discrepancy",
    "Recon File Name": "recon_file_name",
    "Autofixed by HIX": "autofixed_by_hix",
    "Assignee": "assignee",
    "Enrollment Status": "enrollment_status",
}

SQL_SOURCE_COLUMNS: tuple[str, ...] = tuple(
    SOURCE_TO_SQL[name] for name in EXPECTED_HEADER
)

STAGE_AND_RAW_LINEAGE_COLUMNS: tuple[str, ...] = (
    "load_run_id",
    "file_hash",
    "issuer_id",
    "coverage_year",
    "processing_year",
    "processing_month",
    "processing_day",
    "file_timestamp",
    "source_file",
    "source_path",
    "row_number_in_file",
    "quality_status",
)

STAGE_INSERT_COLUMNS: tuple[str, ...] = STAGE_AND_RAW_LINEAGE_COLUMNS + SQL_SOURCE_COLUMNS

RAW_INSERT_COLUMNS: tuple[str, ...] = STAGE_AND_RAW_LINEAGE_COLUMNS + ("loaded_at",) + SQL_SOURCE_COLUMNS


def header_mapping(header: tuple[str, ...]) -> tuple[dict[str, int], bool, str | None]:
    """
    Return (name → index, mapping_safe, drift_reason).

    Safe when the header contains exactly the 19 expected names, uniquely.
    Column order may differ; values are read by name.
    """
    if any(name == "" or name is None for name in header):
        return {}, False, "Header contains empty column names"
    if len(header) != len(set(header)):
        return {}, False, "Header contains duplicate column names"
    observed = set(header)
    expected = set(EXPECTED_HEADER)
    if observed != expected:
        missing = [name for name in EXPECTED_HEADER if name not in observed]
        extra = [name for name in header if name not in expected]
        parts = []
        if missing:
            parts.append(f"missing {missing}")
        if extra:
            parts.append(f"unexpected {extra}")
        if len(header) != EXPECTED_COLUMN_COUNT:
            parts.append(
                f"count {len(header)} != {EXPECTED_COLUMN_COUNT}"
            )
        return {}, False, "; ".join(parts) or "Header does not match RCNI contract"
    return {name: header.index(name) for name in EXPECTED_HEADER}, True, None
