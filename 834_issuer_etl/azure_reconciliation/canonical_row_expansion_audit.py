"""
Read-only audit: raw XML row → canonical row cardinality.

Traces each parsed XML row through canonical generation to detect expansion.
Does not modify parser, canonical mapping, or downstream pipeline logic.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.lifecycle_snapshot_comparison import (
    _filter_partitions,
    enrich_month_bases,
)
from azure_reconciliation.partition_discovery import Partition, discover_partitions
from azure_reconciliation.record_comparison import (
    PRIMARY_JOIN,
    build_canonical_xml_records,
    join_key_series,
)
from azure_reconciliation.safe_export import safe_write_excel
from azure_reconciliation.xml_business_reports import apply_business_month_basis
from azure_reconciliation.xml_loader import load_xml_rows
from config.config import settings
from ingestion.file_discovery import discover_source_files
from ingestion.xml_reader import read_xml_bytes
from parsers.parser_834 import Parser834
from utils.logger import get_logger

logger = get_logger(__name__)


def _debug_dir() -> Path:
    d = settings.outputs_path / "debug"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _zmonth(m: str) -> str:
    return str(m).strip().zfill(2)


def _maintenance_code(row: pd.Series) -> str:
    for col in ("maintenance_type_code", "action_code", "enrollment_action_code"):
        if col in row.index:
            val = str(row.get(col, "") or "").strip()
            if val:
                return val
    return ""


def _raw_status(row: pd.Series) -> str:
    for col in (
        "additional_maint_reason_code",
        "coverage_status",
        "action_code_description",
        "enrollee_event_type_code",
    ):
        if col in row.index:
            val = str(row.get(col, "") or "").strip()
            if val:
                return val
    return ""


def _assign_source_row_ids(xml_raw: pd.DataFrame) -> pd.DataFrame:
    work = xml_raw.copy().reset_index(drop=True)
    ids: list[str] = []
    for i, row in work.iterrows():
        fn = str(row.get("file_name", "") or row.get("raw_xml_path", "") or "unknown")
        iss = str(row.get("issuer", ""))
        yr = str(row.get("year", ""))
        mo = _zmonth(str(row.get("month", "")))
        ids.append(f"{iss}|{yr}|{mo}|{fn}|{i}")
    work["source_row_id"] = ids
    return work


def _count_xml_enrollees(xml_bytes: bytes) -> tuple[int, int]:
    root = ET.fromstring(xml_bytes)
    enrollments = root.findall("enrollment")
    enrollees = sum(len(e.findall("enrollee")) for e in enrollments)
    return len(enrollments), enrollees


def _parser_file_audit(issuer: str) -> pd.DataFrame:
    """Compare XML enrollee elements vs parser output rows per file."""
    parser = Parser834()
    rows: list[dict[str, Any]] = []
    sources = discover_source_files(settings.source_data_path, issuer_filter=issuer)
    for src in sources:
        try:
            xml_bytes = read_xml_bytes(src)
            enroll_n, enrollee_n = _count_xml_enrollees(xml_bytes)
            records = parser.parse_file(
                xml_bytes,
                issuer=src.issuer,
                year=src.year,
                month=src.month,
                file_name=src.file_name,
                file_path=str(src.file_path),
            )
            rows.append({
                "source_file": src.file_name,
                "issuer": src.issuer,
                "source_year": src.year,
                "source_month": _zmonth(src.month),
                "xml_enrollment_elements": enroll_n,
                "xml_enrollee_elements": enrollee_n,
                "parser_rows_emitted": len(records),
                "parser_minus_xml_enrollees": len(records) - enrollee_n,
            })
        except Exception as exc:
            rows.append({
                "source_file": src.file_name,
                "issuer": src.issuer,
                "source_year": src.year,
                "source_month": _zmonth(src.month),
                "xml_enrollment_elements": 0,
                "xml_enrollee_elements": 0,
                "parser_rows_emitted": 0,
                "parser_minus_xml_enrollees": 0,
                "error": str(exc),
            })
    return pd.DataFrame(rows)


def _stage_cardinality(stage_df: pd.DataFrame, stage_name: str) -> pd.DataFrame:
    if stage_df.empty or "source_row_id" not in stage_df.columns:
        return pd.DataFrame(columns=["stage", "source_row_id", "row_count"])
    counts = (
        stage_df.groupby("source_row_id", dropna=False)
        .size()
        .reset_index(name="row_count")
    )
    counts["stage"] = stage_name
    return counts


def _classify_expansion(group: pd.DataFrame) -> str:
    if len(group) <= 1:
        return "one_to_one_mapping"
    members = group.get("member_id", pd.Series(dtype=str)).astype(str).nunique()
    policies = group.get("policy_id", pd.Series(dtype=str)).astype(str).nunique()
    ins = group.get("insurance_type", pd.Series(dtype=str)).astype(str).nunique()
    if members > 1 and policies <= 1:
        return "multiple_member_segments"
    if policies > 1 and members <= 1:
        return "subscriber_dependent_split"
    if ins > 1:
        return "coverage_component_split"
    keys = join_key_series(group, list(PRIMARY_JOIN))
    if keys.nunique() < len(group):
        return "duplicate_loop"
    return "unknown_expansion"


def _run_canonical_pipeline(
    xml_tagged: pd.DataFrame,
    partitions: list[Partition],
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    """Mirror build_enriched_canonical_xml + apply_business_month_basis with source_row_id."""
    if xml_tagged.empty:
        return pd.DataFrame(), {}

    work = xml_tagged.reset_index(drop=True)
    ids = work["source_row_id"]

    base = build_canonical_xml_records(work)
    base["source_row_id"] = ids.values

    enriched = enrich_month_bases(base)
    enriched["source_row_id"] = base["source_row_id"].values

    filtered = _filter_partitions(enriched, partitions)
    if not filtered.empty:
        filtered = filtered.copy()
        filtered["source_row_id"] = enriched.loc[filtered.index, "source_row_id"].values

    if filtered.empty:
        return pd.DataFrame(), {
            "build_canonical_xml_records": base,
            "enrich_month_bases": enriched,
            "filter_partitions": filtered,
        }

    business, _ = apply_business_month_basis(filtered)
    business["source_row_id"] = filtered["source_row_id"].values
    if "_record_key" not in business.columns:
        business["_record_key"] = join_key_series(business, list(PRIMARY_JOIN))

    stages = {
        "build_canonical_xml_records": base,
        "enrich_month_bases": enriched,
        "filter_partitions": filtered,
        "apply_business_month_basis": business,
    }
    return business, stages


def _raw_summary_row(
    raw_row: pd.Series,
    sid: str,
    canon_grp: pd.DataFrame,
    *,
    reason: str,
    keys: list[str],
) -> dict[str, Any]:
    n = len(canon_grp)
    src_y = str(raw_row.get("year", ""))
    src_m = _zmonth(str(raw_row.get("month", "")))
    biz_y = str(canon_grp.iloc[0].get("year", "")) if n else ""
    biz_m = _zmonth(str(canon_grp.iloc[0].get("month", ""))) if n else ""

    if n == 1 and (src_y != biz_y or src_m != biz_m):
        reason = "month_basis_scope_outbound"
    if n == 0:
        reason = "filtered_out"

    return {
        "source_row_id": sid,
        "source_file": str(raw_row.get("file_name", "") or raw_row.get("raw_xml_path", "")),
        "issuer": str(raw_row.get("issuer", "")),
        "policy_id": str(raw_row.get("policy_id", "")),
        "member_id": str(raw_row.get("member_id", "")),
        "subscriber_id": str(raw_row.get("subscriber_id", "")),
        "insurance_type": str(raw_row.get("insurance_type_code", raw_row.get("insurance_type", ""))),
        "raw_status": _raw_status(raw_row),
        "canonical_status": str(canon_grp.iloc[0].get("normalized_status", "")) if n else "",
        "maintenance_code": _maintenance_code(raw_row),
        "benefit_effective_date": str(raw_row.get("benefit_effective_date", "") or ""),
        "benefit_end_date": str(raw_row.get("benefit_end_date", "") or ""),
        "member_maint_effective_date": str(raw_row.get("member_maint_effective_date", "") or ""),
        "source_folder_year": src_y,
        "source_folder_month": src_m,
        "business_year": biz_y,
        "business_month": biz_m,
        "canonical_rows_generated": n,
        "generated_canonical_keys": "; ".join(keys),
        "expansion_reason": reason,
    }


def _build_per_raw_summary(
    xml_tagged: pd.DataFrame,
    canonical_business: pd.DataFrame,
) -> pd.DataFrame:
    grouped = (
        canonical_business.groupby("source_row_id", dropna=False)
        if not canonical_business.empty and "source_row_id" in canonical_business.columns
        else None
    )
    rows: list[dict[str, Any]] = []
    for _, raw_row in xml_tagged.iterrows():
        sid = str(raw_row["source_row_id"])
        if grouped is not None and sid in grouped.groups:
            canon_grp = canonical_business[
                canonical_business["source_row_id"].astype(str) == sid
            ]
            reason = _classify_expansion(canon_grp)
            keys = canon_grp["_record_key"].astype(str).tolist() if "_record_key" in canon_grp.columns else []
        else:
            canon_grp = pd.DataFrame()
            reason = "filtered_out"
            keys = []
        rows.append(_raw_summary_row(raw_row, sid, canon_grp, reason=reason, keys=keys))
    return pd.DataFrame(rows)


def _scope_detail(
    xml_tagged: pd.DataFrame,
    canonical_business: pd.DataFrame,
    per_raw: pd.DataFrame,
    *,
    issuer: str,
    year: str,
    month: str,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    zm = _zmonth(month)
    raw_scope = xml_tagged[
        (xml_tagged["issuer"].astype(str) == str(issuer))
        & (xml_tagged["year"].astype(str) == str(year))
        & (xml_tagged["month"].astype(str).map(_zmonth) == zm)
    ].copy()

    canon_scope = canonical_business[
        (canonical_business["issuer"].astype(str) == str(issuer))
        & (canonical_business["year"].astype(str) == str(year))
        & (canonical_business["month"].astype(str).map(_zmonth) == zm)
    ].copy() if not canonical_business.empty else pd.DataFrame()

    raw_ids = set(raw_scope["source_row_id"].astype(str))
    canon_ids = set(canon_scope["source_row_id"].astype(str)) if not canon_scope.empty else set()
    inbound_ids = canon_ids - raw_ids
    outbound_ids = raw_ids - canon_ids

    inbound_detail = canon_scope[
        canon_scope["source_row_id"].astype(str).isin(inbound_ids)
    ].copy() if inbound_ids and not canon_scope.empty else pd.DataFrame()

    outbound_detail = per_raw[
        per_raw["source_row_id"].astype(str).isin(outbound_ids)
    ].copy() if outbound_ids and not per_raw.empty else pd.DataFrame()

    scope = {
        "raw_rows_source_folder_month": len(raw_scope),
        "canonical_rows_business_month": len(canon_scope),
        "count_delta_canonical_minus_raw": len(canon_scope) - len(raw_scope),
        "canonical_from_other_source_months": len(inbound_ids),
        "raw_source_rows_not_in_business_month": len(outbound_ids),
        "explanation": (
            "Lineage gap is scope mismatch: step 1_raw_xml_rows filters source-folder "
            "year/month; step 2_canonical_records filters business year/month after "
            "apply_business_month_basis. Canonical generation is 1:1 per raw row."
            if inbound_ids or outbound_ids else
            "Source-folder month and business month scopes align for this partition."
        ),
    }
    return inbound_detail, outbound_detail, scope


def build_canonical_expansion_audit(
    *,
    issuer: str,
    year: str,
    month: str,
    parse_source: bool = False,
) -> dict[str, Any]:
    zm = _zmonth(month)
    partitions = discover_partitions(settings.source_data_path, issuer_filter=issuer)
    xml_raw = load_xml_rows(prefer_staging=not parse_source, issuer_filter=issuer)
    if xml_raw.empty:
        raise RuntimeError(f"No XML rows for issuer {issuer}")

    xml_tagged = _assign_source_row_ids(xml_raw)
    parser_audit = _parser_file_audit(issuer)

    canonical_business, stages = _run_canonical_pipeline(xml_tagged, partitions)
    per_raw = _build_per_raw_summary(xml_tagged, canonical_business)

    stage_cardinality = pd.concat(
        [_stage_cardinality(df, name) for name, df in stages.items()],
        ignore_index=True,
    ) if stages else pd.DataFrame()

    inbound_detail, outbound_detail, scope = _scope_detail(
        xml_tagged, canonical_business, per_raw,
        issuer=issuer, year=year, month=zm,
    )

    expanded = per_raw[per_raw["canonical_rows_generated"] > 1]
    reason_counts = expanded["expansion_reason"].value_counts().to_dict() if not expanded.empty else {}

    stage_expansions = (
        stage_cardinality[stage_cardinality["row_count"] > 1]
        if not stage_cardinality.empty else pd.DataFrame()
    )

    summary = {
        "issuer": issuer,
        "year": year,
        "month": zm,
        "total_raw_rows": len(xml_tagged),
        "total_canonical_rows_after_pipeline": len(canonical_business),
        "expansion_count": int((per_raw["canonical_rows_generated"] > 1).sum()),
        "rows_with_expansion": len(expanded),
        "max_canonical_rows_per_raw_row": int(per_raw["canonical_rows_generated"].max()) if not per_raw.empty else 0,
        "expansion_reason_counts": reason_counts,
        "parser_files_with_extra_rows": int((parser_audit["parser_minus_xml_enrollees"] > 0).sum())
        if not parser_audit.empty else 0,
        "stage_rows_with_expansion": len(stage_expansions),
        **scope,
    }

    return {
        "per_raw": per_raw,
        "stage_cardinality": stage_cardinality,
        "stage_expansions": stage_expansions,
        "inbound_detail": inbound_detail,
        "outbound_detail": outbound_detail,
        "parser_audit": parser_audit,
        "scope": scope,
        "summary": summary,
    }


def write_canonical_expansion_audit(
    *,
    issuer: str,
    year: str,
    month: str,
    parse_source: bool = False,
) -> tuple[Path, Path]:
    zm = _zmonth(month)
    tag = f"{issuer}_{year}_{zm}"
    result = build_canonical_expansion_audit(
        issuer=issuer, year=year, month=month, parse_source=parse_source,
    )

    xlsx_path = _debug_dir() / f"canonical_row_expansion_{tag}.xlsx"
    safe_write_excel(
        xlsx_path,
        {
            "per_raw_row": result["per_raw"],
            "stage_cardinality": result["stage_cardinality"],
            "stage_expansions": result["stage_expansions"],
            "inbound_business_month": result["inbound_detail"],
            "outbound_source_month": result["outbound_detail"],
            "parser_file_audit": result["parser_audit"],
            "summary": pd.DataFrame([result["summary"]]),
        },
        drop_duplicate_value_columns=False,
    )

    md_path = _debug_dir() / f"canonical_row_expansion_{tag}.md"
    s = result["summary"]
    generated = datetime.now(timezone.utc).isoformat(timespec="seconds")
    lines = [
        f"# Canonical Row Expansion Audit — {issuer} / {year} / {zm}",
        "",
        f"**Generated:** {generated}",
        "",
        "## Summary",
        "",
        f"- Total raw XML rows (issuer): {s['total_raw_rows']}",
        f"- Raw rows in source folder {year}/{zm}: {s['raw_rows_source_folder_month']}",
        f"- Canonical rows in business month {year}/{zm}: {s['canonical_rows_business_month']}",
        f"- Count delta (canonical − raw source month): {s['count_delta_canonical_minus_raw']}",
        f"- Canonical from other source months (inbound to business month): {s['canonical_from_other_source_months']}",
        f"- Raw source rows not in business month (outbound): {s['raw_source_rows_not_in_business_month']}",
        "",
        f"- Rows with canonical_rows_generated > 1: {s['rows_with_expansion']}",
        f"- Max canonical rows per raw row: {s['max_canonical_rows_per_raw_row']}",
        f"- Stage-level row multiplications: {s['stage_rows_with_expansion']}",
        f"- Parser files emitting more rows than XML enrollees: {s['parser_files_with_extra_rows']}",
        "",
        "## Scope explanation",
        "",
        str(s.get("explanation", "")),
        "",
        "## Expansion reason counts (raw rows with >1 canonical)",
        "",
    ]
    if s.get("expansion_reason_counts"):
        for reason, count in sorted(s["expansion_reason_counts"].items(), key=lambda x: (-x[1], x[0])):
            lines.append(f"- {reason}: {count}")
    else:
        lines.append("- (none — canonical generation is 1:1 per raw row through all stages)")
    lines.extend([
        "",
        "## Conclusion",
        "",
    ])
    if (
        s["rows_with_expansion"] == 0
        and s["stage_rows_with_expansion"] == 0
        and s["parser_files_with_extra_rows"] == 0
    ):
        lines.append(
            "No parser or canonical-stage row multiplication detected. "
            f"The lineage gap of {s['count_delta_canonical_minus_raw']:+d} for {year}/{zm} is explained by "
            "**source-folder month** vs **business-month** filtering in `run_lineage_audit.py`, "
            "not by row expansion in canonical generation."
        )
    else:
        lines.append(
            "Row multiplication detected — inspect `per_raw_row`, `stage_expansions`, "
            "and `parser_file_audit` sheets before applying any fix."
        )
    lines.append("")
    md_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Wrote canonical expansion audit → %s, %s", xlsx_path, md_path)
    return xlsx_path, md_path
