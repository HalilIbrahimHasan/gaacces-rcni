"""
Read-only full data export layer — raw parsed XML + cleaned canonical records.

Does not modify pipeline logic, parsers, cleanup, lifecycle, or Model H.
"""

from __future__ import annotations

import html
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.chandra_business_format import (
    CHANDRA_BUSINESS_COLUMNS_CORE,
    to_chandra_business_summary,
)
from azure_reconciliation.dashboard_difference_analysis import _enrollment_id_series
from azure_reconciliation.partition_discovery import discover_partitions
from azure_reconciliation.record_comparison import join_key_series
from azure_reconciliation.safe_export import ExportErrors, safe_dataframe_for_export, safe_write_csv
from azure_reconciliation.xml_business_reports import (
    PK,
    _dedupe_transactions,
    _latest_state_per_business_month,
    process_issuer_xml_business,
)
from azure_reconciliation.xml_loader import load_xml_rows
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

FULL_EXPORT_EXCEL_MAX_ROWS = 1_000_000

RAW_META_COLUMNS = [
    "issuer",
    "source_year",
    "source_month",
    "source_file",
    "source_folder",
    "parsed_row_number",
]

BUSINESS_FIELD_CANDIDATES: dict[str, list[str]] = {
    "Enrollment_ID": [
        "policy_id",
        "exchg_assigned_policy_id",
        "health_coverage_policy_no",
        "enrollment_id",
        "household_or_employee_case_id",
    ],
    "Enrollee_ID": [
        "member_id",
        "exchg_assigned_enrollee_id",
        "exchg_indiv_identifier",
        "enrollee_id",
    ],
    "Subscriber_ID": ["subscriber_id", "exchg_subscriber_identifier"],
    "Insurance_Type": ["insurance_type", "insurance_type_code"],
    "Status": [
        "canonical_status",
        "normalized_status",
        "status",
        "coverage_status",
        "enrollee_event_type_code",
    ],
    "Maintenance_Code": [
        "maintenance_type_code",
        "action_code",
        "enrollment_action_code",
        "additional_maint_reason_code",
    ],
    "Benefit_Effective_Date": ["benefit_effective_date", "benefit_effective_begin_date"],
    "Benefit_End_Date": ["benefit_end_date"],
    "Member_Maintenance_Effective_Date": ["member_maint_effective_date"],
    "Business_Month": ["month", "business_month", "snapshot_month"],
}


def export_root() -> Path:
    return settings.outputs_path / "full_data_exports"


def _zmonth(m: Any) -> str:
    return str(m).strip().zfill(2)


def _sample_values(series: pd.Series, n: int = 5) -> str:
    vals = series.dropna().astype(str).head(n).tolist()
    return " | ".join(vals)


def column_profile(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["column_name", "non_null_count", "distinct_count", "sample_values"])
    rows = []
    for col in df.columns:
        s = df[col]
        rows.append({
            "column_name": str(col),
            "non_null_count": int(s.notna().sum()),
            "distinct_count": int(s.nunique(dropna=True)),
            "sample_values": _sample_values(s),
        })
    return pd.DataFrame(rows)


def discover_issuer_year_pairs(
    *,
    issuer_filter: str | None = None,
    year_filter: str | None = None,
) -> list[tuple[str, str]]:
    parts = discover_partitions(
        issuer_filter=issuer_filter,
        year_filter=year_filter,
    )
    if not parts:
        return []
    return sorted({(p.issuer, p.year) for p in parts})


def _prepare_raw_export(xml_raw: pd.DataFrame, issuer: str, year: str) -> pd.DataFrame:
    """Parsed XML as-is with metadata columns prepended (no renaming of original columns)."""
    if xml_raw.empty:
        out = pd.DataFrame(columns=RAW_META_COLUMNS)
        return out

    work = xml_raw.copy()
    work = work[work["year"].astype(str) == str(year)] if "year" in work.columns else work

    # source metadata
    if "issuer" not in work.columns:
        work["issuer"] = issuer
    work["source_year"] = work["year"].astype(str) if "year" in work.columns else str(year)
    work["source_month"] = work["month"].astype(str).map(_zmonth) if "month" in work.columns else ""
    if "source_file" not in work.columns:
        if "file_name" in work.columns:
            work["source_file"] = work["file_name"]
        elif "raw_xml_path" in work.columns:
            work["source_file"] = work["raw_xml_path"].astype(str).map(lambda p: Path(p).name)
        else:
            work["source_file"] = ""
    work["source_folder"] = work["source_year"].astype(str) + "/" + work["source_month"].astype(str)
    work["parsed_row_number"] = range(1, len(work) + 1)

    meta = [c for c in RAW_META_COLUMNS if c in work.columns]
    original = [c for c in work.columns if c not in meta]
    return work[meta + original]


def _distinct_id_count(df: pd.DataFrame, col: str) -> int | None:
    if col not in df.columns or df.empty:
        return None
    s = df[col].astype(str).str.strip()
    s = s[s != ""]
    return int(s.nunique()) if len(s) else 0


def raw_counts_by_month(raw: pd.DataFrame, issuer: str, year: str) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=[
            "issuer", "year", "month", "raw_record_count",
            "distinct_policy_count", "distinct_member_count", "distinct_subscriber_count",
            "source_file_count",
        ])
    work = raw.copy()
    work["_month"] = work["source_month"].astype(str).map(_zmonth)
    rows = []
    for month, grp in work.groupby("_month", dropna=False):
        rows.append({
            "issuer": issuer,
            "year": year,
            "month": month,
            "raw_record_count": len(grp),
            "distinct_policy_count": _distinct_id_count(grp, "policy_id"),
            "distinct_member_count": _distinct_id_count(grp, "member_id"),
            "distinct_subscriber_count": _distinct_id_count(grp, "subscriber_id"),
            "source_file_count": int(grp["source_file"].astype(str).nunique()) if "source_file" in grp.columns else None,
        })
    return pd.DataFrame(rows).sort_values(["year", "month"]).reset_index(drop=True)


def raw_counts_by_file(raw: pd.DataFrame, issuer: str, year: str) -> pd.DataFrame:
    if raw.empty or "source_file" not in raw.columns:
        return pd.DataFrame(columns=["issuer", "year", "month", "source_file", "raw_record_count"])
    work = raw.copy()
    work["_month"] = work["source_month"].astype(str).map(_zmonth)
    rows = []
    for (month, src), grp in work.groupby(["_month", "source_file"], dropna=False):
        rows.append({
            "issuer": issuer,
            "year": year,
            "month": month,
            "source_file": src,
            "raw_record_count": len(grp),
        })
    return pd.DataFrame(rows).sort_values(["month", "source_file"]).reset_index(drop=True)


def _attach_source_metadata(canonical: pd.DataFrame, xml_raw: pd.DataFrame) -> pd.DataFrame:
    """Merge source folder year/month/file from parsed XML onto canonical rows."""
    cleaned = canonical.copy()
    if xml_raw.empty:
        cleaned["source_year"] = ""
        cleaned["source_month"] = ""
        cleaned["source_file"] = ""
        cleaned["source_folder"] = ""
        return cleaned

    raw = xml_raw.copy()
    raw["source_year"] = raw["year"].astype(str) if "year" in raw.columns else ""
    raw["source_month"] = raw["month"].astype(str).map(_zmonth) if "month" in raw.columns else ""
    if "file_name" in raw.columns:
        raw["source_file"] = raw["file_name"]
    elif "raw_xml_path" in raw.columns:
        raw["source_file"] = raw["raw_xml_path"].astype(str).map(lambda p: Path(p).name)
    else:
        raw["source_file"] = ""

    merge_keys = [
        c for c in (
            "policy_id", "member_id", "subscriber_id", "file_name",
            "benefit_effective_date", "member_maint_effective_date",
            "maintenance_type_code", "insurance_type_code",
        )
        if c in cleaned.columns and c in raw.columns
    ]
    meta_cols = ["source_year", "source_month", "source_file"]
    if merge_keys:
        raw_sub = raw[merge_keys + meta_cols].drop_duplicates()
        cleaned = cleaned.merge(raw_sub, on=merge_keys, how="left")
    elif len(cleaned) == len(raw):
        for col in meta_cols:
            cleaned[col] = raw[col].values
    else:
        for col in meta_cols:
            cleaned[col] = ""

    cleaned["source_folder"] = (
        cleaned["source_year"].astype(str) + "/" + cleaned["source_month"].astype(str).map(_zmonth)
    )
    cleaned["business_year"] = cleaned["year"].astype(str) if "year" in cleaned.columns else ""
    cleaned["business_month"] = cleaned["month"].astype(str).map(_zmonth) if "month" in cleaned.columns else ""
    return cleaned


def _build_cleaned_export(result: Any, xml_raw: pd.DataFrame, year: str) -> pd.DataFrame:
    """Record-level canonical with normalization columns and cleanup flags."""
    canonical = result.canonical
    if canonical.empty:
        return pd.DataFrame()

    cleaned = _attach_source_metadata(canonical, xml_raw)
    if "source_year" in cleaned.columns:
        by_source = cleaned[cleaned["source_year"].astype(str) == str(year)]
        if not by_source.empty:
            cleaned = by_source.copy()
        elif "business_year" in cleaned.columns:
            cleaned = cleaned[cleaned["business_year"].astype(str) == str(year)].copy()
    elif "business_year" in cleaned.columns:
        cleaned = cleaned[cleaned["business_year"].astype(str) == str(year)].copy()

    cleaned["normalized_enrollment_id"] = _enrollment_id_series(cleaned)
    cleaned["normalized_enrollee_id"] = cleaned["member_id"].astype(str) if "member_id" in cleaned.columns else ""
    if "subscriber_id" in cleaned.columns:
        cleaned["normalized_subscriber_id"] = cleaned["subscriber_id"].astype(str)
    elif "subscriber_id_norm" in cleaned.columns:
        cleaned["normalized_subscriber_id"] = cleaned["subscriber_id_norm"].astype(str)

    dup_idx = set(result.duplicate_df.index)
    maint_idx = set(result.maintenance_df.index)
    sup_idx = set(result.superseded_df.index)
    cleaned["duplicate_flag"] = cleaned.index.isin(dup_idx)
    cleaned["maintenance_only_flag"] = cleaned.index.isin(maint_idx)
    cleaned["superseded_flag"] = cleaned.index.isin(sup_idx)

    deduped = _dedupe_transactions(canonical)
    latest = _latest_state_per_business_month(deduped)
    latest_keys = set(join_key_series(latest, PK + ["year", "month"]))
    row_keys = join_key_series(cleaned, PK + ["year", "month"])
    cleaned["latest_state_flag"] = row_keys.isin(latest_keys)

    li = result.lifecycle_input
    if not li.empty:
        li_keys = set(join_key_series(li, PK + ["year", "month"]))
        cleaned["model_h_inclusion_flag"] = row_keys.isin(li_keys)
        cleaned["final_selected_flag"] = cleaned["model_h_inclusion_flag"]
    else:
        cleaned["model_h_inclusion_flag"] = False
        cleaned["final_selected_flag"] = False

    return cleaned.reset_index(drop=True)


def cleaned_counts_by_month(cleaned: pd.DataFrame, issuer: str, year: str) -> pd.DataFrame:
    cols = [
        "issuer", "year", "month", "cleaned_record_count",
        "distinct_enrollment_count", "distinct_enrollee_count", "distinct_subscriber_count",
        "model_h_input_count",
    ]
    if cleaned.empty:
        return pd.DataFrame(columns=cols)

    work = cleaned.copy()
    work["_month"] = work.get("business_month", work.get("month", pd.Series(dtype=str))).astype(str).map(_zmonth)
    rows = []
    for month, grp in work.groupby("_month", dropna=False):
        mh = int(grp["model_h_inclusion_flag"].sum()) if "model_h_inclusion_flag" in grp.columns else None
        rows.append({
            "issuer": issuer,
            "year": year,
            "month": month,
            "cleaned_record_count": len(grp),
            "distinct_enrollment_count": _distinct_id_count(grp, "normalized_enrollment_id"),
            "distinct_enrollee_count": _distinct_id_count(grp, "normalized_enrollee_id"),
            "distinct_subscriber_count": _distinct_id_count(grp, "normalized_subscriber_id"),
            "model_h_input_count": mh,
        })
    return pd.DataFrame(rows).sort_values(["year", "month"]).reset_index(drop=True)


def cleaned_counts_by_status(cleaned: pd.DataFrame, issuer: str, year: str) -> pd.DataFrame:
    cols = [
        "issuer", "year", "month", "insurance_type", "status",
        "distinct_enrollment_count", "distinct_enrollee_count",
    ]
    if cleaned.empty:
        return pd.DataFrame(columns=cols)

    work = cleaned.copy()
    work["_month"] = work.get("business_month", work.get("month", pd.Series(dtype=str))).astype(str).map(_zmonth)
    work["_status"] = work.get("normalized_status", work.get("canonical_status", work.get("status", ""))).astype(str)
    work["_itype"] = work.get("insurance_type", "").astype(str)
    rows = []
    for (month, itype, status), grp in work.groupby(["_month", "_itype", "_status"], dropna=False):
        rows.append({
            "issuer": issuer,
            "year": year,
            "month": month,
            "insurance_type": itype,
            "status": status,
            "distinct_enrollment_count": _distinct_id_count(grp, "normalized_enrollment_id"),
            "distinct_enrollee_count": _distinct_id_count(grp, "normalized_enrollee_id"),
        })
    return pd.DataFrame(rows).sort_values(["month", "insurance_type", "status"]).reset_index(drop=True)


def cleanup_counts_by_month(
    raw: pd.DataFrame,
    cleaned: pd.DataFrame,
    result: Any,
    issuer: str,
    year: str,
) -> pd.DataFrame:
    cols = [
        "issuer", "year", "month", "raw_count", "canonical_count",
        "duplicate_count", "maintenance_only_count", "superseded_count",
        "latest_state_count", "model_h_group_count",
    ]
    raw_monthly = raw_counts_by_month(raw, issuer, year)
    cleaned_monthly = cleaned_counts_by_month(cleaned, issuer, year)
    months = sorted(set(raw_monthly["month"].astype(str)) | set(cleaned_monthly["month"].astype(str)))
    if not months:
        return pd.DataFrame(columns=cols)

    dup_by_month: dict[str, int] = {}
    if not result.duplicate_df.empty and "month" in result.duplicate_df.columns:
        for m, g in result.duplicate_df.groupby(result.duplicate_df["month"].astype(str).map(_zmonth)):
            dup_by_month[m] = len(g)

    maint_by_month: dict[str, int] = {}
    if not result.maintenance_df.empty and "month" in result.maintenance_df.columns:
        for m, g in result.maintenance_df.groupby(result.maintenance_df["month"].astype(str).map(_zmonth)):
            maint_by_month[m] = len(g)

    sup_by_month: dict[str, int] = {}
    if not result.superseded_df.empty and "month" in result.superseded_df.columns:
        for m, g in result.superseded_df.groupby(result.superseded_df["month"].astype(str).map(_zmonth)):
            sup_by_month[m] = len(g)

    raw_map = {str(r["month"]): int(r["raw_record_count"]) for _, r in raw_monthly.iterrows()}
    canon_map = {str(r["month"]): int(r["cleaned_record_count"]) for _, r in cleaned_monthly.iterrows()}
    latest_map = {}
    mh_map = {}
    if not cleaned.empty:
        work = cleaned.copy()
        work["_month"] = work.get("business_month", work.get("month", "")).astype(str).map(_zmonth)
        for m, g in work.groupby("_month"):
            latest_map[m] = int(g["latest_state_flag"].sum()) if "latest_state_flag" in g.columns else 0
            mh_map[m] = int(g["model_h_inclusion_flag"].sum()) if "model_h_inclusion_flag" in g.columns else 0

    rows = []
    for month in months:
        rows.append({
            "issuer": issuer,
            "year": year,
            "month": month,
            "raw_count": raw_map.get(month, 0),
            "canonical_count": canon_map.get(month, 0),
            "duplicate_count": dup_by_month.get(month, 0),
            "maintenance_only_count": maint_by_month.get(month, 0),
            "superseded_count": sup_by_month.get(month, 0),
            "latest_state_count": latest_map.get(month, 0),
            "model_h_group_count": mh_map.get(month, 0),
        })
    return pd.DataFrame(rows)


def model_h_summary_by_month(result: Any, year: str) -> pd.DataFrame:
    mh = result.model_h_monthly
    if mh.empty:
        return pd.DataFrame(columns=CHANDRA_BUSINESS_COLUMNS_CORE)
    work = mh.copy()
    if "year" in work.columns:
        work = work[work["year"].astype(str) == str(year)]
    if work.empty:
        return pd.DataFrame(columns=CHANDRA_BUSINESS_COLUMNS_CORE)
    return to_chandra_business_summary(work)


def raw_vs_cleaned_reconciliation(
    raw: pd.DataFrame,
    cleaned: pd.DataFrame,
    cleanup: pd.DataFrame,
    issuer: str,
    year: str,
) -> pd.DataFrame:
    raw_m = raw_counts_by_month(raw, issuer, year)
    clean_m = cleaned_counts_by_month(cleaned, issuer, year)
    months = sorted(set(raw_m["month"].astype(str)) | set(clean_m["month"].astype(str)))
    raw_map = {str(r["month"]): int(r["raw_record_count"]) for _, r in raw_m.iterrows()}
    clean_map = {str(r["month"]): int(r["cleaned_record_count"]) for _, r in clean_m.iterrows()}
    cleanup_map: dict[str, pd.Series] = {}
    if not cleanup.empty:
        for _, row in cleanup.iterrows():
            cleanup_map[str(row["month"])] = row

    rows = []
    for month in months:
        raw_c = raw_map.get(month, 0)
        clean_c = clean_map.get(month, 0)
        cu = cleanup_map.get(month)
        latest = int(cu["latest_state_count"]) if cu is not None else 0
        mh = int(cu["model_h_group_count"]) if cu is not None else 0
        canon_c = int(cu["canonical_count"]) if cu is not None else clean_c
        notes = ""
        if raw_c > clean_c:
            notes = "raw uses source-folder month; cleaned uses business-month scope"
        rows.append({
            "issuer": issuer,
            "year": year,
            "month": month,
            "raw_record_count": raw_c,
            "canonical_record_count": canon_c,
            "cleaned_record_count": clean_c,
            "latest_state_count": latest,
            "model_h_group_count": mh,
            "raw_minus_cleaned": raw_c - clean_c,
            "notes": notes,
        })
    return pd.DataFrame(rows)


def column_mapping_audit(xml_raw: pd.DataFrame, cleaned: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for field, candidates in BUSINESS_FIELD_CANDIDATES.items():
        present = [c for c in candidates if c in xml_raw.columns or c in cleaned.columns]
        selected = ""
        profile_df = pd.DataFrame()
        for c in candidates:
            if c in cleaned.columns and cleaned[c].notna().any():
                selected = c
                profile_df = cleaned
                break
            if c in xml_raw.columns and xml_raw[c].notna().any():
                selected = c
                profile_df = xml_raw
                break
        if not selected and present:
            selected = present[0]
            profile_df = cleaned if selected in cleaned.columns else xml_raw
        non_null = distinct = 0
        sample = ""
        if selected and not profile_df.empty and selected in profile_df.columns:
            s = profile_df[selected]
            non_null = int(s.notna().sum())
            distinct = int(s.nunique(dropna=True))
            sample = _sample_values(s)
        rows.append({
            "business_field": field,
            "source_column_candidates": ", ".join(candidates),
            "selected_column": selected,
            "non_null_count": non_null,
            "distinct_count": distinct,
            "sample_values": sample,
        })
    return pd.DataFrame(rows)


def _readme_dataframe(extra_notes: list[str] | None = None) -> pd.DataFrame:
    lines = [
        "RAW_ALL_MONTHS = direct parsed XML records, no cleanup or transformation.",
        "CLEANED_ALL_MONTHS = transformed/canonical/cleaned record-level data.",
        "MODEL_H_SUMMARY_BY_MONTH = final Chandra-like business summary.",
        "Counts are by issuer/year/month.",
        "No data is removed from the raw export.",
    ]
    if extra_notes:
        lines.extend(extra_notes)
    return pd.DataFrame({"README": lines})


def write_full_export_excel(
    output_path: Path,
    sheets: dict[str, pd.DataFrame],
    *,
    csv_fallback_paths: dict[str, Path] | None = None,
    export_errors: ExportErrors | None = None,
) -> list[str]:
    """
    Write Excel workbook; cap sheets at FULL_EXPORT_EXCEL_MAX_ROWS.
    Returns warning messages for truncated sheets.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    warnings: list[str] = []
    excel_sheets: dict[str, pd.DataFrame] = {}

    for name, df in sheets.items():
        if df.empty:
            excel_sheets[name[:31]] = df
            continue
        safe = safe_dataframe_for_export(df, table_name=name, drop_duplicate_value_columns=False)
        if len(safe) > FULL_EXPORT_EXCEL_MAX_ROWS:
            csv_path = (csv_fallback_paths or {}).get(name)
            msg = (
                f"{name}: {len(safe):,} rows exceed Excel limit ({FULL_EXPORT_EXCEL_MAX_ROWS:,}). "
                f"Excel contains first {FULL_EXPORT_EXCEL_MAX_ROWS:,} rows only."
            )
            if csv_path:
                msg += f" Full data: {csv_path}"
            warnings.append(msg)
            excel_sheets[name[:31]] = safe.head(FULL_EXPORT_EXCEL_MAX_ROWS)
        else:
            excel_sheets[name[:31]] = safe

    if warnings:
        readme = sheets.get("README")
        extra = list(readme["README"]) if readme is not None and "README" in readme.columns else []
        excel_sheets["README"] = _readme_dataframe(warnings + extra)

    try:
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            for name, df in excel_sheets.items():
                safe_name = name[:31]
                if df.empty:
                    pd.DataFrame().to_excel(writer, sheet_name=safe_name, index=False)
                else:
                    safe_dataframe_for_export(
                        df, table_name=f"excel:{name}", drop_duplicate_value_columns=False,
                    ).to_excel(writer, sheet_name=safe_name, index=False)
        logger.info("Wrote full export Excel: %s (%d sheets)", output_path, len(excel_sheets))
    except Exception as exc:
        msg = f"Full export Excel failed for {output_path}: {exc}"
        logger.error(msg)
        if export_errors:
            export_errors.record(msg)
    return warnings


@dataclass
class IssuerYearExportResult:
    issuer: str
    year: str
    raw_record_count: int = 0
    cleaned_record_count: int = 0
    raw_excel_path: Path | None = None
    cleaned_excel_path: Path | None = None
    combined_review_path: Path | None = None
    monthly_counts: dict[str, Any] = field(default_factory=dict)


def export_issuer_year(
    issuer: str,
    year: str,
    *,
    parse_source: bool = False,
    export_errors: ExportErrors | None = None,
) -> IssuerYearExportResult:
    """Export raw + cleaned + combined review for one issuer/year."""
    logger.info("Full data export: %s / %s", issuer, year)
    out = IssuerYearExportResult(issuer=issuer, year=year)
    base = export_root() / issuer / year

    partitions = [
        p for p in discover_partitions(issuer_filter=issuer, year_filter=year)
    ]
    xml_raw = load_xml_rows(
        prefer_staging=not parse_source,
        issuer_filter=issuer,
        year_filter=year,
    )
    raw_df = _prepare_raw_export(xml_raw, issuer, year)

    result = process_issuer_xml_business(issuer, xml_raw, partitions)
    cleaned_df = _build_cleaned_export(result, xml_raw, year)

    out.raw_record_count = len(raw_df)
    out.cleaned_record_count = len(cleaned_df)

    raw_counts_m = raw_counts_by_month(raw_df, issuer, year)
    raw_counts_f = raw_counts_by_file(raw_df, issuer, year)
    raw_profile = column_profile(raw_df)

    cleaned_counts_m = cleaned_counts_by_month(cleaned_df, issuer, year)
    cleaned_counts_s = cleaned_counts_by_status(cleaned_df, issuer, year)
    cleanup_counts = cleanup_counts_by_month(raw_df, cleaned_df, result, issuer, year)
    model_h_summary = model_h_summary_by_month(result, year)
    cleaned_profile = column_profile(cleaned_df)
    reconciliation = raw_vs_cleaned_reconciliation(raw_df, cleaned_df, cleanup_counts, issuer, year)
    mapping_audit = column_mapping_audit(xml_raw, cleaned_df)

    # --- raw bundle ---
    raw_dir = base / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_xlsx = raw_dir / "raw_all_months.xlsx"
    raw_csv = raw_dir / "raw_all_months.csv"
    safe_write_csv(raw_csv, raw_df, table_name="RAW_ALL_MONTHS", export_errors=export_errors)
    write_full_export_excel(
        raw_xlsx,
        {
            "RAW_ALL_MONTHS": raw_df,
            "RAW_COUNTS_BY_MONTH": raw_counts_m,
            "RAW_COUNTS_BY_FILE": raw_counts_f,
            "RAW_COLUMN_PROFILE": raw_profile,
        },
        csv_fallback_paths={"RAW_ALL_MONTHS": raw_csv},
        export_errors=export_errors,
    )
    raw_monthly_xlsx = raw_dir / "raw_monthly_counts.xlsx"
    raw_monthly_csv = raw_dir / "raw_monthly_counts.csv"
    safe_write_csv(raw_monthly_csv, raw_counts_m, table_name="RAW_COUNTS_BY_MONTH", export_errors=export_errors)
    write_full_export_excel(
        raw_monthly_xlsx,
        {"RAW_COUNTS_BY_MONTH": raw_counts_m},
        export_errors=export_errors,
    )
    out.raw_excel_path = raw_xlsx

    # --- cleaned bundle ---
    cleaned_dir = base / "cleaned"
    cleaned_dir.mkdir(parents=True, exist_ok=True)
    cleaned_xlsx = cleaned_dir / "cleaned_all_months.xlsx"
    cleaned_csv = cleaned_dir / "cleaned_all_months.csv"
    safe_write_csv(cleaned_csv, cleaned_df, table_name="CLEANED_ALL_MONTHS", export_errors=export_errors)
    write_full_export_excel(
        cleaned_xlsx,
        {
            "CLEANED_ALL_MONTHS": cleaned_df,
            "CLEANED_COUNTS_BY_MONTH": cleaned_counts_m,
            "CLEANED_COUNTS_BY_STATUS": cleaned_counts_s,
            "CLEANUP_COUNTS_BY_MONTH": cleanup_counts,
            "MODEL_H_SUMMARY_BY_MONTH": model_h_summary,
            "CLEANED_COLUMN_PROFILE": cleaned_profile,
        },
        csv_fallback_paths={"CLEANED_ALL_MONTHS": cleaned_csv},
        export_errors=export_errors,
    )
    cleaned_monthly_xlsx = cleaned_dir / "cleaned_monthly_counts.xlsx"
    cleaned_monthly_csv = cleaned_dir / "cleaned_monthly_counts.csv"
    safe_write_csv(cleaned_monthly_csv, cleaned_counts_m, table_name="CLEANED_COUNTS_BY_MONTH", export_errors=export_errors)
    write_full_export_excel(
        cleaned_monthly_xlsx,
        {"CLEANED_COUNTS_BY_MONTH": cleaned_counts_m},
        export_errors=export_errors,
    )
    out.cleaned_excel_path = cleaned_xlsx

    # --- combined review ---
    review_dir = base / "combined_review"
    review_dir.mkdir(parents=True, exist_ok=True)
    review_xlsx = review_dir / "issuer_year_full_review.xlsx"
    write_full_export_excel(
        review_xlsx,
        {
            "README": _readme_dataframe(),
            "RAW_ALL_MONTHS": raw_df,
            "RAW_COUNTS_BY_MONTH": raw_counts_m,
            "CLEANED_ALL_MONTHS": cleaned_df,
            "CLEANED_COUNTS_BY_MONTH": cleaned_counts_m,
            "CLEANUP_COUNTS_BY_MONTH": cleanup_counts,
            "MODEL_H_SUMMARY_BY_MONTH": model_h_summary,
            "RAW_VS_CLEANED_RECONCILIATION": reconciliation,
            "COLUMN_MAPPING_AUDIT": mapping_audit,
        },
        csv_fallback_paths={
            "RAW_ALL_MONTHS": raw_csv,
            "CLEANED_ALL_MONTHS": cleaned_csv,
        },
        export_errors=export_errors,
    )
    out.combined_review_path = review_xlsx

    out.monthly_counts = {
        "raw": raw_counts_m,
        "cleaned": cleaned_counts_m,
        "cleanup": cleanup_counts,
    }
    return out


def _yearly_counts(results: list[IssuerYearExportResult]) -> pd.DataFrame:
    rows = []
    for r in results:
        rows.append({
            "issuer": r.issuer,
            "year": r.year,
            "raw_record_count": r.raw_record_count,
            "cleaned_record_count": r.cleaned_record_count,
        })
    return pd.DataFrame(rows)


def _all_monthly_counts(results: list[IssuerYearExportResult]) -> pd.DataFrame:
    rows = []
    for r in results:
        raw_m = r.monthly_counts.get("raw", pd.DataFrame())
        clean_m = r.monthly_counts.get("cleaned", pd.DataFrame())
        cleanup = r.monthly_counts.get("cleanup", pd.DataFrame())
        months = sorted(
            set(raw_m["month"].astype(str) if not raw_m.empty else [])
            | set(clean_m["month"].astype(str) if not clean_m.empty else [])
        )
        for month in months:
            raw_c = 0
            if not raw_m.empty:
                m = raw_m[raw_m["month"].astype(str) == month]
                raw_c = int(m["raw_record_count"].iloc[0]) if len(m) else 0
            clean_c = enroll = enrollee = sub = mh = None
            if not clean_m.empty:
                m = clean_m[clean_m["month"].astype(str) == month]
                if len(m):
                    clean_c = int(m["cleaned_record_count"].iloc[0])
                    enroll = m["distinct_enrollment_count"].iloc[0]
                    enrollee = m["distinct_enrollee_count"].iloc[0]
                    sub = m["distinct_subscriber_count"].iloc[0]
            if not cleanup.empty:
                m = cleanup[cleanup["month"].astype(str) == month]
                if len(m):
                    mh = int(m["model_h_group_count"].iloc[0])
            rows.append({
                "issuer": r.issuer,
                "year": r.year,
                "month": month,
                "raw_record_count": raw_c,
                "cleaned_record_count": clean_c or 0,
                "distinct_enrollment_count": enroll,
                "distinct_enrollee_count": enrollee,
                "distinct_subscriber_count": sub,
                "model_h_group_count": mh or 0,
            })
    return pd.DataFrame(rows)


def _all_raw_vs_cleaned(results: list[IssuerYearExportResult]) -> pd.DataFrame:
    rows = []
    for r in results:
        rows.append({
            "issuer": r.issuer,
            "year": r.year,
            "raw_record_count": r.raw_record_count,
            "cleaned_record_count": r.cleaned_record_count,
            "difference": r.raw_record_count - r.cleaned_record_count,
        })
    return pd.DataFrame(rows)


def _export_file_index(results: list[IssuerYearExportResult]) -> pd.DataFrame:
    rows = []
    for r in results:
        rows.append({
            "issuer": r.issuer,
            "year": r.year,
            "raw_excel_path": str(r.raw_excel_path) if r.raw_excel_path else "",
            "cleaned_excel_path": str(r.cleaned_excel_path) if r.cleaned_excel_path else "",
            "combined_review_excel_path": str(r.combined_review_path) if r.combined_review_path else "",
        })
    return pd.DataFrame(rows)


def write_export_index_html(results: list[IssuerYearExportResult], root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    lines = [
        "<!DOCTYPE html>",
        "<html><head><meta charset='utf-8'>",
        "<title>Full Data Exports</title>",
        "<style>body{font-family:sans-serif;margin:2em} table{border-collapse:collapse}",
        "th,td{border:1px solid #ccc;padding:8px} th{background:#f0f0f0}</style>",
        "</head><body>",
        "<h1>Full Data Exports</h1>",
        "<p>Raw parsed XML and cleaned canonical record-level exports by issuer and year.</p>",
        "<table>",
        "<tr><th>Issuer</th><th>Year</th><th>Raw</th><th>Cleaned</th><th>Combined Review</th></tr>",
    ]
    for r in sorted(results, key=lambda x: (x.issuer, x.year)):
        def _rel(p: Path | None) -> str:
            if not p:
                return ""
            try:
                return html.escape(str(p.relative_to(root)))
            except ValueError:
                return html.escape(str(p))

        rel_raw = _rel(r.raw_excel_path)
        rel_clean = _rel(r.cleaned_excel_path)
        rel_review = _rel(r.combined_review_path)
        lines.append(
            f"<tr><td>{html.escape(r.issuer)}</td><td>{html.escape(r.year)}</td>"
            f"<td><a href='{rel_raw}'>raw_all_months.xlsx</a></td>"
            f"<td><a href='{rel_clean}'>cleaned_all_months.xlsx</a></td>"
            f"<td><a href='{rel_review}'>issuer_year_full_review.xlsx</a></td></tr>"
        )
    lines.append("</table>")
    lines.append(f"<p><a href='full_export_summary.xlsx'>full_export_summary.xlsx</a></p>")
    lines.append("</body></html>")
    index_path = root / "index.html"
    index_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Wrote export index: %s", index_path)
    return index_path


def run_full_data_exports(
    *,
    issuer_filter: str | None = None,
    year_filter: str | None = None,
    parse_source: bool = False,
    export_errors: ExportErrors | None = None,
) -> dict[str, Any]:
    """Run full raw + cleaned exports for all matching issuer/year pairs."""
    root = export_root()
    root.mkdir(parents=True, exist_ok=True)
    pairs = discover_issuer_year_pairs(issuer_filter=issuer_filter, year_filter=year_filter)
    if not pairs:
        logger.warning("No issuer/year partitions found under source_data")
        return {"issuer_years": 0, "results": []}

    results: list[IssuerYearExportResult] = []
    for issuer, year in pairs:
        try:
            results.append(
                export_issuer_year(
                    issuer, year, parse_source=parse_source, export_errors=export_errors,
                )
            )
        except Exception as exc:
            msg = f"Full export failed for {issuer}/{year}: {exc}"
            logger.error(msg)
            if export_errors:
                export_errors.record(msg)

    summary_path = root / "full_export_summary.xlsx"
    write_full_export_excel(
        summary_path,
        {
            "All_Issuers_Yearly_Counts": _yearly_counts(results),
            "All_Issuers_Monthly_Counts": _all_monthly_counts(results),
            "All_Issuers_Raw_vs_Cleaned": _all_raw_vs_cleaned(results),
            "Export_File_Index": _export_file_index(results),
        },
        export_errors=export_errors,
    )
    write_export_index_html(results, root)

    return {
        "issuer_years": len(results),
        "output_root": str(root),
        "summary_path": str(summary_path),
        "results": results,
    }
