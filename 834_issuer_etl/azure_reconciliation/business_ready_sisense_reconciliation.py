"""
Read-only staged reconciliation: dbo.inbound_automation → Business Ready → Sisense.

Azure access is SELECT-only. Production Business Ready functions are reused
without modification. The only output is a local Excel evidence workbook.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine

from azure_reconciliation.business_transaction_collapse import (
    apply_business_transaction_collapse,
)
from azure_reconciliation.chandra_business_format import (
    chandra_year_rollup,
    to_chandra_business_summary,
)
from azure_reconciliation.lifecycle_snapshot_comparison import (
    build_enriched_canonical_xml,
)
from azure_reconciliation.partition_discovery import Partition
from azure_reconciliation.prior_year_benefit_filter import (
    apply_prior_year_benefit_filter,
)
from azure_reconciliation.reconciliation_analysis import (
    XML_ENROLLEE_ID_COLS,
    XML_ENROLLMENT_ID_COLS,
    _chandra_dashboard,
    _coalesce_id_series,
    _dedupe_transactions,
    _nunique_nonempty,
)
from azure_reconciliation.safe_export import safe_write_excel
from azure_reconciliation.status_mapper import (
    normalize_insurance_type,
    normalize_status,
)
from azure_reconciliation.xml_business_reports import (
    _attach_canonical_subscriber_columns,
    _latest_state_per_business_month,
    apply_business_month_basis,
)
from utils.logger import get_logger

logger = get_logger(__name__)

DEFAULT_EXPECTED_COUNTS = {2025: 2_141_037, 2026: 1_029_577}

STAGE_ORDER = [
    "01_raw_annual_distinct",
    "02_canonicalization",
    "03_business_month_assignment",
    "04_exact_transaction_deduplication",
    "05_business_transaction_collapse",
    "06_prior_year_benefit_filter",
    "07_monthly_model_h",
    "08_production_annual_model_h_rollup",
]

DIMENSIONS = ["issuer", "insurance_type", "coverage_year"]

# Narrow, explicit projection. No SELECT * and no write-capable SQL.
RAW_COLUMNS = [
    "id",
    "issuer",
    "year",
    "month",
    "folder_year",
    "folder_month",
    "filename_file_year",
    "filename_file_month",
    "source_file",
    "source_file_path",
    "file_hash",
    "row_number_in_file",
    "loaded_at",
    "coverage_year",
    "insurance_type",
    "enrolleeStatus",
    "policy_id",
    "member_id",
    "subscriber_id",
    "exchg_assigned_enrollee_id",
    "issuer_subscriber_identifier",
    "issuer_indiv_identifier",
    "enrollee_event_type_code",
    "action_code",
    "action_code_description",
    "maintenance_type_code",
    "additional_maint_reason_code",
    "coverage_status",
    "benefit_effective_date",
    "benefit_end_date",
    "member_maint_effective_date",
    "insurance_type_code",
    "health_coverage_policy_no",
    "enrollment_action_code",
]

COUNT_SQL = text("""
    SELECT folder_year, COUNT_BIG(*) AS row_count
    FROM dbo.inbound_automation
    WHERE folder_year IN :years
      AND id <= :high_water_id
    GROUP BY folder_year
    ORDER BY folder_year
""").bindparams(bindparam("years", expanding=True))

ISSUER_YEAR_SQL = text("""
    SELECT issuer, folder_year, COUNT_BIG(*) AS row_count
    FROM dbo.inbound_automation
    WHERE folder_year IN :years
      AND id <= :high_water_id
    GROUP BY issuer, folder_year
    ORDER BY folder_year, issuer
""").bindparams(bindparam("years", expanding=True))

RAW_EXTRACT_SQL = text(f"""
    SELECT {", ".join(f"[{column}]" for column in RAW_COLUMNS)}
    FROM dbo.inbound_automation
    WHERE issuer = :issuer
      AND folder_year = :folder_year
      AND id <= :high_water_id
    ORDER BY folder_month, source_file, row_number_in_file, id
""")

HIGH_WATER_SQL = text("""
    SELECT COALESCE(MAX(id), 0) AS high_water_id
    FROM dbo.inbound_automation
""")


SISENSE_COLUMNS = [
    "issuer",
    "insurance_type",
    "coverage_year",
    "sisense_enrollment_total",
    "sisense_enrollee_total",
    "sisense_effectuated_enrollments",
    "sisense_effectuated_enrollees",
    "sisense_pending_enrollments",
    "sisense_pending_enrollees",
]

# Exact values transcribed from CS-Effectuated-Enrollments-by-Issuer-7-14-2026.pdf.
# The source PDF repeats the same table on pages 1 and 2; each row appears once here.
SISENSE_REFERENCE_ROWS: list[tuple[Any, ...]] = [
    ("82824", "HEALTH", 2025, 44698, 58721, 44695, 58717, 3, 4),
    ("83761", "HEALTH", 2025, 38050, 55240, 38045, 55232, 5, 8),
    ("83761", "HEALTH", 2026, 59946, 86326, 58556, 84575, 1390, 1751),
    ("70893", "HEALTH", 2025, 525562, 661502, 525408, 661271, 154, 231),
    ("70893", "HEALTH", 2026, 188085, 248693, 186377, 246461, 1708, 2232),
    ("45334", "HEALTH", 2025, 52883, 63789, 52870, 63771, 13, 18),
    ("45334", "HEALTH", 2026, 72643, 90261, 72083, 89501, 560, 760),
    ("49046", "DENTAL", 2025, 14434, 21258, 14428, 21252, 6, 6),
    ("49046", "DENTAL", 2026, 17469, 25176, 17280, 24907, 189, 269),
    ("49046", "HEALTH", 2025, 44141, 63255, 44132, 63238, 9, 17),
    ("49046", "HEALTH", 2026, 47889, 68832, 47524, 68355, 365, 477),
    ("83502", "DENTAL", 2025, 868, 1185, 868, 1185, 0, 0),
    ("83502", "DENTAL", 2026, 852, 1136, 841, 1121, 11, 15),
    ("60224", "HEALTH", 2025, 20494, 25355, 20482, 25340, 12, 15),
    ("60224", "HEALTH", 2026, 15583, 21391, 15294, 21027, 289, 364),
    ("15105", "HEALTH", 2025, 17978, 23062, 17978, 23062, 0, 0),
    ("15105", "HEALTH", 2026, 8892, 11524, 8841, 11450, 51, 74),
    ("86637", "DENTAL", 2025, 6032, 9071, 5989, 9007, 43, 64),
    ("86637", "DENTAL", 2026, 6864, 10248, 6777, 10125, 87, 123),
    ("68806", "DENTAL", 2025, 6606, 9701, 6498, 9529, 108, 172),
    ("68806", "DENTAL", 2026, 5785, 8772, 5161, 7904, 624, 868),
    ("64357", "DENTAL", 2025, 616, 893, 604, 874, 12, 19),
    ("64357", "DENTAL", 2026, 795, 1126, 773, 1094, 22, 32),
    ("37301", "DENTAL", 2025, 2989, 4261, 2989, 4261, 0, 0),
    ("37301", "DENTAL", 2026, 2679, 3786, 2663, 3761, 16, 25),
    ("37001", "DENTAL", 2025, 2306, 3137, 2306, 3137, 0, 0),
    ("37001", "DENTAL", 2026, 3576, 4802, 3535, 4751, 41, 51),
    ("89942", "HEALTH", 2025, 47822, 69448, 47808, 69424, 14, 24),
    ("89942", "HEALTH", 2026, 115093, 181895, 113268, 179168, 1825, 2727),
    ("58081", "HEALTH", 2025, 150535, 210289, 150512, 210261, 23, 28),
    ("58081", "HEALTH", 2026, 139327, 189178, 138186, 187662, 1141, 1516),
    ("13535", "DENTAL", 2025, 718, 1063, 715, 1060, 3, 3),
    ("13535", "DENTAL", 2026, 1116, 1551, 1025, 1416, 91, 135),
    ("43802", "DENTAL", 2025, 5896, 8871, 5868, 8823, 28, 48),
    ("43802", "DENTAL", 2026, 8961, 12837, 8243, 11864, 718, 973),
    ("43802", "HEALTH", 2025, 3877, 5181, 3874, 5172, 3, 9),
    ("43802", "HEALTH", 2026, 2219, 3212, 2173, 3152, 46, 60),
]


@dataclass
class ReconciliationResult:
    output_path: Path
    run_summary: pd.DataFrame
    stage_waterfall: pd.DataFrame
    stage_by_issuer: pd.DataFrame
    final_comparison: pd.DataFrame
    ours_only: pd.DataFrame
    sisense_only: pd.DataFrame
    largest_gaps: pd.DataFrame
    raw_confirm_vs_effectuated: pd.DataFrame
    unresolved_definitions: pd.DataFrame
    data_quality_checks: pd.DataFrame
    collapse_audit: pd.DataFrame = field(default_factory=pd.DataFrame)


def sisense_reference() -> pd.DataFrame:
    return pd.DataFrame(SISENSE_REFERENCE_ROWS, columns=SISENSE_COLUMNS)


def _safe_int_year(value: Any) -> int | None:
    try:
        text_value = str(value).strip()
        if not text_value or text_value.lower() in {"nan", "none"}:
            return None
        return int(float(text_value))
    except (TypeError, ValueError):
        return None


def _dimension_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=DIMENSIONS)
    out = pd.DataFrame(index=df.index)
    out["issuer"] = df.get("issuer", "").astype(str).str.strip()
    insurance = df.get(
        "insurance_type",
        df.get("insurance_type_code", pd.Series([""] * len(df), index=df.index)),
    )
    out["insurance_type"] = insurance.astype(str).map(normalize_insurance_type)
    year_source = df.get(
        "year",
        df.get("coverage_year", df.get("folder_year", pd.Series([None] * len(df), index=df.index))),
    )
    out["coverage_year"] = year_source.map(_safe_int_year)
    return out


def _with_count_keys(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    out = df.copy()
    out["_recon_enrollment_id"] = _coalesce_id_series(out, XML_ENROLLMENT_ID_COLS)
    out["_recon_enrollee_id"] = _coalesce_id_series(out, XML_ENROLLEE_ID_COLS)
    return out


def snapshot_rows(df: pd.DataFrame, stage_name: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                *DIMENSIONS,
                "stage_name",
                "output_row_count",
                "enrollment_count",
                "enrollee_count",
            ]
        )
    keyed = _with_count_keys(df)
    dims = _dimension_frame(keyed)
    keyed = keyed.assign(**{column: dims[column] for column in DIMENSIONS})
    keyed = keyed.dropna(subset=["coverage_year"])
    if keyed.empty:
        return snapshot_rows(pd.DataFrame(), stage_name)
    grouped = (
        keyed.groupby(DIMENSIONS, dropna=False)
        .agg(
            output_row_count=("issuer", "size"),
            enrollment_count=("_recon_enrollment_id", _nunique_nonempty),
            enrollee_count=("_recon_enrollee_id", _nunique_nonempty),
        )
        .reset_index()
    )
    grouped.insert(3, "stage_name", stage_name)
    return grouped


def snapshot_model_h(model_h: pd.DataFrame, stage_name: str) -> pd.DataFrame:
    if model_h.empty:
        return snapshot_rows(pd.DataFrame(), stage_name)
    work = model_h.copy()
    work["coverage_year"] = work["year"].map(_safe_int_year)
    work["insurance_type"] = work["insurance_type"].astype(str).map(normalize_insurance_type)
    grouped = (
        work.groupby(DIMENSIONS, dropna=False)
        .agg(
            output_row_count=("issuer", "size"),
            enrollment_count=("enrollment_count", "sum"),
            enrollee_count=("enrollee_count", "sum"),
        )
        .reset_index()
    )
    grouped.insert(3, "stage_name", stage_name)
    return grouped


def snapshot_production_yearly(model_h_yearly: pd.DataFrame) -> pd.DataFrame:
    if model_h_yearly.empty:
        return snapshot_rows(pd.DataFrame(), "08_production_annual_model_h_rollup")
    work = model_h_yearly.rename(columns={
        "GAA_HIOS_ID": "issuer",
        "Insurance_Type": "insurance_type",
        "Coverage_Year": "coverage_year",
    }).copy()
    work["coverage_year"] = work["coverage_year"].map(_safe_int_year)
    work["insurance_type"] = work["insurance_type"].astype(str).map(normalize_insurance_type)
    grouped = (
        work.groupby(DIMENSIONS, dropna=False)
        .agg(
            output_row_count=("issuer", "size"),
            enrollment_count=("Enrollment_Count", "sum"),
            enrollee_count=("Enrollee_Count", "sum"),
        )
        .reset_index()
    )
    grouped.insert(3, "stage_name", "08_production_annual_model_h_rollup")
    return grouped


def annual_distinct_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    # Intentional small adapter: production candidate columns and normalizers
    # are reused, but month/status are removed to answer Sisense annual totals.
    return snapshot_rows(df, "diagnostic_annual_distinct")


def _finalize_stage_changes(stage_rows: pd.DataFrame) -> pd.DataFrame:
    if stage_rows.empty:
        return stage_rows
    order_map = {name: index for index, name in enumerate(STAGE_ORDER)}
    count_columns = ["output_row_count", "enrollment_count", "enrollee_count"]
    work = (
        stage_rows.groupby(["stage_name", *DIMENSIONS], dropna=False)[count_columns]
        .sum()
        .reset_index()
    )
    dimension_rows = work[DIMENSIONS].drop_duplicates()
    stage_rows_grid = pd.DataFrame({"stage_name": STAGE_ORDER})
    dimension_rows["_join"] = 1
    stage_rows_grid["_join"] = 1
    complete_grid = dimension_rows.merge(stage_rows_grid, on="_join").drop(columns="_join")
    work = complete_grid.merge(
        work,
        on=["stage_name", *DIMENSIONS],
        how="left",
    )
    work[count_columns] = work[count_columns].fillna(0).astype("int64")
    work["_stage_order"] = work["stage_name"].map(order_map)
    work = work.sort_values([*DIMENSIONS, "_stage_order"], kind="stable")
    for metric in ("output_row_count", "enrollment_count", "enrollee_count"):
        previous = work.groupby(DIMENSIONS, dropna=False)[metric].shift(1)
        if metric == "output_row_count":
            work["input_row_count"] = previous.fillna(work[metric]).astype("int64")
            work["rows_removed"] = work["input_row_count"] - work["output_row_count"]
            work["row_change_from_prior"] = work["output_row_count"] - work["input_row_count"]
        else:
            prefix = metric.removesuffix("_count")
            work[f"{prefix}_change_from_prior"] = (work[metric] - previous.fillna(work[metric])).astype("int64")
    columns = [
        "stage_name",
        "input_row_count",
        "output_row_count",
        "rows_removed",
        "row_change_from_prior",
        "enrollment_count",
        "enrollment_change_from_prior",
        "enrollee_count",
        "enrollee_change_from_prior",
        *DIMENSIONS,
    ]
    return work[columns]


def _global_waterfall(stage_by_issuer: pd.DataFrame) -> pd.DataFrame:
    if stage_by_issuer.empty:
        return stage_by_issuer
    numeric = [
        "input_row_count",
        "output_row_count",
        "rows_removed",
        "row_change_from_prior",
        "enrollment_count",
        "enrollment_change_from_prior",
        "enrollee_count",
        "enrollee_change_from_prior",
    ]
    out = stage_by_issuer.groupby("stage_name", as_index=False)[numeric].sum()
    order = {name: index for index, name in enumerate(STAGE_ORDER)}
    out["_order"] = out["stage_name"].map(order)
    return out.sort_values("_order").drop(columns="_order")


def _raw_confirm_counts(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty:
        return pd.DataFrame()
    work = _with_count_keys(raw_df)
    dims = _dimension_frame(work)
    work = work.assign(**{column: dims[column] for column in DIMENSIONS})
    raw_status = work.get("enrolleeStatus", "").astype(str).str.strip().str.upper()
    work = work[raw_status == "CONFIRM"].copy()
    if work.empty:
        return pd.DataFrame(columns=[*DIMENSIONS, "raw_confirm_enrollments", "raw_confirm_enrollees"])
    return (
        work.groupby(DIMENSIONS, dropna=False)
        .agg(
            raw_confirm_enrollments=("_recon_enrollment_id", _nunique_nonempty),
            raw_confirm_enrollees=("_recon_enrollee_id", _nunique_nonempty),
        )
        .reset_index()
    )


def _partitions_for_raw(raw_df: pd.DataFrame, issuer: str, year: int) -> list[Partition]:
    month_source = raw_df.get("folder_month", raw_df.get("month", pd.Series(dtype=object)))
    months = sorted(
        {
            str(int(value)).zfill(2)
            for value in month_source.dropna().tolist()
            if str(value).strip() and str(value).strip().lower() != "nan"
        }
    )
    return [Partition(issuer=str(issuer), year=str(year), month=month) for month in months]


def _adapt_raw_for_business(raw_df: pd.DataFrame) -> pd.DataFrame:
    out = raw_df.copy()
    # Production XML canonicalization expects parser-native year/month and file_name.
    out["year"] = out["folder_year"].astype(str)
    out["month"] = out["folder_month"].astype(str).str.zfill(2)
    out["file_name"] = out["source_file"]
    out["raw_xml_path"] = out["source_file_path"]
    # Preserve raw status inputs; canonicalization reads these production fields.
    out["canonical_status"] = out["enrolleeStatus"].astype(str).map(normalize_status)
    return out


def process_issuer_year(
    raw_df: pd.DataFrame,
    *,
    issuer: str,
    reporting_year: int,
) -> tuple[list[pd.DataFrame], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    raw = _adapt_raw_for_business(raw_df)
    partitions = _partitions_for_raw(raw, issuer, reporting_year)
    snapshots: list[pd.DataFrame] = [
        snapshot_rows(raw, "01_raw_annual_distinct")
    ]

    canonical = build_enriched_canonical_xml(raw, None, partitions=partitions)
    snapshots.append(snapshot_rows(canonical, "02_canonicalization"))

    business_month, _ = apply_business_month_basis(canonical)
    snapshots.append(snapshot_rows(business_month, "03_business_month_assignment"))

    deduped = _dedupe_transactions(business_month)
    snapshots.append(snapshot_rows(deduped, "04_exact_transaction_deduplication"))

    collapse_result = apply_business_transaction_collapse(
        deduped,
        fallback_latest_state_fn=_latest_state_per_business_month,
    )
    collapsed = _attach_canonical_subscriber_columns(
        collapse_result.collapsed,
        business_month,
    )
    snapshots.append(snapshot_rows(collapsed, "05_business_transaction_collapse"))

    filtered = apply_prior_year_benefit_filter(
        collapsed,
        reporting_year=str(reporting_year),
    )
    snapshots.append(snapshot_rows(filtered, "06_prior_year_benefit_filter"))

    model_h = _chandra_dashboard(filtered, source="xml")
    snapshots.append(snapshot_model_h(model_h, "07_monthly_model_h"))
    production_yearly = chandra_year_rollup(to_chandra_business_summary(model_h))
    snapshots.append(snapshot_production_yearly(production_yearly))
    annual_distinct = annual_distinct_snapshot(filtered)

    collapse_audit = pd.DataFrame([{
        "issuer": str(issuer),
        "reporting_year": int(reporting_year),
        "collapse_applied": bool(collapse_result.applied),
        "collapse_warning": collapse_result.warning,
        "production_model_h_monthly_groups": len(model_h),
        "production_model_h_yearly_groups": len(production_yearly),
        "production_model_h_yearly_enrollment_count": (
            int(production_yearly["Enrollment_Count"].sum())
            if not production_yearly.empty
            else 0
        ),
        "production_model_h_yearly_enrollee_count": (
            int(production_yearly["Enrollee_Count"].sum())
            if not production_yearly.empty
            else 0
        ),
        **collapse_result.summary,
    }])
    return snapshots, _raw_confirm_counts(raw), collapse_audit, annual_distinct


def validate_source_counts(
    engine: Engine,
    years: Iterable[int],
    expected_counts: dict[int, int],
    high_water_id: int,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    year_list = sorted({int(year) for year in years})
    with engine.connect() as conn:
        actual = pd.read_sql(
            COUNT_SQL,
            conn,
            params={"years": year_list, "high_water_id": int(high_water_id)},
        )
    actual_map = {
        int(row.folder_year): int(row.row_count)
        for row in actual.itertuples(index=False)
    }
    checks: list[dict[str, Any]] = []
    for year in year_list:
        expected = int(expected_counts[year])
        observed = int(actual_map.get(year, 0))
        checks.append({
            "check_name": f"source_row_count_{year}",
            "expected": expected,
            "actual": observed,
            "passed": observed == expected,
            "severity": "ERROR",
        })
    expected_combined = sum(int(expected_counts[year]) for year in year_list)
    actual_combined = sum(actual_map.get(year, 0) for year in year_list)
    checks.append({
        "check_name": "source_row_count_combined",
        "expected": expected_combined,
        "actual": actual_combined,
        "passed": actual_combined == expected_combined,
        "severity": "ERROR",
    })
    return actual, checks


def fetch_high_water_id(engine: Engine) -> int:
    with engine.connect() as conn:
        row = conn.execute(HIGH_WATER_SQL).one()
    return int(row.high_water_id)


def fetch_issuer_years(
    engine: Engine,
    years: Iterable[int],
    high_water_id: int,
) -> pd.DataFrame:
    year_list = sorted({int(year) for year in years})
    with engine.connect() as conn:
        return pd.read_sql(
            ISSUER_YEAR_SQL,
            conn,
            params={"years": year_list, "high_water_id": int(high_water_id)},
        )


def fetch_raw_issuer_year(
    engine: Engine,
    issuer: str,
    year: int,
    high_water_id: int,
) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(
            RAW_EXTRACT_SQL,
            conn,
            params={
                "issuer": str(issuer),
                "folder_year": int(year),
                "high_water_id": int(high_water_id),
            },
        )


def build_final_comparison(
    final_stage: pd.DataFrame,
    annual_distinct: pd.DataFrame,
    sisense: pd.DataFrame,
    stage_by_issuer: pd.DataFrame,
) -> pd.DataFrame:
    ours = final_stage[
        [*DIMENSIONS, "enrollment_count", "enrollee_count"]
    ].rename(columns={
        "enrollment_count": "our_enrollment_total",
        "enrollee_count": "our_enrollee_total",
    })
    diagnostic = annual_distinct[
        [*DIMENSIONS, "enrollment_count", "enrollee_count"]
    ].rename(columns={
        "enrollment_count": "our_diagnostic_annual_distinct_enrollments",
        "enrollee_count": "our_diagnostic_annual_distinct_enrollees",
    })
    ours = ours.merge(diagnostic, on=DIMENSIONS, how="outer")
    comparison = ours.merge(
        sisense,
        on=DIMENSIONS,
        how="outer",
        indicator=True,
    )
    comparison["row_presence"] = comparison["_merge"].map({
        "left_only": "OURS_ONLY",
        "right_only": "SISENSE_ONLY",
        "both": "BOTH",
    }).astype(str)
    comparison = comparison.drop(columns="_merge")
    for metric in ("enrollment", "enrollee"):
        our_col = f"our_{metric}_total"
        sisense_col = f"sisense_{metric}_total"
        difference_col = f"{metric}_difference"
        pct_col = f"{metric}_percentage_difference"
        comparison[difference_col] = comparison[our_col] - comparison[sisense_col]
        comparison[f"{metric}_absolute_difference"] = comparison[difference_col].abs()
        denominator = comparison[sisense_col].replace(0, pd.NA)
        comparison[pct_col] = (comparison[difference_col] / denominator * 100).round(2)

    comparison["exact_match"] = (
        comparison["enrollment_difference"].fillna(1).eq(0)
        & comparison["enrollee_difference"].fillna(1).eq(0)
    )

    impacts = stage_by_issuer.copy()
    impacts["impact_magnitude"] = (
        impacts["enrollment_change_from_prior"].abs()
        + impacts["enrollee_change_from_prior"].abs()
    )
    impacts = impacts[impacts["stage_name"] != STAGE_ORDER[0]]
    if not impacts.empty:
        max_index = impacts.groupby(DIMENSIONS, dropna=False)["impact_magnitude"].idxmax()
        largest = impacts.loc[max_index, [
            *DIMENSIONS,
            "stage_name",
            "enrollment_change_from_prior",
            "enrollee_change_from_prior",
            "impact_magnitude",
        ]].rename(columns={
            "stage_name": "largest_impact_transformation",
            "enrollment_change_from_prior": "largest_stage_enrollment_change",
            "enrollee_change_from_prior": "largest_stage_enrollee_change",
        })
        comparison = comparison.merge(largest, on=DIMENSIONS, how="left")
    return comparison.sort_values(DIMENSIONS, kind="stable")


def _largest_gaps(comparison: pd.DataFrame) -> pd.DataFrame:
    both = comparison[comparison["row_presence"] == "BOTH"].copy()
    if both.empty:
        return both
    both["combined_absolute_difference"] = (
        both["enrollment_absolute_difference"].fillna(0)
        + both["enrollee_absolute_difference"].fillna(0)
    )
    positive = both.nlargest(20, "enrollment_difference").assign(gap_category="LARGEST_POSITIVE")
    negative = both.nsmallest(20, "enrollment_difference").assign(gap_category="LARGEST_NEGATIVE")
    closest = both.nsmallest(20, "combined_absolute_difference").assign(gap_category="CLOSEST")
    exact = both[both["exact_match"]].assign(gap_category="EXACT_MATCH")
    return pd.concat([exact, closest, positive, negative], ignore_index=True).drop_duplicates(
        subset=[*DIMENSIONS, "gap_category"],
        keep="first",
    )


def _unresolved_definitions() -> pd.DataFrame:
    rows = [
        ("Effectuation", "Raw 834 CONFIRM is not a verified Vimo effectuation predicate.", "Vimo effectuation field, payment/confirmation rule, later CANCEL/TERM handling."),
        ("Pending effectuation", "Total minus raw CONFIRM is not a verified pending predicate.", "Exact pending state and exclusions."),
        ("Enrollment key", "Production XML fallback candidates are known; Sisense internal key is not.", "Sisense enrollment ID and fallback hierarchy."),
        ("Enrollee key", "Raw/canonical identifiers may differ from Vimo internal entities.", "Sisense enrollee key and subscriber-only treatment."),
        ("Coverage year", "Business Ready month/year and Sisense coverage_ may use different source dates.", "Exact Sisense coverage-year formula."),
        ("File-event month", "Production canonicalization currently derives file-event month from canonical event/coverage dates rather than inbound filename year/month columns.", "Whether Sisense uses filename timestamp, maintenance date, benefit date, or another load date."),
        ("Annual aggregation", "Production Model H yearly rollup sums monthly distinct counts.", "Whether Sisense uses annual DISTINCT or monthly-sum counts."),
        ("Population", "Raw ingestion retains accepted parser rows; Vimo may exclude rejected/duplicate/unknown records.", "Sisense source population and rejection filters."),
        ("Snapshot timing", "Raw transaction history is not a current enrollment snapshot.", "Sisense as-of timestamp and retroactive update rules."),
    ]
    return pd.DataFrame(
        rows,
        columns=["definition", "why_unresolved", "clarification_required"],
    )


def _effectuation_comparison(
    raw_confirm: pd.DataFrame,
    sisense: pd.DataFrame,
) -> pd.DataFrame:
    out = raw_confirm.merge(
        sisense[[
            *DIMENSIONS,
            "sisense_effectuated_enrollments",
            "sisense_effectuated_enrollees",
        ]],
        on=DIMENSIONS,
        how="outer",
        indicator=True,
    )
    out["reconciliation_status"] = "UNRESOLVED_VIMO_PREDICATE"
    out["row_presence"] = out["_merge"].map({
        "left_only": "OURS_ONLY",
        "right_only": "SISENSE_ONLY",
        "both": "BOTH",
    }).astype(str)
    return out.drop(columns="_merge").sort_values(DIMENSIONS, kind="stable")


def _run_summary(
    *,
    years: list[int],
    output_path: Path,
    source_counts: pd.DataFrame,
    issuer_years: pd.DataFrame,
    comparison: pd.DataFrame,
    started_at: datetime,
    high_water_id: int,
) -> pd.DataFrame:
    source_by_year = {
        int(row.folder_year): int(row.row_count)
        for row in source_counts.itertuples(index=False)
    }
    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "started_at_utc": started_at.isoformat(timespec="seconds"),
        "years": ",".join(str(year) for year in years),
        "azure_access": "READ_ONLY_SELECT",
        "source_table": "dbo.inbound_automation",
        "source_high_water_id": int(high_water_id),
        "consistency_boundary": "id <= source_high_water_id",
        "source_row_count": int(source_counts["row_count"].sum()),
        "issuer_year_partitions_processed": len(issuer_years),
        "comparison_rows": len(comparison),
        "exact_matches": int(comparison["exact_match"].fillna(False).sum()),
        "ours_only_rows": int((comparison["row_presence"] == "OURS_ONLY").sum()),
        "sisense_only_rows": int((comparison["row_presence"] == "SISENSE_ONLY").sum()),
        "our_enrollment_total_sum": int(comparison["our_enrollment_total"].fillna(0).sum()),
        "sisense_enrollment_total_sum": int(comparison["sisense_enrollment_total"].fillna(0).sum()),
        "our_enrollee_total_sum": int(comparison["our_enrollee_total"].fillna(0).sum()),
        "sisense_enrollee_total_sum": int(comparison["sisense_enrollee_total"].fillna(0).sum()),
        "effectuation_reconciliation": "UNRESOLVED",
        "output_path": str(output_path),
    }
    for year in years:
        summary[f"source_row_count_{year}"] = source_by_year.get(year, 0)
    return pd.DataFrame([summary])


def run_reconciliation(
    engine: Engine,
    *,
    years: Iterable[int] = (2025, 2026),
    output_path: Path,
    expected_counts: dict[int, int] | None = None,
    strict_counts: bool = True,
    raw_fetcher: Any = fetch_raw_issuer_year,
) -> ReconciliationResult:
    started_at = datetime.now(timezone.utc)
    year_list = sorted({int(year) for year in years})
    expectations = dict(expected_counts or DEFAULT_EXPECTED_COUNTS)
    missing_expectations = [year for year in year_list if year not in expectations]
    if missing_expectations:
        raise ValueError(f"Missing expected row counts for year(s): {missing_expectations}")

    high_water_id = fetch_high_water_id(engine)
    source_counts, checks = validate_source_counts(
        engine,
        year_list,
        expectations,
        high_water_id,
    )
    checks.append({
        "check_name": "source_high_water_id_captured",
        "expected": "> 0",
        "actual": high_water_id,
        "passed": high_water_id > 0,
        "severity": "ERROR",
    })
    failed_count_checks = [row for row in checks if not row["passed"]]
    if strict_counts and failed_count_checks:
        details = "; ".join(
            f"{row['check_name']}: expected={row['expected']} actual={row['actual']}"
            for row in failed_count_checks
        )
        raise RuntimeError(f"Source row-count validation failed: {details}")

    issuer_years = fetch_issuer_years(engine, year_list, high_water_id)
    snapshots: list[pd.DataFrame] = []
    annual_distinct_parts: list[pd.DataFrame] = []
    raw_confirm_parts: list[pd.DataFrame] = []
    collapse_parts: list[pd.DataFrame] = []
    fetched_total = 0

    for row in issuer_years.itertuples(index=False):
        issuer = str(row.issuer)
        year = int(row.folder_year)
        expected_partition_rows = int(row.row_count)
        logger.info(
            "Reconciliation partition issuer=%s year=%s expected_rows=%d",
            issuer,
            year,
            expected_partition_rows,
        )
        raw_df = raw_fetcher(engine, issuer, year, high_water_id)
        fetched_total += len(raw_df)
        checks.append({
            "check_name": f"partition_row_count_{issuer}_{year}",
            "expected": expected_partition_rows,
            "actual": len(raw_df),
            "passed": len(raw_df) == expected_partition_rows,
            "severity": "ERROR",
        })
        if raw_df.empty:
            continue
        stage_parts, raw_confirm, collapse_audit, annual_distinct = process_issuer_year(
            raw_df,
            issuer=issuer,
            reporting_year=year,
        )
        snapshots.extend(stage_parts)
        annual_distinct_parts.append(annual_distinct)
        if not raw_confirm.empty:
            raw_confirm_parts.append(raw_confirm)
        collapse_parts.append(collapse_audit)

    checks.append({
        "check_name": "all_fetched_rows_equal_validated_source",
        "expected": int(source_counts["row_count"].sum()),
        "actual": fetched_total,
        "passed": fetched_total == int(source_counts["row_count"].sum()),
        "severity": "ERROR",
    })
    checks.append({
        "check_name": "azure_write_statements_executed",
        "expected": 0,
        "actual": 0,
        "passed": True,
        "severity": "ERROR",
    })

    stage_rows = pd.concat(snapshots, ignore_index=True) if snapshots else pd.DataFrame()
    stage_by_issuer = _finalize_stage_changes(stage_rows)
    stage_waterfall = _global_waterfall(stage_by_issuer)
    final_stage = stage_by_issuer[
        stage_by_issuer["stage_name"] == "08_production_annual_model_h_rollup"
    ].copy()
    annual_distinct = (
        pd.concat(annual_distinct_parts, ignore_index=True)
        if annual_distinct_parts
        else pd.DataFrame(
            columns=[*DIMENSIONS, "enrollment_count", "enrollee_count"]
        )
    )
    if not annual_distinct.empty:
        annual_distinct = (
            annual_distinct.groupby(DIMENSIONS, dropna=False)[
                ["enrollment_count", "enrollee_count"]
            ]
            .sum()
            .reset_index()
        )
    sisense = sisense_reference()
    sisense = sisense[sisense["coverage_year"].isin(year_list)].copy()
    comparison = build_final_comparison(
        final_stage,
        annual_distinct,
        sisense,
        stage_by_issuer,
    )
    ours_only = comparison[comparison["row_presence"] == "OURS_ONLY"].copy()
    sisense_only = comparison[comparison["row_presence"] == "SISENSE_ONLY"].copy()
    largest_gaps = _largest_gaps(comparison)
    raw_confirm = (
        pd.concat(raw_confirm_parts, ignore_index=True)
        if raw_confirm_parts
        else pd.DataFrame(columns=[*DIMENSIONS, "raw_confirm_enrollments", "raw_confirm_enrollees"])
    )
    if not raw_confirm.empty:
        raw_confirm = (
            raw_confirm.groupby(DIMENSIONS, dropna=False)[
                ["raw_confirm_enrollments", "raw_confirm_enrollees"]
            ]
            .sum()
            .reset_index()
        )
    effectuation = _effectuation_comparison(raw_confirm, sisense)
    collapse_audit = (
        pd.concat(collapse_parts, ignore_index=True)
        if collapse_parts
        else pd.DataFrame()
    )

    checks_df = pd.DataFrame(checks)
    checks_df = pd.concat([
        checks_df,
        pd.DataFrame([
            {
                "check_name": "final_comparison_duplicate_dimensions",
                "expected": 0,
                "actual": int(comparison.duplicated(DIMENSIONS).sum()),
                "passed": not comparison.duplicated(DIMENSIONS).any(),
                "severity": "ERROR",
            },
            {
                "check_name": "sisense_reference_effectuation_arithmetic",
                "expected": 0,
                "actual": int(
                    (
                        sisense["sisense_enrollment_total"]
                        - sisense["sisense_effectuated_enrollments"]
                        - sisense["sisense_pending_enrollments"]
                    ).abs().sum()
                    + (
                        sisense["sisense_enrollee_total"]
                        - sisense["sisense_effectuated_enrollees"]
                        - sisense["sisense_pending_enrollees"]
                    ).abs().sum()
                ),
                "passed": bool(
                    (
                        sisense["sisense_enrollment_total"]
                        == sisense["sisense_effectuated_enrollments"]
                        + sisense["sisense_pending_enrollments"]
                    ).all()
                    and (
                        sisense["sisense_enrollee_total"]
                        == sisense["sisense_effectuated_enrollees"]
                        + sisense["sisense_pending_enrollees"]
                    ).all()
                ),
                "severity": "ERROR",
            },
        ]),
    ], ignore_index=True)

    summary = _run_summary(
        years=year_list,
        output_path=output_path,
        source_counts=source_counts,
        issuer_years=issuer_years,
        comparison=comparison,
        started_at=started_at,
        high_water_id=high_water_id,
    )
    unresolved = _unresolved_definitions()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    wrote = safe_write_excel(
        output_path,
        {
            "run_summary": summary,
            "stage_waterfall": stage_waterfall,
            "stage_by_issuer": stage_by_issuer,
            "final_comparison": comparison,
            "ours_only": ours_only,
            "sisense_only": sisense_only,
            "largest_gaps": largest_gaps,
            "raw_confirm_vs_effectuated": effectuation,
            "unresolved_definitions": unresolved,
            "data_quality_checks": checks_df,
            "collapse_audit": collapse_audit,
        },
        drop_duplicate_value_columns=False,
    )
    if not wrote:
        raise RuntimeError(f"Failed to write reconciliation workbook: {output_path}")

    return ReconciliationResult(
        output_path=output_path,
        run_summary=summary,
        stage_waterfall=stage_waterfall,
        stage_by_issuer=stage_by_issuer,
        final_comparison=comparison,
        ours_only=ours_only,
        sisense_only=sisense_only,
        largest_gaps=largest_gaps,
        raw_confirm_vs_effectuated=effectuation,
        unresolved_definitions=unresolved,
        data_quality_checks=checks_df,
        collapse_audit=collapse_audit,
    )
