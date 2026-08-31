"""
Compare legacy vs business enrollment summaries against Chandra reference data.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from config.config import settings

# Optional comparison reference only — NOT used by aggregation logic.
# Optional comparison reference only — NOT used by aggregation logic.
# Business-approved expected counts for issuer 15105 (Chandra report screenshots).
CHANDRA_REFERENCE_15105: list[dict[str, object]] = [
    {"Coverage_Year": "2026", "GAA_HIOS_ID": "15105", "GAA_Load_Date": "1/1/2026", "Insurance_Type": "Health", "enrolleeStatus": "CONFIRM", "Enrollment_Count": 1240, "Enrollee_Count": 1690},
    {"Coverage_Year": "2026", "GAA_HIOS_ID": "15105", "GAA_Load_Date": "1/1/2026", "Insurance_Type": "Health", "enrolleeStatus": "CANCEL", "Enrollment_Count": 47, "Enrollee_Count": 47},
    {"Coverage_Year": "2026", "GAA_HIOS_ID": "15105", "GAA_Load_Date": "2/1/2026", "Insurance_Type": "Health", "enrolleeStatus": "CONFIRM", "Enrollment_Count": 490, "Enrollee_Count": 744},
    {"Coverage_Year": "2026", "GAA_HIOS_ID": "15105", "GAA_Load_Date": "2/1/2026", "Insurance_Type": "Health", "enrolleeStatus": "CANCEL", "Enrollment_Count": 734, "Enrollee_Count": 709},
    {"Coverage_Year": "2026", "GAA_HIOS_ID": "15105", "GAA_Load_Date": "2/1/2026", "Insurance_Type": "Health", "enrolleeStatus": "TERM", "Enrollment_Count": 1435, "Enrollee_Count": 1435},
    {"Coverage_Year": "2026", "GAA_HIOS_ID": "15105", "GAA_Load_Date": "3/1/2026", "Insurance_Type": "Health", "enrolleeStatus": "CONFIRM", "Enrollment_Count": 673, "Enrollee_Count": 659},
    {"Coverage_Year": "2026", "GAA_HIOS_ID": "15105", "GAA_Load_Date": "3/1/2026", "Insurance_Type": "Health", "enrolleeStatus": "CANCEL", "Enrollment_Count": 279, "Enrollee_Count": 272},
    {"Coverage_Year": "2026", "GAA_HIOS_ID": "15105", "GAA_Load_Date": "3/1/2026", "Insurance_Type": "Health", "enrolleeStatus": "TERM", "Enrollment_Count": 189, "Enrollee_Count": 188},
    {"Coverage_Year": "2026", "GAA_HIOS_ID": "15105", "GAA_Load_Date": "4/1/2026", "Insurance_Type": "Health", "enrolleeStatus": "CONFIRM", "Enrollment_Count": 592, "Enrollee_Count": 567},
    {"Coverage_Year": "2026", "GAA_HIOS_ID": "15105", "GAA_Load_Date": "4/1/2026", "Insurance_Type": "Health", "enrolleeStatus": "CANCEL", "Enrollment_Count": 236, "Enrollee_Count": 233},
    {"Coverage_Year": "2026", "GAA_HIOS_ID": "15105", "GAA_Load_Date": "4/1/2026", "Insurance_Type": "Health", "enrolleeStatus": "TERM", "Enrollment_Count": 4267, "Enrollee_Count": 4264},
    {"Coverage_Year": "2026", "GAA_HIOS_ID": "15105", "GAA_Load_Date": "5/1/2026", "Insurance_Type": "Health", "enrolleeStatus": "CONFIRM", "Enrollment_Count": 429, "Enrollee_Count": 422},
    {"Coverage_Year": "2026", "GAA_HIOS_ID": "15105", "GAA_Load_Date": "5/1/2026", "Insurance_Type": "Health", "enrolleeStatus": "CANCEL", "Enrollment_Count": 64, "Enrollee_Count": 63},
    {"Coverage_Year": "2026", "GAA_HIOS_ID": "15105", "GAA_Load_Date": "5/1/2026", "Insurance_Type": "Health", "enrolleeStatus": "TERM", "Enrollment_Count": 184, "Enrollee_Count": 182},
    {"Coverage_Year": "2025", "GAA_HIOS_ID": "15105", "GAA_Load_Date": "10/1/2025", "Insurance_Type": "Health", "enrolleeStatus": "CONFIRM", "Enrollment_Count": 19077, "Enrollee_Count": 24565},
    {"Coverage_Year": "2025", "GAA_HIOS_ID": "15105", "GAA_Load_Date": "11/1/2025", "Insurance_Type": "Health", "enrolleeStatus": "CANCEL", "Enrollment_Count": 14, "Enrollee_Count": 13},
]

COMPARE_KEYS = [
    "Coverage_Year",
    "GAA_HIOS_ID",
    "GAA_Load_Date",
    "Insurance_Type",
    "enrolleeStatus",
]


def build_comparison_report(
    legacy_df: pd.DataFrame,
    new_df: pd.DataFrame,
    *,
    issuer_id: str,
    chandra_rows: list[dict[str, object]] | None = None,
) -> pd.DataFrame:
    """Merge legacy, new, and optional Chandra reference counts with deltas."""
    legacy = legacy_df.rename(
        columns={
            "Enrollment_Count": "legacy_enrollment_count",
            "Enrollee_Count": "legacy_enrollee_count",
        }
    )
    new = new_df.rename(
        columns={
            "Enrollment_Count": "new_enrollment_count",
            "Enrollee_Count": "new_enrollee_count",
        }
    )

    merged = new.merge(legacy, on=COMPARE_KEYS, how="outer")
    merged["legacy_enrollment_count"] = merged["legacy_enrollment_count"].fillna(0).astype(int)
    merged["legacy_enrollee_count"] = merged["legacy_enrollee_count"].fillna(0).astype(int)
    merged["new_enrollment_count"] = merged["new_enrollment_count"].fillna(0).astype(int)
    merged["new_enrollee_count"] = merged["new_enrollee_count"].fillna(0).astype(int)

    merged["enrollment_delta_new_vs_legacy"] = (
        merged["new_enrollment_count"] - merged["legacy_enrollment_count"]
    )
    merged["enrollee_delta_new_vs_legacy"] = (
        merged["new_enrollee_count"] - merged["legacy_enrollee_count"]
    )

    if chandra_rows:
        chandra = pd.DataFrame(chandra_rows)
        chandra = chandra.rename(
            columns={
                "Enrollment_Count": "chandra_enrollment_count",
                "Enrollee_Count": "chandra_enrollee_count",
            }
        )
        merged = merged.merge(chandra, on=COMPARE_KEYS, how="outer")
        merged["chandra_enrollment_count"] = merged["chandra_enrollment_count"].fillna(0).astype(int)
        merged["chandra_enrollee_count"] = merged["chandra_enrollee_count"].fillna(0).astype(int)
        merged["enrollment_delta_new_vs_chandra"] = (
            merged["new_enrollment_count"] - merged["chandra_enrollment_count"]
        )
        merged["enrollee_delta_new_vs_chandra"] = (
            merged["new_enrollee_count"] - merged["chandra_enrollee_count"]
        )

    merged = merged.sort_values(COMPARE_KEYS).reset_index(drop=True)
    merged.insert(0, "issuer_id", issuer_id)
    return merged


def export_issuer_comparison(
    legacy_df: pd.DataFrame,
    new_df: pd.DataFrame,
    *,
    issuer_id: str,
    output_dir: Path | None = None,
    chandra_rows: list[dict[str, object]] | None = None,
) -> Path:
    """Write CSV/XLSX comparison report for one issuer rollup summary."""
    output_dir = output_dir or (settings.reports_path / "enrollment_comparison")
    output_dir.mkdir(parents=True, exist_ok=True)

    report = build_comparison_report(
        legacy_df,
        new_df,
        issuer_id=issuer_id,
        chandra_rows=chandra_rows,
    )
    csv_path = output_dir / f"enrollment_summary_comparison_{issuer_id}.csv"
    xlsx_path = output_dir / f"enrollment_summary_comparison_{issuer_id}.xlsx"
    report.to_csv(csv_path, index=False)
    report.to_excel(xlsx_path, index=False)
    return csv_path
