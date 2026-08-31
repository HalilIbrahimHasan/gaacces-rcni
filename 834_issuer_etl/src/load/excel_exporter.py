"""
Excel exporter — writes enrollment summaries, KPIs, and validation reports.
"""

from pathlib import Path
from typing import Any

import pandas as pd

from transform.enrollment_summary import OUTPUT_COLUMNS
from utils.logger import get_logger

logger = get_logger(__name__)


class ExcelExporter:
    """Export enrollee datasets to formatted Excel workbooks."""

    def export_enrollment_summary(
        self, summary_df: pd.DataFrame, output_stem: str, output_dir: Path
    ) -> Path:
        """Export Hari-format enrollment summary (primary business report)."""
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / f"enrollment_summary_{output_stem}.xlsx"
        export_df = summary_df[OUTPUT_COLUMNS] if not summary_df.empty else summary_df
        export_df.to_excel(path, index=False, sheet_name="enrollment_summary")
        csv_path = output_dir / f"enrollment_summary_{output_stem}.csv"
        export_df.to_csv(csv_path, index=False)
        logger.info(
            "Exported enrollment summary to %s (%d rows)", path, len(export_df)
        )
        return path

    def export_enrollees(
        self, df: pd.DataFrame, output_stem: str, output_dir: Path
    ) -> Path:
        """Export detailed cleaned enrollee rows (legacy detail export)."""
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / f"cleaned_enrollees_{output_stem}.xlsx"
        df.to_excel(path, index=False, sheet_name="enrollees")
        logger.info("Exported enrollees to %s (%d rows)", path, len(df))
        return path

    def export_kpis(
        self,
        kpis: dict[str, Any],
        kpi_summary_df: pd.DataFrame,
        output_stem: str,
        output_dir: Path,
        *,
        enrollment_summary_df: pd.DataFrame | None = None,
        is_rollup: bool = False,
    ) -> Path:
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / f"kpi_summary_{output_stem}.xlsx"
        breakdown_sheets = {
            "subscriber_flag": "member_count_by_subscriber_flag",
            "relationship_code": "member_count_by_relationship_code",
            "event_type": "member_count_by_event_type",
            "event_reason": "member_count_by_event_reason",
            "maintenance_type": "member_count_by_maintenance_type",
            "insurance_type": "member_count_by_insurance_type",
            "rating_area": "member_count_by_rating_area",
            "effective_month": "member_count_by_effective_month",
            "premium_rating_area": "premium_by_rating_area",
            "premium_effective_month": "premium_by_effective_month",
            "file_trend": "file_count_trend",
            "enrollee_by_file": "enrollee_count_by_file",
        }
        if is_rollup:
            breakdown_sheets["source_period"] = "member_count_by_source_period"
            breakdown_sheets["premium_period"] = "premium_by_source_period"

        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            if enrollment_summary_df is not None and not enrollment_summary_df.empty:
                enrollment_summary_df[OUTPUT_COLUMNS].to_excel(
                    writer, sheet_name="enrollment_summary", index=False
                )
            kpi_summary_df.to_excel(writer, sheet_name="summary", index=False)
            for sheet_name, kpi_key in breakdown_sheets.items():
                breakdown_df = kpis.get(kpi_key, pd.DataFrame())
                if isinstance(breakdown_df, pd.DataFrame) and not breakdown_df.empty:
                    breakdown_df.to_excel(
                        writer, sheet_name=sheet_name[:31], index=False
                    )
        logger.info("Exported KPI workbook to %s", path)
        return path

    def export_validation_report(
        self,
        validation_df: pd.DataFrame,
        missingness_df: pd.DataFrame,
        file_profile_df: pd.DataFrame,
        output_stem: str,
        output_dir: Path,
        *,
        enrollment_dedup_debug: dict | None = None,
        parser_field_df: pd.DataFrame | None = None,
        parser_field_summary_df: pd.DataFrame | None = None,
        identifier_comparison_summary_df: pd.DataFrame | None = None,
        identifier_comparison_detail_df: pd.DataFrame | None = None,
    ) -> Path:
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / f"validation_report_{output_stem}.xlsx"
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            validation_df.to_excel(writer, sheet_name="validation_checks", index=False)
            missingness_df.to_excel(writer, sheet_name="missingness", index=False)
            file_profile_df.to_excel(writer, sheet_name="file_profile", index=False)
            if enrollment_dedup_debug:
                pd.DataFrame([enrollment_dedup_debug]).to_excel(
                    writer, sheet_name="enrollment_dedup", index=False
                )
            if parser_field_summary_df is not None and not parser_field_summary_df.empty:
                parser_field_summary_df.to_excel(
                    writer, sheet_name="parser_field_summary", index=False
                )
            if parser_field_df is not None and not parser_field_df.empty:
                parser_field_df.to_excel(
                    writer, sheet_name="parser_optional_fields", index=False
                )
            if (
                identifier_comparison_summary_df is not None
                and not identifier_comparison_summary_df.empty
            ):
                identifier_comparison_summary_df.to_excel(
                    writer, sheet_name="id_comparison_summary", index=False
                )
            if (
                identifier_comparison_detail_df is not None
                and not identifier_comparison_detail_df.empty
            ):
                identifier_comparison_detail_df.to_excel(
                    writer, sheet_name="id_comparison_detail", index=False
                )
        logger.info("Exported validation report to %s", path)
        return path
