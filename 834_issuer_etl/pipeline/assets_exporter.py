"""
Export partition and rollup assets: excel, cleaned_xml, sqlite, dashboards.

Reads from staging SQLite and writes to assets/{issuer}/{year}/{month}/.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

_SRC = settings.project_root / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from dashboard.plotly_dashboard import PlotlyDashboard  # noqa: E402
from load.excel_exporter import ExcelExporter  # noqa: E402
from load.sqlite_loader import SqliteLoader  # noqa: E402
from load.xml_exporter import XmlExporter  # noqa: E402
from transform.enrollment_summary import (  # noqa: E402
    build_enrollment_summary_legacy,
    build_enrollment_summary_with_debug,
)
from transform.identifier_comparison import build_identifier_comparison  # noqa: E402
from reporting.enrollment_comparison import (  # noqa: E402
    CHANDRA_REFERENCE_15105,
    export_issuer_comparison,
)
from transform.kpi_builder import KpiBuilder  # noqa: E402
from validate.data_quality_validator import DataQualityValidator  # noqa: E402
from validate.schema_validator import SchemaValidator  # noqa: E402

_PROJECT_ROOT = settings.project_root
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
from validation.json_sanitize import dumps_json  # noqa: E402
from validation.parser_field_report import (  # noqa: E402
    build_parser_field_report,
    parser_field_report_to_dataframe,
    parser_field_summary_to_dataframe,
)


STG_TO_LEGACY = {
    "issuer": "issuer_id",
    "policy_id": "exchg_assigned_policy_id",
    "member_id": "exchg_indiv_identifier",
    "subscriber_id": "exchg_subscriber_identifier",
    "relationship": "relationship_code",
    "total_premium_amount": "total_premium_amt",
    "individual_responsibility_amount": "total_indiv_responsibility_amt",
    "aptc_amount": "aptc_amt",
    "insurance_type_code": "insurance_type_code",
    "benefit_effective_date": "benefit_effective_begin_date",
    "member_maint_effective_date": "member_maint_effective_date",
    "enrollee_event_type_code": "event_type_code",
    "enrollee_event_reason_code": "event_reason_code",
    "enrollment_action_code": "enrollment_action_code",
    "exchg_assigned_enrollee_id": "exchg_assigned_enrollee_id",
    "request_submit_timestamp": "request_submit_timestamp",
    "issuer_subscriber_identifier": "issuer_subscriber_identifier",
    "issuer_indiv_identifier": "issuer_indiv_identifier",
    "last_premium_paid_date": "last_premium_paid_date",
    "qtyn": "qtyn",
    "qtyy": "qtyy",
    "qtyt": "qtyt",
    "additional_maint_reason_code": "additional_maint_reason_code",
}


def _partition_dirs(issuer: str, year: str, month: str) -> dict[str, Path]:
    base = settings.assets_path / issuer / year / month
    return {
        "base": base,
        "excel": base / "excel",
        "cleaned_xml": base / "cleaned_xml",
        "sqlite": base / "sqlite",
        "dashboards": base / "dashboards",
        "validation_reports": base / "validation_reports",
    }


def _rollup_dirs(issuer: str) -> dict[str, Path]:
    base = settings.assets_path / issuer / "rollups"
    return {
        "base": base,
        "excel": base / "excel",
        "sqlite": base / "sqlite",
        "dashboards": base / "dashboards",
        "validation_reports": base / "validation_reports",
    }


def _stg_to_legacy_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = out.rename(columns=STG_TO_LEGACY)
    if "source_file" not in out.columns and "file_name" in out.columns:
        out["source_file"] = out["file_name"]
    if "year" in out.columns:
        out["source_year"] = out["year"].astype(str)
    if "month" in out.columns:
        out["source_month"] = out["month"].astype(str)
    if "source_year" in out.columns and "source_month" in out.columns:
        out["source_period"] = out["source_year"] + "-" + out["source_month"]
    for col in ("load_timestamp", "file_date", "rating_area",
                "household_or_employee_case_id", "source_exchg_id"):
        if col not in out.columns:
            out[col] = None
    if "event_type_code" not in out.columns and "action_code" in out.columns:
        out["event_type_code"] = out["action_code"]
    return out


def _load_stg(db_conn, issuer: str | None = None) -> pd.DataFrame:
    sql = """
        SELECT s.*, f.file_name
        FROM stg_834_records s
        LEFT JOIN raw_file_inventory f ON s.file_id = f.file_id
    """
    params: tuple = ()
    if issuer:
        sql += " WHERE s.issuer = ?"
        params = (issuer,)
    return pd.read_sql_query(sql, db_conn, params=params)


def export_assets(db_conn, issuer: str | None = None) -> dict[str, int]:
    """Generate assets/ outputs per partition and issuer rollups."""
    settings.ensure_dirs()
    raw = _load_stg(db_conn, issuer)
    if raw.empty:
        logger.warning("No staging records — skipping assets export")
        return {"partitions": 0, "rollups": 0}

    legacy = _stg_to_legacy_df(raw)
    kpi_builder = KpiBuilder()
    schema_val = SchemaValidator()
    dq_val = DataQualityValidator()
    excel = ExcelExporter()
    xml_exp = XmlExporter()
    sqlite = SqliteLoader()
    dashboard = PlotlyDashboard()

    partitions = 0
    issuer_dfs: dict[str, list[pd.DataFrame]] = {}

    for (iid, year, month), grp in legacy.groupby(["issuer_id", "year", "month"]):
        dirs = _partition_dirs(str(iid), str(year), str(month))
        for d in dirs.values():
            d.mkdir(parents=True, exist_ok=True)
        stem = f"{iid}_{year}_{month}"
        part_df = grp.copy()
        raw_part = raw[
            (raw["issuer"].astype(str) == str(iid))
            & (raw["year"].astype(str) == str(year))
            & (raw["month"].astype(str) == str(month))
        ]

        parser_field_report = build_parser_field_report(raw_part, str(iid))
        parser_field_df = parser_field_report_to_dataframe(parser_field_report)
        parser_field_summary_df = parser_field_summary_to_dataframe(parser_field_report)
        id_comparison = build_identifier_comparison(part_df, str(iid))

        validation_df = dq_val.results_to_dataframe(
            schema_val.validate(part_df, str(iid))
            + dq_val.validate(part_df, str(iid))
        )
        missingness_df = dq_val.build_missingness_df(part_df)
        file_profile_df = dq_val.build_file_profile_df(part_df)
        kpis = kpi_builder.build_kpis(part_df, str(iid))
        kpi_summary_df = kpi_builder.kpis_to_summary_df(kpis)
        enrollment_summary_df, dedup_debug = build_enrollment_summary_with_debug(part_df)
        dedup_dict = dedup_debug.to_dict()

        excel.export_enrollment_summary(enrollment_summary_df, stem, dirs["excel"])
        excel.export_kpis(
            kpis, kpi_summary_df, stem, dirs["excel"],
            enrollment_summary_df=enrollment_summary_df,
        )
        excel.export_validation_report(
            validation_df, missingness_df, file_profile_df, stem, dirs["excel"],
            enrollment_dedup_debug=dedup_dict,
            parser_field_df=parser_field_df,
            parser_field_summary_df=parser_field_summary_df,
            identifier_comparison_summary_df=id_comparison["summary"],
            identifier_comparison_detail_df=id_comparison["dedup_ordering_detail"],
        )
        xml_exp.export_enrollees(part_df, stem, dirs["cleaned_xml"])
        sqlite.load(
            part_df, kpi_summary_df, validation_df, stem, dirs["sqlite"],
            enrollment_summary_df=enrollment_summary_df,
        )
        dashboard.generate(
            kpis, kpi_summary_df, validation_df, missingness_df,
            stem, dirs["dashboards"],
            title=f"Issuer {iid} — {year}/{month} Enrollment Summary",
            enrollment_summary_df=enrollment_summary_df,
        )
        json_path = dirs["validation_reports"] / f"validation_report_{stem}.json"
        json_path.write_text(
            dumps_json(validation_df.to_dict(orient="records"), indent=2),
            encoding="utf-8",
        )
        summary_json = dirs["validation_reports"] / f"enrollment_summary_{stem}.json"
        summary_json.write_text(
            dumps_json(enrollment_summary_df.to_dict(orient="records"), indent=2),
            encoding="utf-8",
        )
        dedup_json = dirs["validation_reports"] / f"enrollment_dedup_debug_{stem}.json"
        dedup_json.write_text(dumps_json(dedup_dict, indent=2), encoding="utf-8")
        parser_json = dirs["validation_reports"] / f"parser_field_coverage_{stem}.json"
        parser_json.write_text(dumps_json(parser_field_report, indent=2), encoding="utf-8")
        id_cmp_json = dirs["validation_reports"] / f"identifier_comparison_{stem}.json"
        id_cmp_payload = {
            "comparison_notes": id_comparison.get("comparison_notes", {}),
            "summary": id_comparison["summary"].to_dict(orient="records")
            if not id_comparison["summary"].empty
            else [],
            "dedup_ordering_detail": id_comparison["dedup_ordering_detail"].to_dict(
                orient="records"
            )
            if not id_comparison["dedup_ordering_detail"].empty
            else [],
        }
        id_cmp_json.write_text(dumps_json(id_cmp_payload, indent=2), encoding="utf-8")
        id_cmp_xlsx = dirs["validation_reports"] / f"identifier_comparison_{stem}.xlsx"
        with pd.ExcelWriter(id_cmp_xlsx, engine="openpyxl") as writer:
            if not id_comparison["summary"].empty:
                id_comparison["summary"].to_excel(writer, sheet_name="summary", index=False)
            if not id_comparison["dedup_ordering_detail"].empty:
                id_comparison["dedup_ordering_detail"].to_excel(
                    writer, sheet_name="dedup_detail", index=False
                )
        issuer_dfs.setdefault(str(iid), []).append(part_df)
        partitions += 1
        logger.info("Assets exported: %s/%s/%s", iid, year, month)

    rollups = 0
    for iid, dfs in issuer_dfs.items():
        combined = pd.concat(dfs, ignore_index=True)
        dirs = _rollup_dirs(iid)
        stem = f"{iid}_all_periods"
        validation_df = dq_val.results_to_dataframe(
            schema_val.validate(combined, iid) + dq_val.validate(combined, iid)
        )
        missingness_df = dq_val.build_missingness_df(combined)
        kpis = kpi_builder.build_kpis(combined, iid)
        kpi_summary_df = kpi_builder.kpis_to_summary_df(kpis)
        enrollment_summary_df, dedup_debug = build_enrollment_summary_with_debug(combined)
        dedup_dict = dedup_debug.to_dict()
        legacy_summary_df = build_enrollment_summary_legacy(combined)
        parser_field_report = build_parser_field_report(
            raw[raw["issuer"].astype(str) == str(iid)], str(iid)
        )
        parser_field_df = parser_field_report_to_dataframe(parser_field_report)
        parser_field_summary_df = parser_field_summary_to_dataframe(parser_field_report)
        id_comparison = build_identifier_comparison(combined, str(iid))

        excel.export_enrollment_summary(enrollment_summary_df, stem, dirs["excel"])
        excel.export_kpis(
            kpis, kpi_summary_df, stem, dirs["excel"],
            enrollment_summary_df=enrollment_summary_df,
            is_rollup=True,
        )
        excel.export_validation_report(
            validation_df, missingness_df,
            dq_val.build_file_profile_df(combined), stem, dirs["excel"],
            enrollment_dedup_debug=dedup_dict,
            parser_field_df=parser_field_df,
            parser_field_summary_df=parser_field_summary_df,
            identifier_comparison_summary_df=id_comparison["summary"],
            identifier_comparison_detail_df=id_comparison["dedup_ordering_detail"],
        )
        sqlite.load(
            combined, kpi_summary_df, validation_df, stem, dirs["sqlite"],
            enrollment_summary_df=enrollment_summary_df,
            rollup=True,
        )
        dashboard.generate(
            kpis, kpi_summary_df, validation_df, missingness_df,
            stem, dirs["dashboards"],
            title=f"Issuer {iid} — All Periods Enrollment Summary",
            enrollment_summary_df=enrollment_summary_df,
            is_rollup=True,
        )
        summary_json = dirs["validation_reports"] / f"enrollment_summary_{stem}.json"
        dirs["validation_reports"].mkdir(parents=True, exist_ok=True)
        summary_json.write_text(
            dumps_json(enrollment_summary_df.to_dict(orient="records"), indent=2),
            encoding="utf-8",
        )
        dedup_json = dirs["validation_reports"] / f"enrollment_dedup_debug_{stem}.json"
        dedup_json.write_text(dumps_json(dedup_dict, indent=2), encoding="utf-8")
        parser_json = dirs["validation_reports"] / f"parser_field_coverage_{stem}.json"
        parser_json.write_text(dumps_json(parser_field_report, indent=2), encoding="utf-8")
        id_cmp_json = dirs["validation_reports"] / f"identifier_comparison_{stem}.json"
        id_cmp_payload = {
            "comparison_notes": id_comparison.get("comparison_notes", {}),
            "summary": id_comparison["summary"].to_dict(orient="records")
            if not id_comparison["summary"].empty
            else [],
            "dedup_ordering_detail": id_comparison["dedup_ordering_detail"].to_dict(
                orient="records"
            )
            if not id_comparison["dedup_ordering_detail"].empty
            else [],
        }
        id_cmp_json.write_text(dumps_json(id_cmp_payload, indent=2), encoding="utf-8")
        id_cmp_xlsx = dirs["validation_reports"] / f"identifier_comparison_{stem}.xlsx"
        with pd.ExcelWriter(id_cmp_xlsx, engine="openpyxl") as writer:
            if not id_comparison["summary"].empty:
                id_comparison["summary"].to_excel(writer, sheet_name="summary", index=False)
            if not id_comparison["dedup_ordering_detail"].empty:
                id_comparison["dedup_ordering_detail"].to_excel(
                    writer, sheet_name="dedup_detail", index=False
                )

        chandra_rows = CHANDRA_REFERENCE_15105 if str(iid) == "15105" else None
        comparison_path = export_issuer_comparison(
            legacy_summary_df,
            enrollment_summary_df,
            issuer_id=str(iid),
            chandra_rows=chandra_rows,
        )
        logger.info("Enrollment comparison report: %s", comparison_path)
        rollups += 1
        logger.info("Rollup assets exported: %s", iid)

    # Azure mirror reports (extension — never breaks XML export; gated by ENABLE_AZURE)
    if settings.azure_enabled and settings.azure_mirror_enabled:
        try:
            from azure_reconciliation.azure_mirror.exporter import export_azure_mirror_reports

            azure_stats = export_azure_mirror_reports(issuer_filter=issuer)
            logger.info("Azure mirror export stats: %s", azure_stats)
        except Exception as exc:
            logger.warning("Azure mirror reports skipped — %s", exc)
    else:
        logger.debug("Azure mirror disabled (ENABLE_AZURE=%s ENABLE_AZURE_MIRROR=%s)",
                     settings.azure_enabled, settings.azure_mirror_enabled)

    return {"partitions": partitions, "rollups": rollups}
