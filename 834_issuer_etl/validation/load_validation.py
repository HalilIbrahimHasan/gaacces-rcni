"""Orchestrates all load validation checks."""

from __future__ import annotations

import pandas as pd

from config.config import settings
from database.db import Database
from reporting.csv_writer import write_csv
from reporting.excel_writer import write_excel
from utils.logger import get_logger
from validation.column_validation import run_column_validation
from validation.count_validation import run_count_validation

logger = get_logger(__name__)


def run_load_validation(db: Database, issuer: str | None = None) -> dict[str, pd.DataFrame]:
    logger.info("Running load validation for issuer=%s", issuer or "ALL")
    results: dict[str, pd.DataFrame] = {}

    results["counts"] = run_count_validation(db, issuer)
    results["columns"] = run_column_validation(db, issuer)

    dup_sql = """
        SELECT file_hash, issuer, file_name, COUNT(*) AS duplicate_count
        FROM raw_file_inventory
        GROUP BY file_hash HAVING COUNT(*) > 1
    """
    results["duplicate_files"] = pd.read_sql_query(dup_sql, db.conn)

    err_where = "WHERE issuer = ?" if issuer else ""
    err_params = (issuer,) if issuer else ()
    results["parse_errors"] = pd.read_sql_query(
        f"SELECT * FROM parse_errors {err_where} ORDER BY created_at DESC",
        db.conn, params=err_params,
    )

    miss_sql = f"""
        SELECT issuer, year, month, policy_id, member_id, action_code_description
        FROM stg_834_records {err_where}
        {'AND' if err_where else 'WHERE'}
        (policy_id IS NULL OR member_id IS NULL OR benefit_effective_date IS NULL)
    """
    results["missing_required"] = pd.read_sql_query(miss_sql, db.conn, params=err_params)

    out_dir = settings.reports_path / "validation"
    issuer_label = issuer or "all"
    write_excel(results["counts"], out_dir / f"{issuer_label}_load_validation.xlsx", "counts")
    write_csv(results["counts"], out_dir / "row_count_by_month_action.csv")
    write_csv(results["parse_errors"], out_dir / "parse_errors.csv")
    write_csv(results["missing_required"], out_dir / "missing_required_fields.csv")

    logger.info("Validation reports written to %s", out_dir)
    return results
