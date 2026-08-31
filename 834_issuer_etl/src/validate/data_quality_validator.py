"""
Data quality validation for cleaned 834 enrollee records.

Implements business-rule checks (null IDs, duplicates, QTY consistency,
subscriber flags, premium validity) and profiling metrics (missingness,
per-file counts) used in validation reports and dashboards.
"""

import pandas as pd

from config import REQUIRED_ID_FIELDS, VALID_SUBSCRIBER_FLAGS
from utils.logger import get_logger

logger = get_logger(__name__)


class DataQualityValidator:
    """
    Run comprehensive data-quality checks on cleaned enrollee data.

    Each check returns a structured result dict so reports, SQLite, and
    dashboards can display consistent validation issue summaries.
    """

    def validate(self, df: pd.DataFrame, issuer_id: str) -> list[dict]:
        """
        Execute all data-quality checks and profiling metrics.

        Args:
            df: Cleaned enrollee DataFrame for one issuer.
            issuer_id: Issuer identifier.

        Returns:
            Combined list of validation result dictionaries.
        """
        results: list[dict] = []

        if df.empty:
            results.append(self._result(
                issuer_id, "data_profile", "dataset_not_empty", "FAIL",
                "No enrollee records found", "", 0,
            ))
            return results

        results.extend(self._check_required_ids(df, issuer_id))
        results.extend(self._check_duplicates(df, issuer_id))
        results.extend(self._check_qty_consistency(df, issuer_id))
        results.extend(self._check_subscriber_flags(df, issuer_id))
        results.extend(self._check_insurance_types(df, issuer_id))
        results.extend(self._check_premium_fields(df, issuer_id))
        results.extend(self._check_benefit_effective_date(df, issuer_id))
        results.extend(self._check_source_exchg_id(df, issuer_id))
        results.extend(self._profile_missingness(df, issuer_id))
        results.extend(self._profile_file_counts(df, issuer_id))

        fail_count = sum(1 for r in results if r["status"] == "FAIL")
        warn_count = sum(1 for r in results if r["status"] == "WARN")
        logger.info(
            "Data quality validation for issuer %s: %d PASS/WARN/FAIL checks "
            "(%d FAIL, %d WARN)",
            issuer_id,
            len(results),
            fail_count,
            warn_count,
        )
        return results

    def build_missingness_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Build a missingness percentage table by column.

        Used by Excel export and dashboard charts.

        Args:
            df: Cleaned enrollee DataFrame.

        Returns:
            DataFrame with ``column``, ``missing_count``, ``missing_pct``.
        """
        if df.empty:
            return pd.DataFrame(columns=["column", "missing_count", "missing_pct"])

        total = len(df)
        rows = []
        for col in df.columns:
            missing = int(df[col].isna().sum() + (df[col] == "").sum())
            rows.append({
                "column": col,
                "missing_count": missing,
                "missing_pct": round(100 * missing / total, 2),
            })
        return pd.DataFrame(rows).sort_values("missing_pct", ascending=False)

    def build_file_profile_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Build per-file row counts and unique policy/member counts.

        Args:
            df: Cleaned enrollee DataFrame.

        Returns:
            DataFrame with file-level profiling metrics.
        """
        if df.empty or "source_file" not in df.columns:
            return pd.DataFrame()

        profile = (
            df.groupby("source_file")
            .agg(
                row_count=("source_file", "size"),
                unique_policies=("exchg_assigned_policy_id", "nunique"),
                unique_members=("exchg_indiv_identifier", "nunique"),
                file_date=("file_date", "first"),
            )
            .reset_index()
        )
        return profile.sort_values("file_date")

    def _check_required_ids(
        self, df: pd.DataFrame, issuer_id: str
    ) -> list[dict]:
        """Verify required ID fields are not null or empty."""
        results = []
        for field in REQUIRED_ID_FIELDS:
            if field not in df.columns:
                continue
            null_mask = df[field].isna() | (df[field].astype(str).str.strip() == "")
            count = int(null_mask.sum())
            status = "PASS" if count == 0 else "FAIL"
            results.append(self._result(
                issuer_id, "data_quality", f"required_id_{field}", status,
                f"{count} null/empty value(s) in {field}",
                f"Total rows: {len(df)}", count,
            ))
        return results

    def _check_duplicates(
        self, df: pd.DataFrame, issuer_id: str
    ) -> list[dict]:
        """Check duplicate enrollees within files and across all files."""
        results = []

        # Within-file duplicates: issuer + file + member
        key_file = ["issuer_id", "source_file", "exchg_indiv_identifier"]
        if all(c in df.columns for c in key_file):
            dup_file = df.duplicated(subset=key_file, keep=False)
            count = int(dup_file.sum())
            results.append(self._result(
                issuer_id, "data_quality", "duplicate_within_file", 
                "FAIL" if count > 0 else "PASS",
                f"{count} duplicate row(s) on issuer+file+member",
                "", count,
            ))

        # Cross-file duplicates: issuer + member
        key_cross = ["issuer_id", "exchg_indiv_identifier"]
        if all(c in df.columns for c in key_cross):
            dup_cross = df.duplicated(subset=key_cross, keep=False)
            count = int(dup_cross.sum())
            status = "WARN" if count > 0 else "PASS"
            results.append(self._result(
                issuer_id, "data_quality", "duplicate_across_files", status,
                f"{count} row(s) with duplicate issuer+member across files",
                "Expected for maintenance updates; review if unexpected", count,
            ))

        return results

    def _check_qty_consistency(
        self, df: pd.DataFrame, issuer_id: str
    ) -> list[dict]:
        """
        Compare QTYt header values against actual enrollee counts per enrollment.

        Groups by source_file + st02 + gs06 to approximate enrollment segments.
        """
        results = []
        group_cols = [c for c in ["source_file", "st02", "gs06", "qtyt"] if c in df.columns]
        if len(group_cols) < 4:
            return results

        grouped = df.groupby(["source_file", "st02", "gs06"], dropna=False)
        mismatches = []
        for keys, grp in grouped:
            qtyt_vals = grp["qtyt"].dropna().unique()
            if len(qtyt_vals) == 0:
                continue
            expected = int(qtyt_vals[0])
            actual = len(grp)
            if expected != actual:
                mismatches.append(f"{keys}: QTYt={expected}, actual={actual}")

        count = len(mismatches)
        results.append(self._result(
            issuer_id, "data_quality", "qtyt_consistency",
            "WARN" if count > 0 else "PASS",
            f"{count} enrollment segment(s) with QTYt mismatch",
            "; ".join(mismatches[:10]) + ("..." if count > 10 else ""),
            count,
        ))
        return results

    def _check_subscriber_flags(
        self, df: pd.DataFrame, issuer_id: str
    ) -> list[dict]:
        """Ensure subscriber_flag is Y or N when present."""
        if "subscriber_flag" not in df.columns:
            return []
        invalid = ~df["subscriber_flag"].isin(VALID_SUBSCRIBER_FLAGS) & df[
            "subscriber_flag"
        ].notna() & (df["subscriber_flag"].astype(str).str.strip() != "")
        count = int(invalid.sum())
        return [self._result(
            issuer_id, "data_quality", "subscriber_flag_valid",
            "FAIL" if count > 0 else "PASS",
            f"{count} invalid subscriber_flag value(s)",
            f"Expected: {VALID_SUBSCRIBER_FLAGS}", count,
        )]

    def _check_insurance_types(
        self, df: pd.DataFrame, issuer_id: str
    ) -> list[dict]:
        """
        Track insurance type codes dynamically (informational PASS).

        Confirms types are discovered from data rather than a hardcoded list.
        """
        if "insurance_type_code" not in df.columns:
            return []
        types = df["insurance_type_code"].dropna()
        types = types[types.astype(str).str.strip() != ""]
        unique_types = sorted(types.unique().tolist())
        return [self._result(
            issuer_id, "data_profile", "insurance_type_codes_tracked", "PASS",
            f"{len(unique_types)} distinct insurance type code(s) found",
            ", ".join(unique_types), len(unique_types),
        )]

    def _check_premium_fields(
        self, df: pd.DataFrame, issuer_id: str
    ) -> list[dict]:
        """Validate premium fields are numeric and non-negative where applicable."""
        results = []
        premium_cols = [
            "total_premium_amt",
            "health_coverage_premium_amt",
            "total_indiv_responsibility_amt",
            "aptc_amt",
        ]
        for col in premium_cols:
            if col not in df.columns:
                continue
            non_numeric = df[col].notna() & pd.to_numeric(df[col], errors="coerce").isna()
            count = int(non_numeric.sum())
            results.append(self._result(
                issuer_id, "data_quality", f"{col}_numeric",
                "FAIL" if count > 0 else "PASS",
                f"{count} non-numeric value(s) in {col}", "", count,
            ))

        if "total_premium_amt" in df.columns:
            negative = df["total_premium_amt"].notna() & (df["total_premium_amt"] < 0)
            count = int(negative.sum())
            results.append(self._result(
                issuer_id, "data_quality", "total_premium_amt_non_negative",
                "FAIL" if count > 0 else "PASS",
                f"{count} negative total_premium_amt value(s)", "", count,
            ))

        return results

    def _check_benefit_effective_date(
        self, df: pd.DataFrame, issuer_id: str
    ) -> list[dict]:
        """Flag null benefit_effective_begin_date values."""
        if "benefit_effective_begin_date" not in df.columns:
            return []
        null_mask = df["benefit_effective_begin_date"].isna() | (
            df["benefit_effective_begin_date"].astype(str).str.strip() == ""
        )
        count = int(null_mask.sum())
        return [self._result(
            issuer_id, "data_quality", "benefit_effective_begin_date_not_null",
            "FAIL" if count > 0 else "PASS",
            f"{count} null benefit_effective_begin_date value(s)", "", count,
        )]

    def _check_source_exchg_id(
        self, df: pd.DataFrame, issuer_id: str
    ) -> list[dict]:
        """Warn when source_exchg_id is missing on rows that should have it."""
        if "source_exchg_id" not in df.columns:
            return []
        null_mask = df["source_exchg_id"].isna() | (
            df["source_exchg_id"].astype(str).str.strip() == ""
        )
        count = int(null_mask.sum())
        return [self._result(
            issuer_id, "data_quality", "source_exchg_id_present",
            "WARN" if count > 0 else "PASS",
            f"{count} missing source_exchg_id value(s)",
            "Field should be present when available in source XML", count,
        )]

    def _profile_missingness(
        self, df: pd.DataFrame, issuer_id: str
    ) -> list[dict]:
        """Emit informational missingness summary for columns exceeding 50%."""
        miss_df = self.build_missingness_df(df)
        high_miss = miss_df[miss_df["missing_pct"] > 50]
        return [self._result(
            issuer_id, "data_profile", "high_missingness_columns",
            "WARN" if len(high_miss) > 0 else "PASS",
            f"{len(high_miss)} column(s) with >50% missingness",
            ", ".join(
                f"{r['column']}({r['missing_pct']}%)" for _, r in high_miss.iterrows()
            )[:500],
            len(high_miss),
        )]

    def _profile_file_counts(
        self, df: pd.DataFrame, issuer_id: str
    ) -> list[dict]:
        """Emit per-file row count summary as informational PASS."""
        profile = self.build_file_profile_df(df)
        if profile.empty:
            return []
        summary = "; ".join(
            f"{r['source_file']}: {r['row_count']} rows"
            for _, r in profile.iterrows()
        )
        return [self._result(
            issuer_id, "data_profile", "row_counts_by_file", "PASS",
            f"{len(profile)} file(s) profiled", summary[:500], len(profile),
        )]

    @staticmethod
    def _result(
        issuer_id: str,
        category: str,
        check_name: str,
        status: str,
        message: str,
        details: str,
        affected_count: int,
    ) -> dict:
        """Build a standardized validation result record."""
        return {
            "issuer_id": issuer_id,
            "check_category": category,
            "check_name": check_name,
            "status": status,
            "message": message,
            "details": details,
            "affected_count": affected_count,
        }

    def results_to_dataframe(self, results: list[dict]) -> pd.DataFrame:
        """Convert validation result list to a DataFrame for export/SQLite."""
        if not results:
            return pd.DataFrame()
        return pd.DataFrame(results)
