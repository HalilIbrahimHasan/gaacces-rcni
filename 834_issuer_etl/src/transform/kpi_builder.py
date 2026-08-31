"""
KPI builder — computes issuer-level enrollment metrics and breakdowns.

Aggregates cleaned enrollee data into summary statistics and dimensional
counts used by Excel exports, SQLite, and the Plotly dashboard.
"""

from typing import Any

import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__)


class KpiBuilder:
    """
    Build comprehensive KPI dictionaries and DataFrames from enrollee data.

    All metrics are computed dynamically from actual data values (e.g.
    insurance types) rather than hardcoded domain lists.
    """

    def build_kpis(self, df: pd.DataFrame, issuer_id: str) -> dict[str, Any]:
        """
        Compute all issuer-level KPIs and dimensional breakdowns.

        Args:
            df: Cleaned enrollee DataFrame for a single issuer.
            issuer_id: Issuer identifier for labeling outputs.

        Returns:
            Nested dict with scalar KPIs, breakdown DataFrames, and metadata.
        """
        if df.empty:
            logger.warning("Empty DataFrame for issuer %s — KPIs will be zeroed", issuer_id)
            return self._empty_kpis(issuer_id)

        kpis: dict[str, Any] = {
            "issuer_id": issuer_id,
            "total_files_processed": int(df["source_file"].nunique()),
            "total_enrollment_records": int(self._count_enrollment_groups(df)),
            "total_enrollees": len(df),
            "total_subscribers": int((df["subscriber_flag"] == "Y").sum()),
            "total_dependents": int((df["subscriber_flag"] == "N").sum()),
            "unique_policies": int(df["exchg_assigned_policy_id"].nunique(dropna=True)),
            "unique_members": int(df["exchg_indiv_identifier"].nunique(dropna=True)),
            "unique_households": int(
                df["household_or_employee_case_id"].nunique(dropna=True)
            ),
            "duplicate_member_count": int(self._duplicate_member_count(df)),
            "duplicate_policy_member_count": int(
                self._duplicate_policy_member_count(df)
            ),
            "total_premium_amount": self._safe_sum(df, "total_premium_amt"),
            "total_individual_responsibility_amount": self._safe_sum(
                df, "total_indiv_responsibility_amt"
            ),
            "average_premium_amount": self._safe_mean(df, "total_premium_amt"),
            "average_individual_responsibility_amount": self._safe_mean(
                df, "total_indiv_responsibility_amt"
            ),
        }

        # Dimensional breakdowns as DataFrames
        kpis["member_count_by_subscriber_flag"] = self._count_by(df, "subscriber_flag")
        kpis["member_count_by_relationship_code"] = self._count_by(
            df, "relationship_code"
        )
        kpis["member_count_by_event_type"] = self._count_by(df, "event_type_code")
        kpis["member_count_by_event_reason"] = self._count_by(df, "event_reason_code")
        kpis["member_count_by_maintenance_type"] = self._count_by(
            df, "maintenance_type_code"
        )
        kpis["member_count_by_insurance_type"] = self._count_by(
            df, "insurance_type_code"
        )
        kpis["member_count_by_rating_area"] = self._count_by(df, "rating_area")
        kpis["member_count_by_effective_month"] = self._count_by_effective_month(df)
        kpis["premium_by_rating_area"] = self._sum_by(df, "rating_area", "total_premium_amt")
        kpis["premium_by_effective_month"] = self._premium_by_effective_month(df)
        kpis["file_count_trend"] = self._file_count_trend(df)
        kpis["enrollee_count_by_file"] = self._count_by(df, "source_file")
        if "source_period" in df.columns:
            kpis["member_count_by_source_period"] = self._count_by(
                df, "source_period"
            )
            kpis["premium_by_source_period"] = self._sum_by(
                df, "source_period", "total_premium_amt"
            )

        logger.info(
            "Built KPIs for issuer %s: %d enrollees, %d files",
            issuer_id,
            kpis["total_enrollees"],
            kpis["total_files_processed"],
        )
        return kpis

    def kpis_to_summary_df(self, kpis: dict[str, Any]) -> pd.DataFrame:
        """
        Flatten scalar KPIs into a two-column summary DataFrame for Excel/SQLite.

        Args:
            kpis: Output from ``build_kpis``.

        Returns:
            DataFrame with columns ``metric`` and ``value``.
        """
        scalar_keys = [
            "issuer_id",
            "total_files_processed",
            "total_enrollment_records",
            "total_enrollees",
            "total_subscribers",
            "total_dependents",
            "unique_policies",
            "unique_members",
            "unique_households",
            "duplicate_member_count",
            "duplicate_policy_member_count",
            "total_premium_amount",
            "total_individual_responsibility_amount",
            "average_premium_amount",
            "average_individual_responsibility_amount",
        ]
        rows = [{"metric": k, "value": kpis.get(k)} for k in scalar_keys]
        return pd.DataFrame(rows)

    def _count_enrollment_groups(self, df: pd.DataFrame) -> int:
        """
        Approximate enrollment record count via unique header combinations per file.

        Groups by interchange header fields that identify a distinct enrollment
        segment within a source file.
        """
        group_cols = [
            c
            for c in ["source_file", "st02", "gs06", "qtyt"]
            if c in df.columns
        ]
        if not group_cols:
            return 0
        return int(df.groupby(group_cols, dropna=False).ngroups)

    def _duplicate_member_count(self, df: pd.DataFrame) -> int:
        """Count rows that duplicate issuer_id + exchg_indiv_identifier."""
        key = ["issuer_id", "exchg_indiv_identifier"]
        dupes = df.duplicated(subset=key, keep=False)
        return int(dupes.sum())

    def _duplicate_policy_member_count(self, df: pd.DataFrame) -> int:
        """Count rows duplicating issuer + policy + member combination."""
        key = [
            "issuer_id",
            "exchg_assigned_policy_id",
            "exchg_indiv_identifier",
        ]
        available = [c for c in key if c in df.columns]
        if len(available) < 3:
            return 0
        dupes = df.duplicated(subset=available, keep=False)
        return int(dupes.sum())

    @staticmethod
    def _safe_sum(df: pd.DataFrame, col: str) -> float:
        """Sum a numeric column, returning 0.0 when missing or empty."""
        if col not in df.columns:
            return 0.0
        return float(df[col].sum(skipna=True) or 0.0)

    @staticmethod
    def _safe_mean(df: pd.DataFrame, col: str) -> float:
        """Mean of a numeric column, returning 0.0 when missing or empty."""
        if col not in df.columns:
            return 0.0
        mean_val = df[col].mean(skipna=True)
        return float(mean_val) if pd.notna(mean_val) else 0.0

    @staticmethod
    def _count_by(df: pd.DataFrame, column: str) -> pd.DataFrame:
        """Group-by count for a single dimension."""
        if column not in df.columns:
            return pd.DataFrame(columns=[column, "count"])
        counts = (
            df[column]
            .fillna("(missing)")
            .value_counts()
            .reset_index()
        )
        counts.columns = [column, "count"]
        return counts

    def _count_by_effective_month(self, df: pd.DataFrame) -> pd.DataFrame:
        """Count members by benefit effective month (YYYY-MM)."""
        if "benefit_effective_begin_date" not in df.columns:
            return pd.DataFrame(columns=["effective_month", "count"])
        months = df["benefit_effective_begin_date"].astype(str).str[:7]
        counts = months.fillna("(missing)").value_counts().reset_index()
        counts.columns = ["effective_month", "count"]
        return counts.sort_values("effective_month")

    def _sum_by(
        self, df: pd.DataFrame, group_col: str, value_col: str
    ) -> pd.DataFrame:
        """Sum a numeric measure grouped by a dimension."""
        if group_col not in df.columns or value_col not in df.columns:
            return pd.DataFrame(columns=[group_col, f"total_{value_col}"])
        grouped = (
            df.groupby(df[group_col].fillna("(missing)"), dropna=False)[value_col]
            .sum()
            .reset_index()
        )
        grouped.columns = [group_col, f"total_{value_col}"]
        return grouped.sort_values(f"total_{value_col}", ascending=False)

    def _premium_by_effective_month(self, df: pd.DataFrame) -> pd.DataFrame:
        """Total premium aggregated by benefit effective month."""
        if "benefit_effective_begin_date" not in df.columns:
            return pd.DataFrame(columns=["effective_month", "total_premium"])
        tmp = df.copy()
        tmp["effective_month"] = (
            tmp["benefit_effective_begin_date"].astype(str).str[:7]
        )
        grouped = (
            tmp.groupby(tmp["effective_month"].fillna("(missing)"), dropna=False)[
                "total_premium_amt"
            ]
            .sum()
            .reset_index()
        )
        grouped.columns = ["effective_month", "total_premium"]
        return grouped.sort_values("effective_month")

    def _file_count_trend(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrollee counts and file dates for trend analysis."""
        if "source_file" not in df.columns:
            return pd.DataFrame(columns=["source_file", "file_date", "enrollee_count"])
        trend = (
            df.groupby(["source_file", "file_date"], dropna=False)
            .size()
            .reset_index(name="enrollee_count")
        )
        return trend.sort_values("file_date")

    def _empty_kpis(self, issuer_id: str) -> dict[str, Any]:
        """Return zeroed KPI structure when no data is available."""
        empty_df = pd.DataFrame()
        return {
            "issuer_id": issuer_id,
            "total_files_processed": 0,
            "total_enrollment_records": 0,
            "total_enrollees": 0,
            "total_subscribers": 0,
            "total_dependents": 0,
            "unique_policies": 0,
            "unique_members": 0,
            "unique_households": 0,
            "duplicate_member_count": 0,
            "duplicate_policy_member_count": 0,
            "total_premium_amount": 0.0,
            "total_individual_responsibility_amount": 0.0,
            "average_premium_amount": 0.0,
            "average_individual_responsibility_amount": 0.0,
            "member_count_by_subscriber_flag": empty_df,
            "member_count_by_relationship_code": empty_df,
            "member_count_by_event_type": empty_df,
            "member_count_by_event_reason": empty_df,
            "member_count_by_maintenance_type": empty_df,
            "member_count_by_insurance_type": empty_df,
            "member_count_by_rating_area": empty_df,
            "member_count_by_effective_month": empty_df,
            "premium_by_rating_area": empty_df,
            "premium_by_effective_month": empty_df,
            "file_count_trend": empty_df,
            "enrollee_count_by_file": empty_df,
            "member_count_by_source_period": empty_df,
            "premium_by_source_period": empty_df,
        }
