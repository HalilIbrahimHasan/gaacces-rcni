"""Score Azure logic strategies against XML monthly summaries."""

from __future__ import annotations

from typing import Any

import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__)

_PERIOD_COLS = ("year", "month")
_DATE_DERIVE_COLS = (
    "GAA_834_File_Date",
    "file_date",
    "memberMaintEffectiveDate",
    "member_maint_effective_date",
    "event_date",
    "benefit_effective_date",
    "enrollment_last_update_date",
    "application_last_update_date",
)


def _zfill_month(val: Any) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "nat"):
        return ""
    if s.isdigit():
        return s.zfill(2)
    return s


def _normalize_year_month_df(
    df: pd.DataFrame,
    *,
    partitions: list[Any] | None = None,
    context: str = "",
) -> pd.DataFrame:
    """Ensure dataframe has normalized year and month columns."""
    if df.empty:
        return df.copy()

    work = df.copy()
    rename_map: dict[str, str] = {}
    for src, dst in (
        ("coverage_year", "year"),
        ("Coverage_Year", "year"),
        ("xml_year", "year"),
        ("source_year", "year"),
        ("_source_year", "year"),
        ("invoice_year", "year"),
        ("xml_month", "month"),
        ("source_month", "month"),
        ("_source_month", "month"),
        ("invoice_month", "month"),
        ("report_month", "month"),
    ):
        if src in work.columns and "year" not in work.columns and dst == "year":
            rename_map[src] = "year"
        elif src in work.columns and "month" not in work.columns and dst == "month":
            rename_map[src] = "month"
        elif src in work.columns and dst in ("year", "month"):
            if dst not in work.columns:
                rename_map[src] = dst
    if rename_map:
        work = work.rename(columns=rename_map)

    for col in _DATE_DERIVE_COLS:
        if col not in work.columns:
            continue
        dt = pd.to_datetime(work[col], errors="coerce")
        if dt.notna().any():
            if "year" not in work.columns:
                work["year"] = dt.dt.year
            else:
                work["year"] = work["year"].fillna(dt.dt.year)
            derived_month = dt.dt.month.apply(_zfill_month)
            if "month" not in work.columns:
                work["month"] = derived_month
            else:
                work["month"] = work["month"].apply(_zfill_month).where(
                    work["month"].apply(_zfill_month).astype(bool), derived_month
                )
            break

    if "year" in work.columns:
        work["year"] = work["year"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
        work.loc[work["year"].isin(("", "nan", "None", "NaT")), "year"] = pd.NA
    if "month" in work.columns:
        work["month"] = work["month"].apply(_zfill_month)
        work.loc[work["month"] == "", "month"] = pd.NA

    if partitions and "month" in work.columns:
        month_to_years: dict[str, set[str]] = {}
        for part in partitions:
            m = _zfill_month(getattr(part, "month", ""))
            y = str(getattr(part, "year", "")).strip()
            if m and y:
                month_to_years.setdefault(m, set()).add(y)

        if "year" not in work.columns:
            work["year"] = pd.NA

        def _resolve_year(row: pd.Series) -> Any:
            existing = row.get("year")
            if pd.notna(existing) and str(existing).strip() not in ("", "nan", "None"):
                return str(existing).strip()
            m = _zfill_month(row.get("month", ""))
            years = month_to_years.get(m, set())
            if len(years) == 1:
                return next(iter(years))
            return existing

        work["year"] = work.apply(_resolve_year, axis=1)

    missing = [c for c in _PERIOD_COLS if c not in work.columns]
    if missing:
        logger.warning(
            "Cannot derive period columns %s for %s — available columns: %s",
            missing, context, list(work.columns),
        )
    return work


def _has_period_columns(df: pd.DataFrame) -> bool:
    return all(col in df.columns for col in _PERIOD_COLS)


def _empty_period_frame(extra_cols: list[str] | None = None) -> pd.DataFrame:
    cols = list(_PERIOD_COLS) + (extra_cols or [])
    return pd.DataFrame(columns=cols)


def _month_totals(strategy_df: pd.DataFrame, *, partitions: list[Any] | None = None) -> pd.DataFrame:
    if strategy_df.empty:
        return _empty_period_frame([
            "strategy_id", "strategy_name", "source_table",
            "enrollment_count", "enrollee_count", "raw_rows",
        ])

    st = _normalize_year_month_df(strategy_df, partitions=partitions, context="strategy_df")
    if not _has_period_columns(st):
        return _empty_period_frame([
            "strategy_id", "strategy_name", "source_table",
            "enrollment_count", "enrollee_count", "raw_rows",
        ])

    group_cols = [c for c in ["strategy_id", "strategy_name", "source_table", "year", "month"] if c in st.columns]
    agg: dict[str, tuple[str, str]] = {}
    if "enrollment_count" in st.columns:
        agg["enrollment_count"] = ("enrollment_count", "sum")
    if "enrollee_count" in st.columns:
        agg["enrollee_count"] = ("enrollee_count", "sum")
    if "raw_rows" in st.columns:
        agg["raw_rows"] = ("raw_rows", "sum")
    if not agg:
        agg["rows"] = ("strategy_id", "count")

    out = st.groupby(group_cols, dropna=False).agg(**agg).reset_index()
    if "rows" in out.columns and "raw_rows" not in out.columns:
        out = out.rename(columns={"rows": "raw_rows"})
    return out


def _xml_month_totals(xml_totals: pd.DataFrame, *, partitions: list[Any] | None = None) -> pd.DataFrame:
    if xml_totals.empty:
        return _empty_period_frame(["enrollment_count", "enrollee_count"])

    xml = _normalize_year_month_df(xml_totals, partitions=partitions, context="xml_totals")
    if not _has_period_columns(xml):
        return _empty_period_frame(["enrollment_count", "enrollee_count"])

    agg: dict[str, tuple[str, str]] = {}
    if "enrollment_count" in xml.columns:
        agg["enrollment_count"] = ("enrollment_count", "sum")
    elif "Enrollment_Count" in xml.columns:
        xml["enrollment_count"] = pd.to_numeric(xml["Enrollment_Count"], errors="coerce").fillna(0)
        agg["enrollment_count"] = ("enrollment_count", "sum")
    if "enrollee_count" in xml.columns:
        agg["enrollee_count"] = ("enrollee_count", "sum")
    elif "Enrollee_Count" in xml.columns:
        xml["enrollee_count"] = pd.to_numeric(xml["Enrollee_Count"], errors="coerce").fillna(0)
        agg["enrollee_count"] = ("enrollee_count", "sum")
    if not agg:
        agg["rows"] = ("year", "count")

    return xml.groupby(["year", "month"], dropna=False).agg(**agg).reset_index()


def _low_confidence_result(
    *,
    strategy_id: str,
    source_table: str,
    logic_type: str,
    source_date_column: str = "",
    source_status_column: str = "",
    source_policy_column: str = "",
    source_member_column: str = "",
    missing_column_penalty: float = 0.0,
    notes: str,
) -> dict[str, Any]:
    return {
        "strategy_id": strategy_id,
        "source_table": source_table,
        "logic_type": logic_type,
        "source_date_column": source_date_column,
        "source_status_column": source_status_column,
        "source_policy_column": source_policy_column,
        "source_member_column": source_member_column,
        "confidence_score": 0.0,
        "enrollment_count_diff_total": None,
        "enrollee_count_diff_total": None,
        "month_coverage_pct": 0.0,
        "month_pattern_variance": None,
        "status_similarity": None,
        "missing_column_penalty": missing_column_penalty,
        "notes": notes,
    }


def score_strategy_vs_xml(
    strategy_df: pd.DataFrame,
    xml_totals: pd.DataFrame,
    *,
    strategy_id: str,
    source_table: str,
    logic_type: str,
    source_date_column: str = "",
    source_status_column: str = "",
    source_policy_column: str = "",
    source_member_column: str = "",
    missing_column_penalty: float = 0.0,
    partitions: list[Any] | None = None,
) -> dict[str, Any]:
    """Score one strategy/table combination against XML reference."""
    if strategy_df.empty:
        return _low_confidence_result(
            strategy_id=strategy_id,
            source_table=source_table,
            logic_type=logic_type,
            source_date_column=source_date_column,
            source_status_column=source_status_column,
            source_policy_column=source_policy_column,
            source_member_column=source_member_column,
            missing_column_penalty=missing_column_penalty,
            notes="No strategy rows produced",
        )

    st = strategy_df[strategy_df["strategy_id"] == strategy_id].copy() if "strategy_id" in strategy_df.columns else strategy_df.copy()
    if st.empty:
        st = strategy_df.copy()

    st_month = _month_totals(st, partitions=partitions)
    xml_month = _xml_month_totals(xml_totals, partitions=partitions)

    logger.info(
        "score_strategy_vs_xml %s/%s — st_month.columns=%s xml_month.columns=%s",
        strategy_id, source_table, list(st_month.columns), list(xml_month.columns),
    )
    print(f"st_month.columns: {list(st_month.columns)}")
    print(f"xml_month.columns: {list(xml_month.columns)}")
    if not st_month.empty:
        logger.info("st_month head:\n%s", st_month.head().to_string())
        print(f"st_month head:\n{st_month.head().to_string()}")
    if not xml_month.empty:
        logger.info("xml_month head:\n%s", xml_month.head().to_string())
        print(f"xml_month head:\n{xml_month.head().to_string()}")

    if not _has_period_columns(st_month) or not _has_period_columns(xml_month):
        note = (
            f"Skipped scoring — missing year/month after normalization "
            f"(st_month cols={list(st_month.columns)}, xml_month cols={list(xml_month.columns)})"
        )
        logger.warning("%s for %s/%s", note, strategy_id, source_table)
        return _low_confidence_result(
            strategy_id=strategy_id,
            source_table=source_table,
            logic_type=logic_type,
            source_date_column=source_date_column,
            source_status_column=source_status_column,
            source_policy_column=source_policy_column,
            source_member_column=source_member_column,
            missing_column_penalty=missing_column_penalty + 10.0,
            notes=note,
        )

    merge_keys = [c for c in _PERIOD_COLS if c in st_month.columns and c in xml_month.columns]
    if len(merge_keys) < 2:
        note = f"Skipped scoring — merge keys unavailable: {merge_keys}"
        logger.warning("%s for %s/%s", note, strategy_id, source_table)
        return _low_confidence_result(
            strategy_id=strategy_id,
            source_table=source_table,
            logic_type=logic_type,
            source_date_column=source_date_column,
            source_status_column=source_status_column,
            source_policy_column=source_policy_column,
            source_member_column=source_member_column,
            missing_column_penalty=missing_column_penalty + 10.0,
            notes=note,
        )

    merged = st_month.merge(
        xml_month, on=merge_keys, how="outer", suffixes=("_az", "_xml"), indicator=True
    )

    enrollee_diffs: list[float] = []
    enroll_diffs: list[float] = []
    for _, row in merged.iterrows():
        if row["_merge"] == "both":
            enrollee_diffs.append(abs(float(row.get("enrollee_count_az", 0) or 0) - float(row.get("enrollee_count_xml", 0) or 0)))
            enroll_diffs.append(abs(float(row.get("enrollment_count_az", 0) or 0) - float(row.get("enrollment_count_xml", 0) or 0)))

    xml_months = set(zip(xml_month["year"].astype(str), xml_month["month"].astype(str).str.zfill(2))) if not xml_month.empty else set()
    az_months = set(zip(st_month["year"].astype(str), st_month["month"].astype(str).str.zfill(2))) if not st_month.empty else set()
    coverage = len(xml_months & az_months) / len(xml_months) if xml_months else 0.0

    monthly_enrollees = st_month.groupby(["year", "month"])["enrollee_count"].sum() if "enrollee_count" in st_month.columns else pd.Series(dtype=float)
    pattern_var = float(monthly_enrollees.var()) if len(monthly_enrollees) > 1 else 0.0
    pattern_bonus = min(pattern_var / 100.0, 10.0) if pattern_var > 0 else -5.0

    status_sim = None
    xml_norm = _normalize_year_month_df(xml_totals, partitions=partitions, context="xml_status")
    st_norm = _normalize_year_month_df(st, partitions=partitions, context="strategy_status")
    if not xml_norm.empty and not st_norm.empty and "status" in xml_norm.columns and "status" in st_norm.columns:
        xml_st = set(xml_norm["status"].astype(str).unique())
        az_st = set(st_norm["status"].astype(str).unique())
        if xml_st:
            status_sim = len(xml_st & az_st) / len(xml_st)

    total_enrollee_diff = sum(enrollee_diffs) if enrollee_diffs else None
    total_enroll_diff = sum(enroll_diffs) if enroll_diffs else None

    xml_enrollee_total = float(xml_month["enrollee_count"].sum()) if "enrollee_count" in xml_month.columns and not xml_month.empty else 1.0
    diff_penalty = (total_enrollee_diff or 9999) / max(1.0, xml_enrollee_total)
    score = 100.0
    score -= min(diff_penalty * 50, 60)
    score += coverage * 20
    score += (status_sim or 0) * 15
    score += pattern_bonus
    score -= missing_column_penalty
    score = max(0.0, min(100.0, score))

    notes_parts = []
    if pattern_var == 0 and len(monthly_enrollees) > 1:
        notes_parts.append("Identical counts all months — likely snapshot not event logic")
    if logic_type == "event":
        notes_parts.append("Event-based logic — check month-to-month variation")
    if "834_Inbound" in source_table:
        notes_parts.append("834 inbound table — candidate for XML transaction match")

    return {
        "strategy_id": strategy_id,
        "source_table": source_table,
        "logic_type": logic_type,
        "source_date_column": source_date_column,
        "source_status_column": source_status_column,
        "source_policy_column": source_policy_column,
        "source_member_column": source_member_column,
        "confidence_score": round(score, 2),
        "enrollment_count_diff_total": total_enroll_diff,
        "enrollee_count_diff_total": total_enrollee_diff,
        "month_coverage_pct": round(coverage * 100, 1),
        "month_pattern_variance": round(pattern_var, 2),
        "status_similarity": round(status_sim, 3) if status_sim is not None else None,
        "missing_column_penalty": missing_column_penalty,
        "notes": "; ".join(notes_parts) if notes_parts else "",
    }


def closest_match_by_month(
    all_strategies: pd.DataFrame,
    xml_totals: pd.DataFrame,
    *,
    partitions: list[Any] | None = None,
) -> pd.DataFrame:
    """For each XML month, find strategy with smallest enrollee diff."""
    if xml_totals.empty or all_strategies.empty:
        return pd.DataFrame()

    st_month = _month_totals(all_strategies, partitions=partitions)
    xml_month = _xml_month_totals(xml_totals, partitions=partitions)
    if st_month.empty or xml_month.empty or not _has_period_columns(st_month) or not _has_period_columns(xml_month):
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for _, xml_row in xml_month.iterrows():
        year = str(xml_row["year"])
        month = _zfill_month(xml_row["month"])
        xml_e = float(xml_row.get("enrollee_count", 0) or 0)
        subset = st_month[
            (st_month["year"].astype(str) == year)
            & (st_month["month"].astype(str).str.zfill(2) == month)
        ]
        if subset.empty:
            rows.append({"year": year, "month": month, "xml_enrollee_count": xml_e, "best_strategy": None, "best_diff": None})
            continue
        subset = subset.copy()
        subset["diff"] = (subset["enrollee_count"] - xml_e).abs()
        best = subset.loc[subset["diff"].idxmin()]
        rows.append({
            "year": year,
            "month": month,
            "xml_enrollee_count": xml_e,
            "best_strategy_id": best.get("strategy_id"),
            "best_source_table": best.get("source_table"),
            "best_enrollee_count": best.get("enrollee_count"),
            "best_diff": best["diff"],
        })
    return pd.DataFrame(rows)


def build_recommendation(scores_df: pd.DataFrame) -> pd.DataFrame:
    if scores_df.empty:
        return pd.DataFrame([{"recommendation": "No strategies scored — check Azure table access and XML summaries"}])

    best = scores_df.sort_values("confidence_score", ascending=False).iloc[0]
    rows = [{
        "best_azure_table": best["source_table"],
        "best_strategy_id": best["strategy_id"],
        "best_logic_type": best["logic_type"],
        "best_date_column": best.get("source_date_column", ""),
        "best_status_column": best.get("source_status_column", ""),
        "best_policy_column": best.get("source_policy_column", ""),
        "best_member_column": best.get("source_member_column", ""),
        "confidence_score": best["confidence_score"],
        "snapshot_or_event": best["logic_type"],
        "why_recommended": (
            f"Strategy {best['strategy_id']} on {best['source_table']} scored highest "
            f"({best['confidence_score']}). {best.get('notes', '')}"
        ),
        "what_still_does_not_match": (
            "Review enrollee_count_diff_total and closest_match_by_month. "
            "Event/lifecycle replay may still differ from Chandra if Azure stores snapshots."
        ),
    }]

    for i, (_, row) in enumerate(scores_df.sort_values("confidence_score", ascending=False).head(3).iterrows()):
        rows.append({
            "rank": i + 1,
            "best_azure_table": row["source_table"],
            "best_strategy_id": row["strategy_id"],
            "confidence_score": row["confidence_score"],
            "why_recommended": row.get("notes", ""),
        })

    return pd.DataFrame(rows)
