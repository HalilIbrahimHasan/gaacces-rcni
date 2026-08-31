"""
Business-facing enrollment dashboard with XML vs Azure KPIs and filters.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

STATUS_BUCKETS = {
    "ENROLLED": ("ENROLLED", "CONFIRM", "ACTIVE", "REINSTATE"),
    "CANCELLED": ("CANCELLED", "CANCEL", "CANCELED"),
    "TERMINATED": ("TERMINATED", "TERM"),
    "PENDING": ("PENDING",),
}


def _bucket_status(status: str) -> str:
    s = str(status).upper()
    for bucket, aliases in STATUS_BUCKETS.items():
        if s in aliases or any(a in s for a in aliases):
            return bucket
    return s or "OTHER"


def _kpi_frame(df: pd.DataFrame, *, source: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    work = df.copy()
    if "canonical_status" in work.columns:
        work["_bucket"] = work["canonical_status"].map(_bucket_status)
    else:
        work["_bucket"] = "OTHER"
    ins_col = "insurance_type" if "insurance_type" in work.columns else None
    group = ["issuer", "coverage_year", "snapshot_month"]
    if ins_col:
        group.append(ins_col)
    group.append("_bucket")
    present = [c for c in group if c in work.columns]
    if not present:
        return pd.DataFrame()
    agg: dict[str, tuple[str, str]] = {}
    if "enrollee_id" in work.columns:
        agg["enrollee_count"] = ("enrollee_id", "nunique")
    else:
        agg["enrollee_count"] = ("issuer", "count")
    if "enrollment_id" in work.columns:
        agg["enrollment_count"] = ("enrollment_id", "nunique")
    out = work.groupby(present, dropna=False).agg(**agg).reset_index()
    out["source"] = source
    return out


def _pivot_status_kpis(kpi_df: pd.DataFrame) -> pd.DataFrame:
    if kpi_df.empty:
        return pd.DataFrame()
    idx = [c for c in ("issuer", "coverage_year", "snapshot_month", "insurance_type", "source") if c in kpi_df.columns]
    if "_bucket" not in kpi_df.columns:
        return kpi_df
    val = "enrollee_count" if "enrollee_count" in kpi_df.columns else kpi_df.columns[-1]
    wide = kpi_df.pivot_table(index=idx, columns="_bucket", values=val, aggfunc="sum", fill_value=0).reset_index()
    return wide


def generate_business_dashboard(
    *,
    xml_lifecycle: pd.DataFrame,
    azure_lifecycle: pd.DataFrame,
    comparison_summary: pd.DataFrame,
    level_stats: pd.DataFrame,
    output_path: Path | None = None,
) -> Path:
    if not settings.business_dashboard_enabled:
        return Path()

    out = output_path or settings.outputs_path / "dashboard" / "business_dashboard.html"
    out.parent.mkdir(parents=True, exist_ok=True)

    xml_kpi = _kpi_frame(xml_lifecycle, source="XML")
    az_kpi = _kpi_frame(azure_lifecycle, source="Azure")
    combined = pd.concat([xml_kpi, az_kpi], ignore_index=True) if not (xml_kpi.empty and az_kpi.empty) else pd.DataFrame()

    fig = make_subplots(
        rows=3, cols=2,
        subplot_titles=(
            "Enrollee Count by Source",
            "Enrollment Count by Source",
            "Status Buckets (Enrolled/Cancel/Term)",
            "Issuer Monthly Trend",
            "Match Rate by Level",
            "Health vs Dental",
        ),
        specs=[
            [{"type": "bar"}, {"type": "bar"}],
            [{"type": "bar"}, {"type": "scatter"}],
            [{"type": "indicator"}, {"type": "bar"}],
        ],
        vertical_spacing=0.12,
    )

    if not combined.empty and "enrollee_count" in combined.columns:
        by_src = combined.groupby("source", dropna=False)["enrollee_count"].sum().reset_index()
        fig.add_trace(go.Bar(x=by_src["source"], y=by_src["enrollee_count"], name="Enrollees"), row=1, col=1)
    if not combined.empty and "enrollment_count" in combined.columns:
        by_src = combined.groupby("source", dropna=False)["enrollment_count"].sum().reset_index()
        fig.add_trace(go.Bar(x=by_src["source"], y=by_src["enrollment_count"], name="Enrollments"), row=1, col=2)

    wide = _pivot_status_kpis(combined)
    for col in ("ENROLLED", "CANCELLED", "TERMINATED", "PENDING"):
        if col in wide.columns:
            fig.add_trace(go.Bar(name=col, x=wide.get("source", wide.index), y=wide[col]), row=2, col=1)

    if not combined.empty and {"issuer", "snapshot_month", "enrollee_count"}.issubset(combined.columns):
        trend = combined.groupby(["issuer", "snapshot_month", "source"])["enrollee_count"].sum().reset_index()
        for src in trend["source"].unique():
            sub = trend[trend["source"] == src]
            fig.add_trace(
                go.Scatter(x=sub["snapshot_month"], y=sub["enrollee_count"], mode="lines+markers", name=f"Trend {src}"),
                row=2, col=2,
            )

    if not level_stats.empty and "matched_keys" in level_stats.columns:
        total = level_stats["matched_keys"].sum()
        fig.add_trace(go.Indicator(mode="number", value=total, title={"text": "Lifecycle Matches"}), row=3, col=1)
    elif not comparison_summary.empty and "match_rate_pct" in comparison_summary.columns:
        fig.add_trace(
            go.Indicator(mode="number", value=float(comparison_summary["match_rate_pct"].mean()), title={"text": "Avg Match %"}),
            row=3, col=1,
        )

    if not combined.empty and "insurance_type" in combined.columns:
        ins = combined.groupby(["insurance_type", "source"])["enrollee_count"].sum().reset_index()
        for src in ins["source"].unique():
            sub = ins[ins["source"] == src]
            fig.add_trace(go.Bar(x=sub["insurance_type"], y=sub["enrollee_count"], name=f"{src} ins"), row=3, col=2)

    fig.update_layout(
        title="Business Enrollment Dashboard — XML vs Azure",
        height=1100,
        template="plotly_dark",
        barmode="group",
        legend=dict(orientation="h"),
    )
    fig.write_html(str(out), include_plotlyjs="cdn")
    logger.info("Business dashboard: %s", out)
    return out
