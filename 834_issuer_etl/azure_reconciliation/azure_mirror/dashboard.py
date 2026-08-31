"""Plotly dashboards for Azure mirror reports."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from utils.logger import get_logger

logger = get_logger(__name__)

_HARI_HEADER = "#2B2B2B"
_HARI_CELL = "#1A1A1A"
_HARI_FONT = "#FFFFFF"
_HARI_ALT = "#242424"

SUMMARY_COLUMNS = [
    "Coverage_Year",
    "GAA_HIOS_ID",
    "GAA_Load_Date",
    "Insurance_Type",
    "status_id",
    "enrolleeStatus",
    "Enrollment_Count",
    "Enrollee_Count",
]


def _summary_table(fig: go.Figure, summary_df: pd.DataFrame, row: int, col: int) -> None:
    if summary_df is None or summary_df.empty:
        summary_df = pd.DataFrame({c: ["—"] for c in SUMMARY_COLUMNS})
    display = summary_df[[c for c in SUMMARY_COLUMNS if c in summary_df.columns]].copy()
    for c in SUMMARY_COLUMNS:
        if c not in display.columns:
            display[c] = "—"
    display = display[SUMMARY_COLUMNS]
    n = len(display)
    row_colors = [_HARI_CELL if i % 2 == 0 else _HARI_ALT for i in range(n)]
    fig.add_trace(
        go.Table(
            header=dict(
                values=SUMMARY_COLUMNS,
                fill_color=_HARI_HEADER,
                font=dict(color=_HARI_FONT, size=12),
                align="left",
            ),
            cells=dict(
                values=[display[c].astype(str) for c in SUMMARY_COLUMNS],
                fill_color=[row_colors] * len(SUMMARY_COLUMNS),
                font=dict(color=_HARI_FONT, size=11),
                align="left",
                height=28,
            ),
        ),
        row=row,
        col=col,
    )


def generate_monthly_dashboard(
    monthly_kpi: pd.DataFrame,
    enrollment_summary: pd.DataFrame,
    *,
    issuer: str,
    output_path: Path,
) -> Path:
    """Monthly Azure KPI dashboard mirroring XML monthly dashboard layout."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fig = make_subplots(
        rows=4,
        cols=2,
        subplot_titles=(
            "Azure Enrollment Summary",
            "",
            "Enrollees by Insurance Type",
            "Status Counts",
            "Premium Totals by Insurance Type",
            "Enrollee Count by Month Partition",
            "Monthly KPI Table",
            "",
        ),
        specs=[
            [{"type": "table", "colspan": 2}, None],
            [{"type": "bar"}, {"type": "bar"}],
            [{"type": "bar"}, {"type": "bar"}],
            [{"type": "table", "colspan": 2}, None],
        ],
        vertical_spacing=0.08,
        row_heights=[0.35, 0.22, 0.22, 0.21],
    )

    _summary_table(fig, enrollment_summary, row=1, col=1)

    if not monthly_kpi.empty:
        by_ins = monthly_kpi.groupby("insurance_type", dropna=False)["enrollee_count"].sum().reset_index()
        fig.add_trace(
            go.Bar(x=by_ins["insurance_type"], y=by_ins["enrollee_count"], marker_color="#70AD47"),
            row=2, col=1,
        )
        status_cols = ["enrolled_count", "cancelled_count", "terminated_count", "pending_count"]
        totals = {c: int(monthly_kpi[c].sum()) for c in status_cols if c in monthly_kpi.columns}
        fig.add_trace(
            go.Bar(
                x=list(totals.keys()),
                y=list(totals.values()),
                marker_color=["#4472C4", "#C00000", "#ED7D31", "#FFC000"],
            ),
            row=2, col=2,
        )
        if "gross_premium_total" in monthly_kpi.columns:
            prem = monthly_kpi.groupby("insurance_type")["gross_premium_total"].sum().reset_index()
            fig.add_trace(
                go.Bar(x=prem["insurance_type"], y=prem["gross_premium_total"], marker_color="#2E75B6"),
                row=3, col=1,
            )
        part = monthly_kpi.groupby(["year", "month"])["enrollee_count"].sum().reset_index()
        part["period"] = part["year"] + "-" + part["month"]
        fig.add_trace(
            go.Bar(x=part["period"], y=part["enrollee_count"], marker_color="#9E480E"),
            row=3, col=2,
        )
        kpi_display = monthly_kpi.copy()
        fig.add_trace(
            go.Table(
                header=dict(values=list(kpi_display.columns), fill_color="#4472C4", font=dict(color="white")),
                cells=dict(values=[kpi_display[c].astype(str) for c in kpi_display.columns], fill_color="#E9EDF4"),
            ),
            row=4, col=1,
        )

    fig.update_layout(
        title_text=f"Azure Monthly KPI — Issuer {issuer}",
        height=1800,
        template="plotly_dark",
        paper_bgcolor="#121212",
        plot_bgcolor="#121212",
        showlegend=False,
    )
    fig.write_html(output_path, include_plotlyjs="cdn", full_html=True)
    logger.info("Azure dashboard written: %s", output_path)
    return output_path


def generate_rollup_dashboard(
    rollup: dict[str, pd.DataFrame],
    *,
    issuer: str,
    output_path: Path,
) -> Path:
    """Rollup Azure KPI dashboard with issuer/year/status/trend panels."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fig = make_subplots(
        rows=3,
        cols=2,
        subplot_titles=(
            "Issuer Totals",
            "Status Totals",
            "Year Totals",
            "Insurance Type Totals",
            "Month-over-Month Trend",
            "Premium Totals",
        ),
        specs=[
            [{"type": "table"}, {"type": "bar"}],
            [{"type": "bar"}, {"type": "bar"}],
            [{"type": "scatter"}, {"type": "table"}],
        ],
        vertical_spacing=0.1,
    )

    issuer_df = rollup.get("issuer_totals", pd.DataFrame())
    if not issuer_df.empty:
        fig.add_trace(
            go.Table(
                header=dict(values=list(issuer_df.columns), fill_color="#4472C4", font=dict(color="white")),
                cells=dict(values=[issuer_df[c].astype(str) for c in issuer_df.columns], fill_color="#E9EDF4"),
            ),
            row=1, col=1,
        )

    status_df = rollup.get("status_totals", pd.DataFrame())
    if not status_df.empty:
        fig.add_trace(
            go.Bar(x=status_df["status"], y=status_df["row_count"], marker_color="#7030A0"),
            row=1, col=2,
        )

    year_df = rollup.get("year_totals", pd.DataFrame())
    if not year_df.empty:
        fig.add_trace(
            go.Bar(x=year_df["year"], y=year_df["enrollee_count"], marker_color="#2E75B6"),
            row=2, col=1,
        )

    ins_df = rollup.get("insurance_type_totals", pd.DataFrame())
    if not ins_df.empty:
        fig.add_trace(
            go.Bar(x=ins_df["insurance_type"], y=ins_df["enrollee_count"], marker_color="#70AD47"),
            row=2, col=2,
        )

    trend_df = rollup.get("month_trend", pd.DataFrame())
    if not trend_df.empty:
        trend_df = trend_df.copy()
        trend_df["period"] = trend_df["year"].astype(str) + "-" + trend_df["month"].astype(str)
        fig.add_trace(
            go.Scatter(
                x=trend_df["period"],
                y=trend_df["enrollee_count"],
                mode="lines+markers",
                line=dict(color="#ED7D31"),
                name="Enrollees",
            ),
            row=3, col=1,
        )

    prem_df = rollup.get("premium_totals", pd.DataFrame())
    if not prem_df.empty:
        fig.add_trace(
            go.Table(
                header=dict(values=list(prem_df.columns), fill_color="#4472C4", font=dict(color="white")),
                cells=dict(values=[prem_df[c].astype(str) for c in prem_df.columns], fill_color="#E9EDF4"),
            ),
            row=3, col=2,
        )

    fig.update_layout(
        title_text=f"Azure Rollup KPI — Issuer {issuer}",
        height=1400,
        template="plotly_dark",
        paper_bgcolor="#121212",
        plot_bgcolor="#121212",
        showlegend=False,
    )
    fig.write_html(output_path, include_plotlyjs="cdn", full_html=True)
    logger.info("Azure dashboard written: %s", output_path)
    return output_path


def generate_kpi_dashboard(
    monthly_kpi: pd.DataFrame,
    rollup: dict[str, pd.DataFrame],
    *,
    issuer: str,
    output_path: Path,
) -> Path:
    """Combined Azure KPI overview dashboard."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    issuer_df = rollup.get("issuer_totals", pd.DataFrame())
    trend_df = rollup.get("month_trend", pd.DataFrame())

    fig = make_subplots(
        rows=2,
        cols=2,
        subplot_titles=("Issuer KPI Summary", "Status Distribution", "Monthly Enrollee Trend", "Premium by Month"),
        specs=[[{"type": "table"}, {"type": "pie"}], [{"type": "scatter"}, {"type": "bar"}]],
    )

    if not issuer_df.empty:
        fig.add_trace(
            go.Table(
                header=dict(values=list(issuer_df.columns), fill_color="#4472C4", font=dict(color="white")),
                cells=dict(values=[issuer_df[c].astype(str) for c in issuer_df.columns], fill_color="#E9EDF4"),
            ),
            row=1, col=1,
        )

    status_df = rollup.get("status_totals", pd.DataFrame())
    if not status_df.empty:
        fig.add_trace(
            go.Pie(labels=status_df["status"], values=status_df["row_count"]),
            row=1, col=2,
        )

    if not trend_df.empty:
        trend_df = trend_df.copy()
        trend_df["period"] = trend_df["year"].astype(str) + "-" + trend_df["month"].astype(str)
        fig.add_trace(
            go.Scatter(x=trend_df["period"], y=trend_df["enrollee_count"], mode="lines+markers"),
            row=2, col=1,
        )
        fig.add_trace(
            go.Bar(x=trend_df["period"], y=trend_df["gross_premium_total"], marker_color="#2E75B6"),
            row=2, col=2,
        )

    fig.update_layout(
        title_text=f"Azure KPI Overview — Issuer {issuer}",
        height=1000,
        template="plotly_dark",
        showlegend=False,
    )
    fig.write_html(output_path, include_plotlyjs="cdn", full_html=True)
    logger.info("Azure dashboard written: %s", output_path)
    return output_path


def write_enrollment_summary_html(
    summary_df: pd.DataFrame,
    *,
    issuer: str,
    output_path: Path,
) -> Path:
    """Static HTML table mirroring enrollment summary Excel."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if summary_df.empty:
        body = "<p>No Azure enrollment summary data.</p>"
    else:
        body = summary_df.to_html(index=False, border=1)
    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Azure Enrollment Summary — {issuer}</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 24px; background: #121212; color: #eee; }}
table {{ border-collapse: collapse; width: 100%; }}
th {{ background: #2B2B2B; color: #fff; padding: 8px; text-align: left; }}
td {{ padding: 8px; border: 1px solid #444; }}
tr:nth-child(even) {{ background: #1A1A1A; }}
</style></head><body>
<h1>Azure Enrollment Summary — Issuer {issuer}</h1>
{body}
</body></html>"""
    output_path.write_text(html, encoding="utf-8")
    logger.info("Azure HTML report written: %s", output_path)
    return output_path
