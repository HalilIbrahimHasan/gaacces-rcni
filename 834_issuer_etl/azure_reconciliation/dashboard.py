"""Plotly dashboard for Azure vs XML reconciliation."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from utils.logger import get_logger

logger = get_logger(__name__)


def generate_reconciliation_dashboard(
    summary_df: pd.DataFrame,
    status_summary: pd.DataFrame,
    output_path: Path,
    *,
    title: str = "Azure vs XML Reconciliation",
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            "Overall Match Rate",
            "Status Differences by Partition",
            "XML Not In Azure",
            "Azure Not In XML (Diagnostic)",
        ),
        specs=[[{"type": "indicator"}, {"type": "bar"}],
               [{"type": "bar"}, {"type": "bar"}]],
    )

    if not summary_df.empty:
        avg_match = summary_df["match_rate_pct"].mean() if "match_rate_pct" in summary_df else 0
        fig.add_trace(
            go.Indicator(mode="number", value=avg_match, title={"text": "Avg Match %"}),
            row=1, col=1,
        )
        if "partition" in summary_df.columns:
            fig.add_trace(
                go.Bar(
                    x=summary_df["partition"],
                    y=summary_df.get("status_differences", []),
                    name="Status Diff",
                ),
                row=1, col=2,
            )
            fig.add_trace(
                go.Bar(
                    x=summary_df["partition"],
                    y=summary_df.get("xml_not_in_azure", []),
                    name="XML Only",
                ),
                row=2, col=1,
            )
            fig.add_trace(
                go.Bar(
                    x=summary_df["partition"],
                    y=summary_df.get("azure_not_in_xml", []),
                    name="Azure Only",
                ),
                row=2, col=2,
            )

    fig.update_layout(title=title, height=800, template="plotly_dark")
    fig.write_html(str(output_path), include_plotlyjs="cdn")
    logger.info("Dashboard written: %s", output_path)
    return output_path
