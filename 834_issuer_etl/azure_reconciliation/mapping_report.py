"""HTML column mapping report for Azure vs XML reconciliation."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from azure_reconciliation.column_mapper import ColumnMappingResult, mapping_report_sheets
from utils.logger import get_logger

logger = get_logger(__name__)


def write_column_mapping_html(
    output_path: Path,
    mapping: ColumnMappingResult,
    *,
    xml_row_count: int = 0,
    azure_row_count: int = 0,
) -> Path:
    sheets = mapping_report_sheets(mapping)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    sections: list[str] = []
    for name, df in sheets.items():
        if df.empty:
            continue
        sections.append(f"<h2>{name.replace('_', ' ').title()}</h2>\n{df.to_html(index=False)}")

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Column Mapping Report</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 24px; background: #121212; color: #eee; }}
table {{ border-collapse: collapse; width: 100%; margin: 12px 0; font-size: 13px; }}
th, td {{ border: 1px solid #444; padding: 6px; text-align: left; }}
th {{ background: #2B2B2B; }}
h1, h2 {{ color: #6eb6ff; }}
.meta {{ margin: 12px 0; padding: 12px; background: #1e1e1e; border-radius: 4px; }}
</style></head><body>
<h1>Column Mapping Report</h1>
<div class="meta">
<p>XML rows: {xml_row_count} | Azure rows: {azure_row_count}</p>
<p>Canonical join: issuer + enrollment_id + enrollee_id + insurance_type</p>
</div>
{"".join(sections) if sections else "<p>No mappings generated</p>"}
</body></html>"""
    output_path.write_text(html, encoding="utf-8")
    logger.info("Column mapping HTML: %s", output_path)
    return output_path
