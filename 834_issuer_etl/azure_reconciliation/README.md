# Azure vs XML Enrollment Reconciliation

Extension module — does **not** modify the existing pipeline, SFTP downloader, parser, or `main.py`.

## Feature flags (.env)

| Flag | Default | Effect |
|------|---------|--------|
| `ENABLE_AZURE` | `false` | Master switch — when false, XML-only pipeline unchanged |
| `ENABLE_AZURE_MIRROR` | `true` | Mirror reports under `assets/{issuer}/azurevs/` after XML export |
| `ENABLE_AZURE_DISCOVERY` | `true` | Logic discovery (strategies A–E vs XML) |
| `ENABLE_AZURE_RECONCILIATION` | `false` | Run reconciliation after `main.py` pipeline completes |

## Run

```bash
# Unified discovery + reconciliation
python run_azure_intelligence.py --issuer 13535

# Discovery only (interactive Azure login)
python run_azure_discovery.py --issuer 13535

# Full reconciliation (requires Azure credentials in .env)
python run_azure_reconciliation.py

# Filtered scope (matches source_data partitions)
python run_azure_reconciliation.py --issuer 13535 --year 2026 --month 02

# XML + column mapping only (no Azure connection)
python run_azure_reconciliation.py --skip-azure
```

## Discovery findings (issuer 13535)

From reverse-engineering reports:

| Rank | Table | Strategy | Logic |
|------|-------|----------|-------|
| 1 | `dbo.834_Inbound_test` | C | Event — `GAA_834_File_Date` + `enrolleeStatus` |
| 2 | `dbo.834_Inbound_test` | D | Event — `memberMaintEffectiveDate` + `actionCode` |
| 3 | `dbo.CarrierInvoice` | E | Financial — `invoice_month` |
| Snapshot | `dbo.Enrollments_PY2026` | A | Active coverage — repeating monthly counts |

Reconciliation uses discovery recommendations automatically when available under `outputs/azure_discovery/`.

Override via env: `AZURE_ENROLLMENTS_TABLE`, `AZURE_STRATEGY`, `AZURE_DATE_COLUMN`.

## Azure credentials (.env)

```
SERVER=your-server.database.windows.net
DATABASE=your-db
USERNAME=your-user@domain.com
DRIVER=ODBC Driver 17 for SQL Server
AZURE_ENROLLMENTS_TABLE=Enrollments_PY2026
```

Optional: `CONNECTION_TIMEOUT=600` (seconds) — increase if the browser sign-in page takes longer (default **300** / 5 minutes).

Authentication is **ActiveDirectoryInteractive** (sign in via browser; no PASSWORD in `.env`).

Legacy `AZURE_SQL_*` aliases for `SERVER`, `DATABASE`, `USERNAME`, and `DRIVER` are also supported.

## Data sources

| Source | Path | Notes |
|--------|------|-------|
| XML | `source_data/{issuer}/{year}/{month}/` | **Only** XML input; never reads `assets/` |
| Staging (optional) | `data/issuer_834.db` | Used when populated by existing pipeline |
| Azure | `dbo.Enrollments_PY2026` (configurable) | SELECT only via SQLAlchemy |

## Outputs

All under `outputs/`:

| Output | Location |
|--------|----------|
| Azure discovery (unified) | `azure_discovery/azure_discovery.xlsx`, `.sqlite`, `.html` |
| Per-issuer recommendations | `azure_discovery/recommendations_{issuer}.xlsx` |
| Column mapping report | `reconciliation/excel/column_mapping_report.xlsx` |
| Column mapping HTML | `reconciliation/reports/column_mapping_report.html` |
| Comparison workbook | `reconciliation/excel/azure_xml_comparison.xlsx` |
| Dashboards | `reconciliation/dashboards/reconciliation_dashboard.html`, `lifecycle_dashboard.html` |
| SQLite | `reconciliation/sqlite/reconciliation.db` |

## Azure mirror reports (assets/)

After each XML assets export, Azure mirror reports are written under:

```
assets/{issuer}/azurevs/
  excel/     azure_enrollment_summary_{issuer}.xlsx, azure_monthly_kpi_{issuer}.xlsx, ...
  html/      azure_enrollment_summary_{issuer}.html
  dashboards/ azure_monthly_kpi_dashboard_{issuer}.html, azure_rollup_kpi_dashboard_{issuer}.html, ...
  sqlite/    azure_snapshot_{issuer}.sqlite
```

Standalone:

```bash
python run_azure_mirror.py
python run_azure_mirror.py --issuer 13535
```

If Azure connection fails, XML reports are unchanged.

## Active coverage reports

```
assets/{issuer}/azurevs/active_coverage/
  excel/     azure_enrollment_summary_{issuer}.xlsx, ...
  dashboards/ ...
```

Uses benefit_effective_date / benefit_end_date active window (not GAA_Load_Date).

## Logic discovery (compare strategies vs XML)

```
assets/{issuer}/azurevs/discovery/
  azure_table_discovery_{issuer}.xlsx
  azure_logic_candidates_{issuer}.xlsx
  azure_event_candidate_summary_{issuer}.html
  strategy_vs_xml_comparison_{issuer}.xlsx
```

Run standalone:
```bash
python run_azure_discovery.py --issuer 13535
```

Strategies tested: A=active coverage, B=status snapshot, C=event dates, D=834 inbound, E=CarrierInvoice.
Scores compared against XML enrollment summaries in assets/{issuer}/.

SQLite tables: `xml_snapshot`, `azure_snapshot`, `comparison_detail`, `comparison_summary`, `issuer_month_summary`

## Architecture

```
source_data discovery → XML load/parse → lifecycle replay (chronological)
                                              ↓
Azure SQL (dynamic) ──────────────→ column mapping → compare → reports
```

## Join key (canonical)

`issuer + enrollment_id + enrollee_id + insurance_type`

Mapped dynamically from available columns — see `column_mapping_report.xlsx`.

## Lifecycle engine

Replays **all** XML maintenance events chronologically up to each target month.
Final state per member = comparison state (not latest row alone).
