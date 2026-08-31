# 834 Issuer ETL Framework

Data pipeline for daily **834 XML enrollment files**. Extracts enrollee data, loads into **SQLite**, validates source-to-target counts, and generates **reconciliation / user-fee KPI reports**.

## Purpose

- Process 834 XML files organized by **issuer / year / month**
- Support **local**, **FTP**, and **SFTP** ingestion (FTP/SFTP are placeholders today)
- Load into staging tables with full **raw payload** preserved as JSON
- Validate row counts before KPI calculation
- Reconcile policy lifecycle, cancellations, premiums, and **3.25% user fees**
- Apply **90-day cancellation window** rules and rolling 3-month KPIs

## Folder Structure

```
834_issuer_etl/
├── .env.example              # Copy to .env
├── main.py                   # Full pipeline
├── run_validation.py         # Validation only
├── run_kpi_reports.py        # KPI reports only
├── run.py                    # Alias for main.py
│
├── config/
│   └── config.py             # Paths, filters, .env
│
├── connectors/
│   ├── base_connector.py     # SourceConnector interface
│   ├── local_connector.py    # Default — reads source_data/
│   ├── ftp_connector.py      # Placeholder
│   └── sftp_connector.py     # Placeholder
│
├── ingestion/
│   ├── file_discovery.py     # Dynamic issuer/year/month scan
│   ├── zip_handler.py        # Zip extraction (originals kept)
│   └── xml_reader.py
│
├── parsers/
│   └── parser_834.py         # 834 XML → staging records
│
├── database/
│   ├── schema.sql            # raw_file_inventory, stg_834_records
│   ├── db.py
│   └── loaders.py
│
├── validation/
│   ├── load_validation.py
│   ├── count_validation.py
│   └── column_validation.py
│
├── reconciliation/
│   ├── policy_lifecycle.py
│   ├── cancellation_analysis.py
│   ├── cancellation_window.py  # 90-day rules
│   ├── premium_validation.py
│   └── user_fee_calculation.py
│
├── reporting/
│   ├── excel_writer.py
│   ├── csv_writer.py
│   └── report_runner.py
│
├── pipeline/
│   └── orchestrator.py
│
├── source_data/              # INPUT
│   └── {issuer}/{year}/{month}/*.xml
├── extracted/                # Zip extractions
├── database/                 # SQLite DB (gitignored)
├── reports/                  # Excel/CSV outputs (gitignored)
└── logs/
```

## First-Time Setup

```bash
cd 834_issuer_etl
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
```

## Configure `.env`

```env
PROCESSING_MODE=local
SOURCE_DATA_PATH=source_data
DATABASE_PATH=database/issuer_834.db
REPORTS_PATH=reports

# Optional filters (empty = process all)
ISSUER_FILTER=
YEAR_FILTER=
MONTH_FILTER=

# Reference counts for validation (issuer=count)
REFERENCE_ROW_COUNTS=Sigma=16464

USER_FEE_RATE=0.0325
CANCELLATION_WINDOW_DAYS=90
```

FTP/SFTP dummy settings are included for future use. **Local mode works without credentials.**

## Input Structure

```
source_data/
  Sigma/          # or numeric issuer ID like 86637
    2025/
      11/
        file1.xml
        archive.zip    # extracted to extracted/, zip kept
      12/
    2026/
      01/
      02/
```

Rules:
- **No hardcoded issuers** — any folder name under `source_data/`
- Year = 4 digits, month = 1–2 digits (normalized to `01`–`12`)
- Supports **nested subfolders** and **zip files**
- Original source files are **never deleted or overwritten**

## Run Commands

```bash
# Full pipeline — processes all issuers in source_data/
python main.py

# Filter by 5-digit issuer / year / month
python main.py --issuer 86637 --year 2026 --month 02

# Validation only
python run_validation.py --issuer 86637

# KPI reports only (uses existing database)
python run_kpi_reports.py --issuer 86637
```

### Clean Start (every run by default)

Each run wipes `data/`, `reports/`, `extracted/`, `logs/`, and `assets/` before processing — **only current source_data is reflected in outputs**.

Set `CLEAN_ON_START=false` in `.env` or pass `--no-clean` to keep previous outputs.

### Output Locations

| Output | Path |
|--------|------|
| Staging DB | `data/issuer_834.db` |
| Excel/KPI reports | `reports/validation/`, `reports/kpi/` |
| Per-partition assets | `assets/{issuer}/{year}/{month}/excel`, `cleaned_xml`, `sqlite`, `dashboards` |
| Issuer rollups | `assets/{issuer}/rollups/` |

## Database Tables

| Table | Purpose |
|-------|---------|
| `raw_file_inventory` | File metadata, hash, processing status |
| `stg_834_records` | Parsed enrollee rows + `raw_payload` JSON |
| `parse_errors` | Failed file log |

## Validation Reports (`reports/validation/`)

- `{issuer}_load_validation.xlsx` — row counts by issuer/month/action
- `row_count_by_month_action.csv`
- `parse_errors.csv`
- `missing_required_fields.csv`

Compare against `REFERENCE_ROW_COUNTS` in `.env` (e.g. Sigma = 16,464).

## KPI Reports (`reports/kpi/`)

| Report | Content |
|--------|---------|
| `issuer_kpi_summary.xlsx` | Confirmed / cancelled / terminated per month |
| `user_fee_validation.xlsx` | User fee revenue by issuer/month |
| `cancellation_gap_report.xlsx` | Active then cancelled later |
| `repeated_cancel_report.xlsx` | Duplicate cancellations |
| `premium_mismatch_report.xlsx` | APTC + responsibility ≠ premium |
| `cancellation_window_summary.xlsx` | Within/outside 90 days |
| `rolling_3_month_kpi_summary.xlsx` | Jan–Mar, Feb–Apr, etc. |
| `refund_eligibility_report.xlsx` | REFUND_REQUIRED vs NO_REFUND_TERMINATION |

## 90-Day Cancellation Rule

| Status | Meaning |
|--------|---------|
| `WITHIN_90_DAYS` | Cancel within 90 days of effective date → refund may apply |
| `OUTSIDE_90_DAYS` | Cancel after 90 days → treated as termination |
| `REFUND_REQUIRED` | User fee refund eligible |
| `NO_REFUND_TERMINATION` | No refund for valid covered months |

## Future FTP/SFTP Integration

1. Set `PROCESSING_MODE=ftp` or `sftp` in `.env`
2. Connector downloads files into `source_data/`
3. Same parsing pipeline runs automatically

Connectors are stubbed today and fall back to local scan with a warning.

## Sigma Validation Flow (Example)

```bash
# 1. Place Sigma XML under source_data/Sigma/2025/11/
# 2. Set reference count
echo "REFERENCE_ROW_COUNTS=Sigma=16464" >> .env

# 3. Run full pipeline
python main.py --issuer Sigma

# 4. Check validation report
open reports/validation/Sigma_load_validation.xlsx
```

## Legacy Code

The original `src/` modules (dashboard, plotly, assets export) are preserved for reference. The new architecture uses `main.py` at project root as the primary entry point.
