# 834 Issuer ETL — Operations Handbook

This handbook describes how the **834 Issuer ETL** pipeline works end to end: what happens during cleaning, how data is transformed, how reports and Plotly dashboards are produced, and how enrollee uniqueness, subscribers, and KPIs are defined.

**Entry point:** `main.py` (not `src/main.py`, which is a legacy standalone path).

---

## Table of Contents

1. [Pipeline Overview](#1-pipeline-overview)
2. [Data Flow](#2-data-flow)
3. [Cleaning Stage](#3-cleaning-stage)
4. [Transformations](#4-transformations)
5. [Reconciliation & Business Rules](#5-reconciliation--business-rules)
6. [KPI Definitions](#6-kpi-definitions)
7. [Reports](#7-reports)
8. [Plotly HTML Dashboards](#8-plotly-html-dashboards)
9. [Database Tables](#9-database-tables)
10. [Configuration Reference](#10-configuration-reference)
11. [Output Directory Layout](#11-output-directory-layout)
12. [Standalone Runners](#12-standalone-runners)

---

## 1. Pipeline Overview

`Pipeline.run_full()` in `pipeline/orchestrator.py` runs these stages in order:

| Step | Stage | Module | What it does |
|------|-------|--------|--------------|
| 0 | **Workspace clean** | `utils/cleanup.py` | Wipes prior outputs (`data/`, `reports/`, `assets/`, `extracted/`, `logs/`) unless `CLEAN_ON_START=false` |
| 1 | **Ingest & load** | Connectors + `parsers/parser_834.py` + `database/loaders.py` | Discover XML files, parse, load into SQLite staging |
| 2 | **Reconcile** | `reconciliation/*.py` | Premium validation, user fees, 90-day cancellation rules |
| 3 | **Validate** | `validation/load_validation.py` | Row counts, column nulls, parse errors, reference counts |
| 4 | **Report** | `reporting/report_runner.py` | Excel KPI and reconciliation reports |
| 5 | **Export assets** | `pipeline/assets_exporter.py` | Per-partition + rollup Excel, XML, SQLite, Plotly HTML |

```
SFTP / Local files
       ↓
source_data/{issuer}/{year}/{month}/*.xml
       ↓
Parser834 → stg_834_records (SQLite)
       ↓
Reconciliation rules (in-place updates)
       ↓
reports/validation/ + reports/kpi/
       ↓
assets/{issuer}/{year}/{month}/ + rollups
```

---

## 2. Data Flow

### Input sources

| Mode | Connector | Source path |
|------|-----------|-------------|
| `local` | `connectors/local_connector.py` | `source_data/{issuer}/{year}/{month}/` |
| `sftp` | `connectors/sftp_connector.py` | Remote `/archive/in/good/834/{issuer}/{year}/{month}/**` → flattened local `source_data/` |
| `ftp` | `connectors/ftp_connector.py` | Placeholder |

### Discovery rules (`ingestion/file_discovery.py`)

- Issuer folders must be **5-digit numeric** (e.g. `86637`, `13535`)
- Year = 4-digit, month = 1–2 digit
- Accepts `.xml` files; `.zip` archives are expanded to `extracted/`
- Optional filters: `ISSUER_FILTER`, `YEAR_FILTER`, `MONTH_FILTER` (comma-separated, independent)

### File deduplication

`database/loaders.py` → `register_file()` computes a **SHA-256 hash** of each file. Duplicate hashes are skipped — the same physical file is never loaded twice.

---

## 3. Cleaning Stage

Cleaning is **not a single monolithic step** in the primary pipeline. It is distributed across parse time, column mapping, validation, and (optionally) the legacy `DataCleaner`.

### 3.1 Parse-time normalization (`parsers/parser_834.py`)

When XML is parsed into staging records:

| Field type | Rule |
|------------|------|
| Dates | `YYYYMMDD` → `YYYY-MM-DD` via `_parse_date()` |
| Premiums / amounts | Converted to `float` |
| Action descriptions | Mapped from `additionalMaintReason`: `CONFIRM`, `CANCEL`, `TERM`, `REINSTATE` |
| User fee (initial) | `user_fee_amount = round(total_premium × 0.0325, 4)` at parse time |
| Raw payload | Full enrollee XML serialized to `raw_payload` JSON for audit |

### 3.2 Column mapping for exports (`pipeline/assets_exporter.py`)

Staging column names are renamed to the legacy export schema before KPI/validation/dashboard generation:

| Staging column | Export column |
|----------------|---------------|
| `issuer` | `issuer_id` |
| `policy_id` | `exchg_assigned_policy_id` |
| `member_id` | `exchg_indiv_identifier` |
| `subscriber_id` | `exchg_subscriber_identifier` |
| `total_premium_amount` | `total_premium_amt` |
| `individual_responsibility_amount` | `total_indiv_responsibility_amt` |
| `aptc_amount` | `aptc_amt` |
| `benefit_effective_date` | `benefit_effective_begin_date` |

Added metadata: `source_year`, `source_month`, `source_period` (`YYYY-MM`), `load_timestamp`, `file_date`.

### 3.3 Legacy DataCleaner (`src/transform/cleaner.py`)

Used only by the legacy `src/main.py` path. **Not invoked** by the primary `main.py` pipeline.

When used, `DataCleaner.clean()` performs:

| Step | What it does |
|------|--------------|
| Strip strings | Trim whitespace on all text columns |
| Convert dates | `YYYYMMDD` → `YYYY-MM-DD` for configured `DATE_COLUMNS` |
| Convert numerics | Cast premium and quantity columns to numeric |
| Add metadata | `load_timestamp`, issuer/year/month from partition |
| Mask PII | Unless `EXPORT_PII=True`: mask SSN, phone, email, names, address → `***MASKED***` |
| Ensure schema | Add missing `REQUIRED_COLUMNS` in standard order |

### 3.4 Data quality validation (`src/validate/data_quality_validator.py`)

Runs during **assets export** (not during DB load). Checks per partition:

| Check | Key | Severity |
|-------|-----|----------|
| Required IDs non-null | `issuer_id`, `exchg_indiv_identifier`, `exchg_assigned_policy_id` | FAIL |
| Duplicate within file | `issuer_id + source_file + exchg_indiv_identifier` | FAIL |
| Duplicate across files | `issuer_id + exchg_indiv_identifier` | WARN |
| Subscriber flag | Must be `Y` or `N` | FAIL |
| Premium numeric / non-negative | `total_premium_amt`, etc. | FAIL |
| Benefit effective date | Non-null | FAIL |
| High missingness | Columns > 50% missing | WARN |

**Important:** Duplicates are **reported**, not removed. The pipeline does not deduplicate enrollee rows during load.

### 3.5 Workspace cleaning (`utils/cleanup.py`)

On each run (default `CLEAN_ON_START=true`):

| Removed | Kept |
|---------|------|
| `data/` (central DB) | `source_data/` (input XML files) |
| `reports/` | — |
| `assets/` | — |
| `extracted/` | — |
| `logs/` | — |

Use `--no-clean` or `CLEAN_ON_START=false` to preserve prior outputs.

---

## 4. Transformations

### 4.1 XML parsing (`parsers/parser_834.py`)

- Reads 834 enrollment XML structure: `enrollment` → `enrollee` elements
- Produces one staging dict per enrollee row
- Key staging fields: `issuer`, `year`, `month`, `policy_id`, `member_id`, `subscriber_id`, `relationship`, premiums, dates, action codes, `raw_payload`

### 4.2 Database loading (`database/loaders.py`)

1. `register_file()` — inventory row in `raw_file_inventory` with hash dedup
2. `load_records()` — bulk insert into `stg_834_records`
3. `mark_file_status()` — `success` or `failed`
4. `log_parse_error()` — failed files logged to `parse_errors`

### 4.3 Reconciliation (in-place updates on `stg_834_records`)

Applied in this order (`pipeline/orchestrator.py` → `reconcile()`):

```
apply_premium_validation()
    → apply_user_fees()
        → apply_business_rules()
```

See [Section 5](#5-reconciliation--business-rules) for rule details.

### 4.4 Assets export transformation (`pipeline/assets_exporter.py`)

For each `(issuer, year, month)` partition:

1. Load staging records from central DB
2. Rename columns to legacy schema
3. Run schema + data quality validation
4. Build KPIs via `KpiBuilder`
5. Export Excel, cleaned XML, SQLite, Plotly dashboard, validation JSON

Issuer-level **rollups** concatenate all months for that issuer and repeat the same export steps with `is_rollup=True`.

---

## 5. Reconciliation & Business Rules

### 5.1 Premium validation (`reconciliation/premium_validation.py`)

Validates: **Individual Responsibility + APTC ≈ Total Premium**

| Result | Condition |
|--------|-----------|
| `PASS` | \|IR + APTC − premium\| ≤ $0.02 |
| `MISMATCH` | Difference exceeds tolerance |
| `MISSING_PREMIUM` | Premium field is null |

Stored in: `premium_validation_status`

### 5.2 User fee calculation (`reconciliation/user_fee_calculation.py`)

| Parameter | Default | Formula |
|-----------|---------|---------|
| `USER_FEE_RATE` | `0.0325` (3.25%) | `expected_user_fee = ROUND(total_premium_amount × USER_FEE_RATE, 4)` |

- Reconciliation **overwrites** parse-time `user_fee_amount` if null
- Fee reports sum `expected_user_fee`, refund-eligible fees, withheld fees, revenue at risk

### 5.3 90-day cancellation rules (`reconciliation/business_rules.py`)

Controlled by `CANCELLATION_WINDOW_DAYS` (default **90**).

**Date fields used:**

| Field | Role |
|-------|------|
| `benefit_effective_date` | Coverage start date |
| `member_maint_effective_date` | Transaction / cancel date |
| `benefit_end_date` | Coverage end (for timing calculations) |

**Classification logic (`classify_record()`):**

| Action | Days (effective → transaction) | `transaction_classification` | `cancellation_window_status` | `refund_eligibility` |
|--------|-------------------------------|------------------------------|------------------------------|----------------------|
| CONFIRM | — | `CONFIRMATION` | `N/A` | `N/A` |
| CANCEL | ≤ 90 | `CANCELLATION` | `WITHIN_90_DAYS` | `REFUND_REQUIRED` |
| CANCEL | > 90 | `TERMINATION` (reclassified) | `OUTSIDE_90_DAYS` | `NO_REFUND_TERMINATION` |
| TERM | — | `TERMINATION` | `OUTSIDE_90_DAYS` | `NO_REFUND_TERMINATION` |
| Missing dates | — | `UNKNOWN_REVIEW` | `UNKNOWN_DATE` | `REVIEW_REQUIRED` |

**Fee impact fields:**

| Field | When set |
|-------|----------|
| `refund_eligible_user_fee` | Cancel within 90 days → equals `expected_user_fee` |
| `withheld_user_fee` | Cancel after 90 days or explicit termination |
| `revenue_at_risk` | Refund-required cancellations |

**Derived timing fields:**

- `days_between_effective_and_cancel`
- `months_between_effective_and_cancel` (days ÷ 30.44)
- `reporting_month` = `{year}-{month}` from source partition

### 5.4 Cancellation analysis (`reconciliation/cancellation_analysis.py`)

| Report | Logic |
|--------|-------|
| Repeated cancel | Same issuer + policy + member with > 1 cancel action |
| Cancel without confirm | Cancel record with no prior CONFIRM in earlier period |
| Cancellation gap | CONFIRM in month A, CANCEL in later month B (same policy + member) |

---

## 6. KPI Definitions

KPIs are computed in two layers: **assets export** (`KpiBuilder`) and **reconciliation reports** (SQL over staging DB).

### 6.1 KpiBuilder metrics (`src/transform/kpi_builder.py`)

Used for Excel exports, SQLite asset DBs, and Plotly dashboards.

| Metric | Definition | Unique? |
|--------|------------|---------|
| `total_enrollees` | `len(df)` — total row count | No — includes all maintenance rows |
| `total_subscribers` | Rows where `subscriber_flag == 'Y'` | No |
| `total_dependents` | Rows where `subscriber_flag == 'N'` | No |
| `unique_policies` | `nunique(exchg_assigned_policy_id)` | Yes — distinct policies |
| `unique_members` | `nunique(exchg_indiv_identifier)` | Yes — distinct members |
| `unique_households` | `nunique(household_or_employee_case_id)` | Yes — distinct households |
| `duplicate_member_count` | Rows where `(issuer_id, exchg_indiv_identifier)` appears more than once | Detection only |
| `duplicate_policy_member_count` | Rows where `(issuer_id, policy_id, member_id)` duplicated | Detection only |
| `total_enrollment_records` | Unique groups on `source_file + st02 + gs06 + qtyt` | EDI segment proxy |
| `total_premium_amount` | Sum of `total_premium_amt` | — |
| `average_premium_amount` | Mean of `total_premium_amt` | — |

### 6.2 How uniqueness is enforced

| Level | Mechanism | Behavior |
|-------|-----------|----------|
| **File level** | SHA-256 hash in `raw_file_inventory` | Duplicate files skipped entirely |
| **Member level** | `unique_members` KPI + duplicate counters | Counted and flagged; rows are **not** dropped |
| **Cross-file duplicates** | Data quality validator | WARN status; rows retained |
| **Within-file duplicates** | Data quality validator | FAIL status; rows retained |
| **Subscriber vs dependent** | `subscriber_flag` field (`Y` / `N`) | Counted separately; not deduplicated |

There is **no automatic deduplication** of enrollee rows. Analysts use duplicate counts and validation reports to identify data quality issues.

### 6.3 Reconciliation KPIs (`reconciliation/policy_lifecycle.py`)

SQL-based lifecycle metrics per issuer/year/month:

| Metric | Source |
|--------|--------|
| `confirmed_policies` | `transaction_classification = CONFIRMATION` or `additional_maint_reason_code = CONFIRM` |
| `cancelled_policies` | `transaction_classification = CANCELLATION` |
| `terminated_policies` | `transaction_classification = TERMINATION` |
| `distinct_policies` | `COUNT(DISTINCT policy_id)` |
| `distinct_members` | `COUNT(DISTINCT member_id)` |
| `within_90_day_cancels` | `cancellation_window_status = WITHIN_90_DAYS` |
| `outside_90_day_cancels` | `cancellation_window_status = OUTSIDE_90_DAYS` |
| `refund_eligible_user_fee` | Sum of fees where refund required |
| `withheld_user_fee` | Sum of fees where no refund |
| `revenue_at_risk` | Sum of at-risk revenue from early cancellations |

### 6.4 Rolling 3-month KPIs

Calendar windows within each year: Jan–Mar, Feb–Apr, … Oct–Dec. Monthly KPI columns are summed across the included months.

### 6.5 Dimensional breakdowns (KpiBuilder)

These feed Excel sheets and dashboard charts:

| Breakdown key | Grouped by |
|---------------|------------|
| `member_count_by_subscriber_flag` | `subscriber_flag` |
| `member_count_by_relationship_code` | `relationship_code` |
| `member_count_by_event_type` | `event_type_code` |
| `member_count_by_event_reason` | `event_reason_code` |
| `member_count_by_maintenance_type` | `maintenance_type_code` |
| `member_count_by_insurance_type` | `insurance_type_code` |
| `member_count_by_rating_area` | `rating_area` |
| `member_count_by_effective_month` | Month from `benefit_effective_begin_date` |
| `enrollee_count_by_file` | `source_file` |
| `premium_by_rating_area` | Sum of premium by `rating_area` |
| `premium_by_effective_month` | Sum of premium by effective month |
| `member_count_by_source_period` | `source_period` (rollup only) |
| `premium_by_source_period` | Premium by period (rollup only) |

---

## 7. Reports

### 7.1 Validation reports → `reports/validation/`

Generated by `validation/load_validation.py`:

| File | Contents |
|------|----------|
| `{issuer\|all}_load_validation.xlsx` | Sheet `counts`: validation type, issuer, year, month, action, row counts, reference match |
| `row_count_by_month_action.csv` | Count subset |
| `parse_errors.csv` | All parse error log entries |
| `missing_required_fields.csv` | Rows with null policy_id, member_id, or benefit date |

Optional reference matching via `REFERENCE_ROW_COUNTS=issuer=count,...` in `.env`.

### 7.2 KPI reports → `reports/kpi/`

Generated by `reporting/report_runner.py` → `run_kpi_reports()`:

| File | Source module | Key columns |
|------|---------------|-------------|
| `issuer_kpi_summary.xlsx` | `policy_lifecycle.issuer_kpi_summary` | confirmed/cancelled/terminated counts, 90-day splits, fee totals, distinct policies/members |
| `user_fee_validation.xlsx` | `user_fee_calculation` | record counts, total fees, premiums; sheet `refunds` |
| `repeated_cancel_report.xlsx` | `cancellation_analysis` | policy, member, cancel count, periods |
| `cancellation_gap_report.xlsx` | `cancellation_analysis` | confirm month → cancel month gaps |
| `{issuer\|all}_cancel_without_confirm.xlsx` | `cancellation_analysis` | cancels with no prior confirm |
| `premium_mismatch_report.xlsx` | `premium_validation` | IR + APTC vs premium mismatches |
| `cancellation_window_summary.xlsx` | `cancellation_window` | classification, window status, refund eligibility, fee sums |
| `rolling_3_month_kpi_summary.xlsx` | `policy_lifecycle.rolling_3_month_kpi` | rolling 3-month aggregates |
| `refund_eligibility_report.xlsx` | `policy_lifecycle` | per-record timing and fee detail; sheet `refund_summary` |
| `{issuer\|all}_household_counts.xlsx` | `policy_lifecycle` | members and subscribers per policy |

### 7.3 SFTP ingestion reports → `reports/`

When `PROCESSING_MODE=sftp`:

| File | When |
|------|------|
| `sftp_ingestion_summary.csv` | After SFTP audit or download |
| `sftp_ingestion_summary.xlsx` | Same data in Excel format |

Columns: issuer, year, month, folders_scanned, max_depth, files_scanned, valid_xml/gz/xz counts, downloaded, existing, failed, skip counts, local_xml_final_count, missing_count, status.

---

## 8. Plotly HTML Dashboards

**Module:** `src/dashboard/plotly_dashboard.py` → `PlotlyDashboard.generate()`

**Invoked by:** `pipeline/assets_exporter.py` (primary pipeline)

### 8.1 Output locations

| Scope | Path |
|-------|------|
| Monthly partition | `assets/{issuer}/{year}/{month}/dashboards/issuer_{issuer}_{year}_{month}_dashboard.html` |
| Issuer rollup | `assets/{issuer}/rollups/dashboards/issuer_{issuer}_all_periods_dashboard.html` |

### 8.2 Generation parameters

```python
dashboard.generate(
    kpis,              # Full dict from KpiBuilder.build_kpis()
    kpi_summary_df,    # Scalar KPI table (metric / value)
    validation_df,     # Data quality check results
    missingness_df,    # Column missingness profile
    output_stem,       # e.g. "13535_2026_04"
    output_dir,        # dashboards/ folder
    title="...",       # Page title
    is_rollup=False,   # True for all-periods rollup
)
```

**HTML settings:** `include_plotlyjs="cdn"`, `full_html=True`, height 1600px, `plotly_white` template.

### 8.3 Dashboard panels (4 rows × 2 columns)

| Position | Chart title | Data source | Partition | Rollup |
|----------|-------------|-------------|-----------|--------|
| Row 1, Col 1 | **KPI Summary** | `kpi_summary_df` (metric/value table) | Both | Both |
| Row 1, Col 2 | **Enrollees by Source File** | `kpis["enrollee_count_by_file"]` | Bar chart | — |
| Row 1, Col 2 | **Enrollees by Period** | `kpis["member_count_by_source_period"]` | — | Bar chart |
| Row 2, Col 1 | **Subscribers vs Dependents** | `kpis["member_count_by_subscriber_flag"]` | Pie (`Y`=Subscriber, `N`=Dependent) | Same |
| Row 2, Col 2 | **Premium by Rating Area** | `kpis["premium_by_rating_area"]` | Bar chart | Same |
| Row 3, Col 1 | **Members by Effective Month** | `kpis["member_count_by_effective_month"]` | Bar chart | — |
| Row 3, Col 1 | **Premium by Period** | `kpis["premium_by_source_period"]` | — | Bar chart |
| Row 3, Col 2 | **Validation Issue Summary** | `validation_df["status"]` counts | Bar (PASS=green, WARN=orange, FAIL=red) | Same |
| Row 4, Col 1 | **Missingness by Column (Top 15)** | `missingness_df` head 15 | Horizontal bar | Same |
| Row 4, Col 2 | **Duplicate Count Summary** | `duplicate_member_count`, `duplicate_policy_member_count` | Indicator gauges | Same |

Open any `.html` file in a browser — no web server required. Plotly.js is loaded from CDN.

---

## 9. Database Tables

### 9.1 Central staging database (`data/issuer_834.db`)

| Table | Purpose |
|-------|---------|
| `raw_file_inventory` | File metadata; `file_hash` unique index for dedup |
| `stg_834_records` | All parsed + reconciliation-enriched enrollee rows |
| `parse_errors` | Failed file parse log |

Key `stg_834_records` columns after reconciliation:

- Identity: `issuer`, `year`, `month`, `policy_id`, `member_id`, `subscriber_id`
- Premiums: `total_premium_amount`, `individual_responsibility_amount`, `aptc_amount`
- Dates: `benefit_effective_date`, `member_maint_effective_date`, `benefit_end_date`
- Fees: `user_fee_amount`, `expected_user_fee`, `refund_eligible_user_fee`, `withheld_user_fee`, `revenue_at_risk`
- Rules: `transaction_classification`, `cancellation_window_status`, `refund_eligibility`, `premium_validation_status`
- Audit: `raw_payload`, `action_code_description`

### 9.2 Per-partition asset SQLite (`assets/.../sqlite/`)

| Partition table | Rollup table |
|-----------------|--------------|
| `issuer_enrollees` | `issuer_enrollees_all_periods` |
| `issuer_kpis` | `issuer_kpis_all_periods` |
| `validation_results` | `validation_results_all_periods` |

---

## 10. Configuration Reference

| Variable | Default | Controls |
|----------|---------|----------|
| `PROCESSING_MODE` | `local` | `local` \| `ftp` \| `sftp` |
| `CLEAN_ON_START` | `true` | Wipe outputs before each run |
| `SOURCE_DATA_PATH` | `source_data` | Input XML root |
| `DATABASE_PATH` | `data/issuer_834.db` | Central SQLite |
| `REPORTS_PATH` | `reports` | Validation + KPI reports |
| `ASSETS_PATH` | `assets` | Partition exports + dashboards |
| `EXTRACTED_PATH` | `extracted` | Zip extraction target |
| `LOGS_PATH` | `logs` | Log files |
| `ISSUER_FILTER` | (empty = all) | Comma-separated 5-digit issuers |
| `YEAR_FILTER` | (empty = all) | Comma-separated years |
| `MONTH_FILTER` | (empty = all) | Comma-separated months (`1` or `01`) |
| `USER_FEE_RATE` | `0.0325` | User fee percentage |
| `CANCELLATION_WINDOW_DAYS` | `90` | Cancel vs termination threshold |
| `REFERENCE_ROW_COUNTS` | — | `issuer=count,...` for validation |
| `SFTP_HOST/PORT/USERNAME/PASSWORD` | — | SFTP connection |
| `SFTP_REMOTE_PATH` | `/archive/in/good/834` | Remote SFTP root |
| `SFTP_AUDIT_ONLY` | `false` | Scan remote without download |
| `FORCE_DOWNLOAD` | `false` | Re-download existing local XML |
| `KEEP_COMPRESSED` | `false` | Keep `.gz`/`.xz` after decompress |

**CLI overrides:** `--issuer`, `--year`, `--month`, `--no-clean`

---

## 11. Output Directory Layout

```
source_data/{issuer}/{year}/{month}/*.xml     ← input (flat, no day/batch folders)

data/issuer_834.db                            ← central staging DB

reports/
  validation/                                 ← load validation Excel/CSV
  kpi/                                        ← reconciliation KPI Excel files
  sftp_ingestion_summary.csv/.xlsx            ← SFTP audit/download summary

assets/{issuer}/{year}/{month}/
  excel/
    cleaned_enrollees_{issuer}_{year}_{month}.xlsx
    kpi_summary_{issuer}_{year}_{month}.xlsx
    validation_report_{issuer}_{year}_{month}.xlsx
  cleaned_xml/
    cleaned_enrollees_{issuer}_{year}_{month}.xml
  sqlite/
    issuer_{issuer}_{year}_{month}.db
  dashboards/
    issuer_{issuer}_{year}_{month}_dashboard.html
  validation_reports/
    validation_report_{issuer}_{year}_{month}.json

assets/{issuer}/rollups/
  excel/, sqlite/, dashboards/                ← all-periods combined
```

### KPI Excel sheets (`src/load/excel_exporter.py`)

**Partition exports:** `summary`, `subscriber_flag`, `relationship_code`, `event_type`, `event_reason`, `maintenance_type`, `insurance_type`, `rating_area`, `effective_month`, `premium_rating_area`, `premium_effective_month`, `file_trend`, `enrollee_by_file`

**Rollup exports:** same sheets plus `source_period`, `premium_period`

---

## 12. Standalone Runners

| Script | Purpose |
|--------|---------|
| `main.py` | Full pipeline (ingest → reconcile → validate → report → assets) |
| `run_validation.py` | Validation reports only (existing DB) |
| `run_kpi_reports.py` | Re-runs reconciliation + KPI reports on existing DB |
| `src/main.py` | **Legacy** — local-only path with `DataCleaner` and richer `Xml834Parser` |

### Typical commands

```bash
# Full pipeline (local data)
python main.py

# Filtered run
python main.py --issuer 86637 --year 2026 --month 02

# SFTP audit only (no download, no parser)
# Set PROCESSING_MODE=sftp and SFTP_AUDIT_ONLY=true in .env
python main.py

# Windows
.\.venv\Scripts\python.exe main.py
```

---

## Quick Reference: Subscriber & Enrollee Logic

```
834 XML file
  └── enrollment (action: CONFIRM / CANCEL / TERM)
        └── enrollee (one row per member maintenance event)
              ├── subscriber_flag = Y  →  counted as subscriber
              ├── subscriber_flag = N  →  counted as dependent
              └── exchg_indiv_identifier  →  unique member ID for dedup reporting
```

- **One row = one maintenance event**, not necessarily one unique person
- A member can appear multiple times (confirm, cancel, reinstate) → `total_enrollees` > `unique_members`
- Subscribers and dependents are distinguished by `subscriber_flag`, not by deduplication
- Business rules classify each row's action and compute refund eligibility based on the 90-day window

---

*Last updated: reflects SFTP recursive ingestion, `.xml.xz` support, unified partition filters, and `sftp_ingestion_summary` reporting.*
