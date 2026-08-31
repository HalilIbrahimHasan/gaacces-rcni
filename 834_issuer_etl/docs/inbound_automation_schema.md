# Inbound Automation Schema & Implementation Plan

**Status:** Schema **approved** — implementation **blocked** until explicit go-ahead  
**Last updated:** 2026-07-08

## Goal

Production-safe raw ingestion runner that discovers XML from `source_data`, parses with existing `Parser834` (unmodified), and loads rows into **new** Azure SQL tables only:

| Table | Purpose |
|-------|---------|
| `dbo.inbound_automation` | One row per `<enrollee>` — full Parser834 output |
| `dbo.inbound_automation_run_log` | One row per load run |
| `dbo.inbound_automation_file_log` | One row per file (idempotency + parse metrics) |

**Boundaries (unchanged):**

- Do **not** modify `dbo.834_Inbound_test`, `Enrollments_*`, reconciliation tables, or any existing SQL object
- Do **not** modify `Parser834`, business-ready, Chandra, or lifecycle/collapse pipelines
- DDL is `CREATE TABLE IF NOT EXISTS` + `CREATE INDEX IF NOT EXISTS` only — no DROP/TRUNCATE/ALTER

**DDL artifact:** [`sql/inbound_automation_ddl.sql`](../sql/inbound_automation_ddl.sql)

---

## Schema preservation policy

Every key produced by `Parser834.parse_file()` must either:

1. Exist as a **dedicated SQL column** (exact snake_case name), or
2. Be preserved inside **`raw_json`**

**Approved approach:**

- All **43 Parser834 keys** → dedicated columns
- All **automation metadata** → dedicated columns
- **Derived** `insurance_type`, `enrolleeStatus` → dedicated columns
- **`raw_payload`** → dedicated column (parser key #43)
- **`raw_json`** → lossless enriched backup (superset, not substitute)

**Nothing parser-produced is `raw_json`-only.**

---

## Parser834 field inventory (43 keys)

Fixed by `parsers/parser_834.py` — one dict per `<enrollee>`.

| # | Parser key | SQL column | SQL type | PII |
|---|------------|------------|----------|-----|
| 1 | `policy_id` | `policy_id` | `NVARCHAR(100)` | |
| 2 | `member_id` | `member_id` | `NVARCHAR(100)` | **yes** |
| 3 | `subscriber_id` | `subscriber_id` | `NVARCHAR(100)` | **yes** |
| 4 | `exchg_assigned_enrollee_id` | `exchg_assigned_enrollee_id` | `NVARCHAR(100)` | **yes** |
| 5 | `issuer_subscriber_identifier` | `issuer_subscriber_identifier` | `NVARCHAR(100)` | **yes** |
| 6 | `issuer_indiv_identifier` | `issuer_indiv_identifier` | `NVARCHAR(100)` | **yes** |
| 7 | `member_first_name` | `member_first_name` | `NVARCHAR(200)` | **yes** |
| 8 | `member_last_name` | `member_last_name` | `NVARCHAR(200)` | **yes** |
| 9 | `relationship` | `relationship` | `NVARCHAR(50)` | |
| 10 | `subscriber_flag` | `subscriber_flag` | `NVARCHAR(20)` | |
| 11 | `enrollee_event_type_code` | `enrollee_event_type_code` | `NVARCHAR(50)` | |
| 12 | `enrollee_event_reason_code` | `enrollee_event_reason_code` | `NVARCHAR(50)` | |
| 13 | `action_code_description` | `action_code_description` | `NVARCHAR(100)` | |
| 14 | `maintenance_type_code` | `maintenance_type_code` | `NVARCHAR(50)` | |
| 15 | `additional_maint_reason_code` | `additional_maint_reason_code` | `NVARCHAR(50)` | |
| 16 | `coverage_status` | `coverage_status` | `NVARCHAR(100)` | |
| 17 | `benefit_effective_date` | `benefit_effective_date` | `DATE` | |
| 18 | `benefit_end_date` | `benefit_end_date` | `DATE` | |
| 19 | `member_maint_effective_date` | `member_maint_effective_date` | `DATE` | |
| 20 | `last_premium_paid_date` | `last_premium_paid_date` | `NVARCHAR(20)` | |
| 21 | `request_submit_timestamp` | `request_submit_timestamp` | `NVARCHAR(100)` | |
| 22 | `total_premium_amount` | `total_premium_amount` | `DECIMAL(18,4)` | |
| 23 | `individual_responsibility_amount` | `individual_responsibility_amount` | `DECIMAL(18,4)` | |
| 24 | `aptc_amount` | `aptc_amount` | `DECIMAL(18,4)` | |
| 25 | `user_fee_amount` | `user_fee_amount` | `DECIMAL(18,4)` | |
| 26 | `insurance_type_code` | `insurance_type_code` | `NVARCHAR(50)` | |
| 27 | `health_coverage_policy_no` | `health_coverage_policy_no` | `NVARCHAR(100)` | |
| 28 | `household_or_employee_case_id` | `household_or_employee_case_id` | `NVARCHAR(100)` | |
| 29 | `rating_area` | `rating_area` | `NVARCHAR(50)` | |
| 30 | `source_exchg_id` | `source_exchg_id` | `NVARCHAR(100)` | |
| 31 | `enrollment_action_code` | `enrollment_action_code` | `NVARCHAR(50)` | |
| 32 | `insurer_tax_id_number` | `insurer_tax_id_number` | `NVARCHAR(50)` | |
| 33 | `qtyn` | `qtyn` | `NVARCHAR(50)` | |
| 34 | `qtyy` | `qtyy` | `NVARCHAR(50)` | |
| 35 | `qtyt` | `qtyt` | `NVARCHAR(50)` | |
| 36 | `issuer` | `issuer` | `NVARCHAR(20)` | |
| 37 | `year` | `year` | `NVARCHAR(4)` | |
| 38 | `month` | `month` | `NVARCHAR(2)` | |
| 39 | `file_name` | `file_name` | `NVARCHAR(500)` | |
| 40 | `raw_xml_path` | `raw_xml_path` | `NVARCHAR(1000)` | |
| 41 | `created_at` | `created_at` | `NVARCHAR(40)` | |
| 42 | `action_code` | `action_code` | `NVARCHAR(50)` | |
| 43 | `raw_payload` | `raw_payload` | `NVARCHAR(MAX)` | |

---

## PII fields

The following columns contain **personally identifiable information** and require appropriate Azure access controls, encryption-at-rest, and data-handling approval:

| Column | Category |
|--------|----------|
| `member_first_name` | Name |
| `member_last_name` | Name |
| `member_id` | Member identifier |
| `subscriber_id` | Member identifier |
| `exchg_assigned_enrollee_id` | Member identifier (exchange-assigned) |
| `issuer_subscriber_identifier` | Member identifier (issuer-assigned) |
| `issuer_indiv_identifier` | Member identifier (issuer-assigned) |

**Note:** `raw_payload` and `raw_json` also embed PII from the columns above. Treat both as PII-bearing at rest.

---

## `dbo.inbound_automation` — complete column list (64 columns)

### A. Surrogate / run (3)

| Column | Type | Nullable |
|--------|------|----------|
| `id` | `BIGINT IDENTITY` PK | no |
| `load_run_id` | `NVARCHAR(100)` | no |
| `loaded_at` | `DATETIME2(3)` | no |

### B. Automation lineage (9)

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `folder_year` | `INT` | no | From `source_data/<issuer>/<year>/<month>/` |
| `folder_month` | `INT` | no | |
| `filename_file_year` | `INT` | yes | Parsed from filename timestamp |
| `filename_file_month` | `INT` | yes | |
| `source_file` | `NVARCHAR(500)` | no | Canonical file name |
| `source_file_path` | `NVARCHAR(1000)` | no | Discovered path |
| `file_hash` | `NVARCHAR(128)` | no | SHA-256 of file bytes |
| `row_number_in_file` | `INT` | no | 1..N in parser return order |
| `raw_record_hash` | `NVARCHAR(128)` | no | SHA-256 of canonical enriched JSON |

### C. Provenance / coverage (6) — **approved additions**

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `parser_version` | `NVARCHAR(50)` | yes | Parser module version label |
| `runner_version` | `NVARCHAR(50)` | yes | Inbound automation runner version |
| `git_commit` | `NVARCHAR(100)` | yes | Git SHA at load time |
| `coverage_year` | `INT` | yes | Business coverage year for the row |
| `coverage_year_source` | `NVARCHAR(50)` | yes | How `coverage_year` was resolved |
| `warning_count` | `INT` | yes | Warnings emitted for this row during parse/enrich |

**`coverage_year_source` allowed values (implementation):**

| Value | Meaning |
|-------|---------|
| `cli_filter` | From `--year` CLI argument |
| `folder_year` | From `source_data` partition folder |
| `filename_year` | From filename timestamp parse |
| `benefit_effective_year` | Derived from `benefit_effective_date` |
| `unresolved` | Could not determine |

### D. Derived (2)

| Column | Type | Source |
|--------|------|--------|
| `insurance_type` | `NVARCHAR(50)` | `_insurance_type_display(insurance_type_code)` |
| `enrolleeStatus` | `NVARCHAR(50)` | `_resolve_enrollee_status()` or `UNMAPPED` |

### E. Parser834 keys (43)

See inventory table above. All promoted as dedicated columns with exact parser names.

### F. Lossless backup (1)

| Column | Type | Content |
|--------|------|---------|
| `raw_json` | `NVARCHAR(MAX)` | Full enriched row JSON (see below) |

### Intentional overlap (audit fidelity)

| Parser key | Automation column | Relationship |
|------------|-------------------|--------------|
| `issuer` | `issuer` | Same value |
| `year` | `folder_year` | Parser string vs typed `INT` |
| `month` | `folder_month` | Parser `"01"` vs typed `INT` |
| `file_name` | `source_file` | Same value |
| `raw_xml_path` | `source_file_path` | Same value |

Keep both parser-named and automation-named columns.

### `raw_payload` vs `raw_json`

| Field | Content |
|-------|---------|
| `raw_payload` | Parser's `json.dumps(row)` **excluding** the `raw_payload` key |
| `raw_json` | Runner's lossless JSON of the **full enriched row** |

### `raw_json` construction rule

```text
raw_json = {
  ...parser_row,                    # all 43 keys exactly as returned
  folder_year, folder_month,
  filename_file_year, filename_file_month,
  source_file, source_file_path,
  file_hash, row_number_in_file, raw_record_hash,
  load_run_id, loaded_at,
  parser_version, runner_version, git_commit,
  coverage_year, coverage_year_source, warning_count,
  insurance_type, enrolleeStatus
}
```

Use `sort_keys=True` when computing `raw_record_hash`.

### Constraints & indexes

- `PK`: `id`
- `UNIQUE`: `(file_hash, row_number_in_file)` — row-level idempotency safety net
- Indexes: `source_file`, `(issuer, folder_year, folder_month)`, `(issuer, filename_file_year, filename_file_month)`, `(issuer, coverage_year)`, `policy_id`, `member_id`, `load_run_id`, `file_hash`, `enrolleeStatus`, `insurance_type`

---

## `dbo.inbound_automation_run_log`

Run-level audit. Canonical home for `parser_version`, `runner_version`, `git_commit` (also denormalized to each row).

| Column | Type | Notes |
|--------|------|-------|
| `load_run_id` | `NVARCHAR(100)` PK | |
| `started_at` | `DATETIME2(3)` | |
| `completed_at` | `DATETIME2(3)` | |
| `run_mode` | `NVARCHAR(20)` | `dry_run` \| `load` \| `create_table` |
| `source_mode` | `NVARCHAR(20)` | `local` \| `sftp` (future) |
| `year_filter` | `NVARCHAR(50)` | e.g. `2025` or `ALL` |
| `issuer_filter` | `NVARCHAR(200)` | |
| `month_filter` | `NVARCHAR(50)` | |
| `parser_version` | `NVARCHAR(50)` | |
| `runner_version` | `NVARCHAR(50)` | |
| `git_commit` | `NVARCHAR(100)` | |
| `files_discovered` | `INT` | |
| `files_parsed` | `INT` | |
| `files_loaded` | `INT` | |
| `files_skipped_duplicate` | `INT` | |
| `files_failed` | `INT` | |
| `rows_parsed` | `INT` | |
| `rows_inserted` | `INT` | |
| `rows_skipped` | `INT` | |
| `total_warning_count` | `INT` | Sum of row `warning_count` values |
| `status` | `NVARCHAR(20)` | `running` \| `success` \| `failed` \| `dry_run` |
| `error_summary` | `NVARCHAR(MAX)` | |
| `report_output_path` | `NVARCHAR(1000)` | |

---

## `dbo.inbound_automation_file_log`

Per-file idempotency and parse metrics.

| Column | Type | Notes |
|--------|------|-------|
| `id` | `BIGINT IDENTITY` PK | |
| `load_run_id` | `NVARCHAR(100)` | |
| `loaded_at` | `DATETIME2(3)` | |
| `issuer` | `NVARCHAR(20)` | |
| `folder_year` | `INT` | |
| `folder_month` | `INT` | |
| `filename_file_year` | `INT` | |
| `filename_file_month` | `INT` | |
| `source_file` | `NVARCHAR(500)` | |
| `source_file_path` | `NVARCHAR(1000)` | |
| `file_hash` | `NVARCHAR(128)` UNIQUE | File-level skip key |
| `file_size_bytes` | `BIGINT` | |
| `parse_status` | `NVARCHAR(20)` | `loaded` \| `skipped_duplicate` \| `failed` \| `dry_run` |
| `row_count` | `INT` | |
| `parse_duration_ms` | `INT` | **Approved addition** — wall-clock parse time per file |
| `error_message` | `NVARCHAR(MAX)` | |

---

## Implementation plan (not started)

### Modules to reuse

| Module | Function |
|--------|----------|
| `ingestion/file_discovery.py` | `discover_source_files()` |
| `ingestion/xml_reader.py` | `read_xml_bytes()` |
| `parsers/parser_834.py` | `Parser834.parse_file()` — unmodified |
| `database/loaders.py` | `file_hash()` |
| `config/config.py` | `settings.source_data_path` |
| `azure_reconciliation/azure_client.py` | `connect_azure()` — read path only; **do not modify** |
| `src/transform/enrollment_summary.py` | `_resolve_enrollee_status`, `_insurance_type_display` |
| `raw_validation/raw_validation_common.py` | `parse_filename_year_month()` — extract, don't couple |

### New package (planned)

```
834_issuer_etl/
├── run_inbound_automation_load.py
├── inbound_automation/
│   ├── cli.py, discovery.py, enrich.py, ddl.py
│   ├── azure_writer.py, idempotency.py, run_context.py
│   ├── reports.py, constants.py
└── sql/inbound_automation_ddl.sql
```

### CLI modes

| Flag | Effect |
|------|--------|
| `--dry-run` | Discover + parse + reports; no Azure writes |
| `--create-table` | Execute DDL (`IF NOT EXISTS` only) |
| `--load` | Insert rows + logs |

`--year` required unless `--all-years`. Optional `--issuer`, `--month`.

### Idempotency (hybrid)

1. **File level:** skip if `file_hash` exists in `inbound_automation_file_log`
2. **Row level:** `UNIQUE (file_hash, row_number_in_file)` on `inbound_automation`

### Load flow

Discover → hash → skip-if-seen → parse → enrich (status, insurance, coverage_year, provenance) → dry-run reports OR batch insert → file_log + run_log → Excel reports under `outputs/inbound_automation/<load_run_id>/`.

Per-file transactions recommended.

### Safety controls

- Writer allowlist: only the 3 new tables
- No DROP/TRUNCATE/ALTER on any table
- Explicit modes required (no default load)
- Optional `INBOUND_AUTOMATION_ENABLED=true` env gate
- Continue on per-file parse failure

### Suggested implementation order

1. DDL + `--create-table`
2. Discovery + enrich + `--dry-run` + reports
3. Azure writer + `--load` + idempotency
4. Hardening (env gate, permissions, batch tuning)

---

## Pre-implementation checklist

| Step | Status |
|------|--------|
| Static parser key inventory (43 keys) | Done |
| Sample parse verification | Done (smoke file) |
| Full multi-issuer sample re-scan | Pending (needs prod `source_data`) |
| Column promotion list | Done (all 43 promoted) |
| `raw_json` lossless rule | Defined |
| PII fields documented | Done |
| Provenance columns (`parser_version`, etc.) | Approved |
| `coverage_year` / `coverage_year_source` | Approved |
| `parse_duration_ms` on file_log | Approved |
| Schema / DDL review | **Approved** |
| Phase 1 dry-run implementation | **Done** |
| Azure load (Phase 2+) | **Blocked** |

---

## Resolved questions (from prior review)

| Question | Resolution |
|----------|------------|
| `coverage_year` column needed? | **Yes** — `INT NULL` + `coverage_year_source` |
| `raw_payload` + `raw_json` both? | **Yes** — both kept |
| PII in Azure? | Documented; requires org approval for load |
| All parser keys as columns? | **Yes** — all 43 |

## Open questions (implementation phase)

1. Azure AAD principal permissions for CREATE TABLE + INSERT
2. `INBOUND_AUTOMATION_ENABLED` env gate — adopt?
3. Re-load policy for changed file hashes (new hash = new rows in v1)
4. `--all-years` requires `--confirm-all-years`?
5. Zip path vs extracted path in `source_file_path`
6. DDL via Python vs standalone SQL for DBA (both planned: `.sql` + `ddl.py`)
