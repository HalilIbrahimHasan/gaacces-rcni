# RCNI Phase 2 — Azure raw ingestion foundation (review, corrected)

Status: **proposed**. No Azure DDL was executed. No Azure INSERT was attempted.
`run_rcni.py` remains Phase 1 (discover/validate only).

---

## 1. Corrected DDL

Script: `sql/rcni_raw_ddl.sql`

SQL Server / Azure SQL guards only:

```sql
IF OBJECT_ID(N'dbo.<table>', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.<table> ( ... );
END;
GO
```

There is **no** `CREATE TABLE IF NOT EXISTS`. Indexes use `sys.indexes` existence
checks. No DROP / TRUNCATE / ALTER of existing objects.

---

## 2. Corrected `dbo.rcni_file_log`

`processing_status` and `file_disposition` are independent columns.

| Column | Values |
|---|---|
| `processing_status` | DISCOVERED, DOWNLOADING, DOWNLOADED, VALIDATING, LOADING, SUCCESS, FAILED, SKIPPED_DUPLICATE |
| `file_disposition` | NEW, DUPLICATE, POSSIBLE_REPLACEMENT |

Replacement that loaded:

- `processing_status = SUCCESS`
- `file_disposition = POSSIBLE_REPLACEMENT`

That file is included in normal SUCCESS queries.

---

## 3. Idempotency

Physical duplicate (same SHA-256 as a prior **SUCCESS** file):

- do not reload
- `processing_status = SKIPPED_DUPLICATE`
- `file_disposition = DUPLICATE`

Possible replacement (same `issuer_id + document_type + coverage_year + file_timestamp`, different SHA-256):

- load independently
- `processing_status = SUCCESS` if load succeeds
- `file_disposition = POSSIBLE_REPLACEMENT`
- prior raw rows are not overwritten or deleted

---

## 4. Revised transaction flow

Staging batches are **not** held in one enormous transaction with promote.

1. Stream source (`csv.reader`; gzip text or plain text).
2. Insert valid rows into `rcni_stage` in configurable batches; **commit each batch**.
3. Insert quality issues in **their own audit transactions**.
4. Reconcile: `source_records = staged_valid_records + structural_malformed_records`.
5. Only after reconcile succeeds:

   ```
   BEGIN TRANSACTION
     INSERT dbo.rcni_raw (...)
     SELECT ... FROM dbo.rcni_stage
     WHERE load_run_id = :run_id AND file_hash = :file_hash
     verify COUNT(rcni_raw) for that run/hash
   COMMIT
   ```

6. Then delete **that file’s** stage rows (separate transaction).

A promote rollback cannot leave partial `rcni_raw` rows. Stage leftovers from a
failed promote are not in `rcni_raw`. `rcni_raw` is never truncated.

---

## 5. Retry / stale-stage behavior

Every stage row is scoped by `(load_run_id, file_hash, row_number_in_file)`
with unique constraint `UX_rcni_stage_attempt`.

On load start (and on SKIPPED_DUPLICATE):

```sql
DELETE FROM dbo.rcni_stage WHERE file_hash = :file_hash
```

That removes stale rows from a failed attempt. It does **not** `TRUNCATE` the
shared stage table. Other files’ stage rows are untouched.

---

## 6. Quality issue durability

Quality rows are committed in their own audit writes **before** promote.

If promote fails, `rcni_data_quality_issue` rows remain.

Retry uniqueness: `UX_rcni_dq_natural`
`(file_hash, issue_row_key, issue_code, issue_column_key)`
where NULL row/column map to `-1` / `''` via persisted computed columns.

Inserts skip existing natural keys. No duplicate quality rows on retry.

---

## 7. Indexes and constraints

| Object | Constraint / index |
|---|---|
| `rcni_run_log` | PK `load_run_id` |
| `rcni_file_log` | PK `file_id`; `IX_rcni_file_log_hash (file_hash, processing_status)`; logical identity; run; disposition |
| `rcni_stage` | PK `stage_id`; **unique** `UX_rcni_stage_attempt (load_run_id, file_hash, row_number_in_file)`; `IX_rcni_stage_file_hash` |
| `rcni_raw` | PK `rcni_raw_id`; **unique** `UX_rcni_raw_file_row (file_hash, row_number_in_file)` |
| `rcni_data_quality_issue` | PK; **unique** `UX_rcni_dq_natural`; file/run indexes |

`row_number_in_file` is the 1-based source data-record ordinal (header
excluded). `csv.Error` and column-count mismatches consume a number so gzip
and plain streams of the same logical content produce identical numbers.
Physical line number is stored separately on quality issues.

---

## 8. Expected first Azure test (do not run yet)

Issuer **15105**, PY2026, May, 14,268 rows:

`834_issuer_etl/last reports/15105/2026/05/16/3066767_888586925866/to_15105_INDV_MONTHLYDISCREPANCY_2026_20260517000035.OUT.good`

| Check | Expected |
|---|---|
| source | 14,268 |
| parsed | 14,268 |
| staged | 14,268 |
| malformed | 0 |
| raw | 14,268 |
| difference | 0 |
| processing_status | SUCCESS |
| file_disposition | NEW |

---

## 9. Expected second Azure test — real malformed source (do not run yet)

Synthetic `tests/fixtures/rcni/malformed_extra_comma.csv` remains a unit fixture.

Before any broad load, run one **real** previously identified malformed RCNI
file. Preferred: **issuer 58081 / PY2025 / January** (Phase 1 validation:
674,682 parsed, 4 structural malformed). Alternates: 70893 or 83761 files
already flagged in Phase 1. That file is not in this workspace; it comes from
the live SFTP archive.

| Check | Expected example |
|---|---|
| source | 674,682 |
| valid | 674,678 |
| malformed | 4 |
| accounted | 674,682 |
| rcni_raw | 674,678 |
| DQ issues | 4 |
| unaccounted | 0 |
| processing_status | SUCCESS |
| file_disposition | NEW |

---

## 10. Performance metrics

`RCNI_AZURE_BATCH_SIZE` default **3000**. Per file the loader records:

- `batch_number`, `rows_in_batch`, `batch_duration_ms`, `rows_per_second`
- `total_stage_duration_ms`, `promote_duration_ms`, `total_file_duration_ms`

No premature optimization.

---

## 11. Tests updated

`tests/test_rcni_raw_ingest.py` (in-memory store only — no Azure):

- SUCCESS + NEW for a clean file, including batch timings
- gzip vs plain: same payload **and** same `row_number_in_file`
- extra comma → SUCCESS, 2 raw (rows 1 and 3) + 1 quality (row 2)
- SHA-256 skip → SKIPPED_DUPLICATE + DUPLICATE
- replacement → SUCCESS + POSSIBLE_REPLACEMENT; both hashes in raw
- promote failure → 0 raw, stage leftover, quality kept
- retry after promote failure → stale stage cleared, quality not duplicated, SUCCESS
- May 15105 parse-count 14,268 (skip if file absent)
