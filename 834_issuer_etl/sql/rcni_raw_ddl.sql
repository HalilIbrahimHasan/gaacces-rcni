-- =============================================================================
-- Azure SQL DDL: RCNI Phase 2 raw ingestion (NEW TABLES ONLY)
-- =============================================================================
-- Status: PROPOSED — do not execute until review/approval.
-- Does NOT modify dbo.834_Inbound_test, Enrollments_*, inbound_automation*,
-- or any existing table.
--
-- Tables:
--   1. dbo.rcni_run_log
--   2. dbo.rcni_file_log
--   3. dbo.rcni_stage
--   4. dbo.rcni_raw
--   5. dbo.rcni_data_quality_issue
--
-- Safety: SQL Server OBJECT_ID guards only. Do NOT use CREATE TABLE IF NOT EXISTS.
-- No DROP / TRUNCATE / ALTER of existing objects. Indexes use sys.indexes guards.
--
-- Grain of dbo.rcni_raw: one row = one parsed RCNI discrepancy record.
-- No member/policy/discrepancy deduplication. No latest-state logic.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. dbo.rcni_run_log
-- -----------------------------------------------------------------------------
IF OBJECT_ID(N'dbo.rcni_run_log', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.rcni_run_log
    (
        load_run_id             UNIQUEIDENTIFIER    NOT NULL
            CONSTRAINT PK_rcni_run_log PRIMARY KEY,
        started_at              DATETIME2(3)        NOT NULL
            CONSTRAINT DF_rcni_run_log_started_at DEFAULT SYSUTCDATETIME(),
        completed_at            DATETIME2(3)        NULL,
        run_mode                VARCHAR(30)         NOT NULL,   -- dry_run | load
        issuer_scope            NVARCHAR(200)       NULL,
        year_scope              NVARCHAR(50)        NULL,
        month_scope             NVARCHAR(50)        NULL,
        files_discovered        INT                 NOT NULL CONSTRAINT DF_rcni_run_log_disc DEFAULT 0,
        files_attempted         INT                 NOT NULL CONSTRAINT DF_rcni_run_log_att DEFAULT 0,
        files_successful        INT                 NOT NULL CONSTRAINT DF_rcni_run_log_ok DEFAULT 0,
        files_failed            INT                 NOT NULL CONSTRAINT DF_rcni_run_log_fail DEFAULT 0,
        files_skipped           INT                 NOT NULL CONSTRAINT DF_rcni_run_log_skip DEFAULT 0,
        rows_parsed             BIGINT              NOT NULL CONSTRAINT DF_rcni_run_log_parsed DEFAULT 0,
        rows_loaded             BIGINT              NOT NULL CONSTRAINT DF_rcni_run_log_loaded DEFAULT 0,
        rows_flagged            BIGINT              NOT NULL CONSTRAINT DF_rcni_run_log_flagged DEFAULT 0,
        status                  VARCHAR(30)         NOT NULL,   -- RUNNING | SUCCESS | FAILED | DRY_RUN
        error_message           NVARCHAR(MAX)       NULL
    );
END;
GO


-- -----------------------------------------------------------------------------
-- 2. dbo.rcni_file_log
-- -----------------------------------------------------------------------------
-- processing_status = pipeline outcome (SUCCESS/FAILED/SKIPPED_DUPLICATE/...).
-- file_disposition  = identity class (NEW/DUPLICATE/POSSIBLE_REPLACEMENT).
-- file_hash is indexed but NOT unique so skip/replacement attempts can be logged.
-- Idempotency: skip load when any SUCCESS row already exists for this SHA-256.
-- -----------------------------------------------------------------------------
IF OBJECT_ID(N'dbo.rcni_file_log', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.rcni_file_log
    (
        file_id                 BIGINT IDENTITY(1,1) NOT NULL
            CONSTRAINT PK_rcni_file_log PRIMARY KEY CLUSTERED,
        source_file             NVARCHAR(500)       NOT NULL,
        source_path             NVARCHAR(1500)      NOT NULL,
        issuer_id               VARCHAR(20)         NOT NULL,
        document_type           VARCHAR(50)         NOT NULL
            CONSTRAINT DF_rcni_file_log_doc DEFAULT 'INDV_MONTHLYDISCREPANCY',
        coverage_year           INT                 NULL,
        processing_year         INT                 NULL,
        processing_month        TINYINT             NULL,
        processing_day          TINYINT             NULL,
        file_timestamp          DATETIME2(3)        NULL,
        compression_type        VARCHAR(20)         NOT NULL,   -- gzip | none
        file_size_bytes         BIGINT              NULL,
        file_hash               CHAR(64)            NOT NULL,
        rows_read               BIGINT              NOT NULL CONSTRAINT DF_rcni_file_log_read DEFAULT 0,
        rows_parsed             BIGINT              NOT NULL CONSTRAINT DF_rcni_file_log_parsed DEFAULT 0,
        rows_loaded             BIGINT              NOT NULL CONSTRAINT DF_rcni_file_log_loaded DEFAULT 0,
        rows_flagged            BIGINT              NOT NULL CONSTRAINT DF_rcni_file_log_flagged DEFAULT 0,
        rows_rejected           BIGINT              NOT NULL CONSTRAINT DF_rcni_file_log_rej DEFAULT 0,
        processing_status       VARCHAR(30)         NOT NULL,
            -- DISCOVERED | DOWNLOADING | DOWNLOADED | VALIDATING | LOADING
            -- | SUCCESS | FAILED | SKIPPED_DUPLICATE
        file_disposition        VARCHAR(30)         NOT NULL
            CONSTRAINT DF_rcni_file_log_disposition DEFAULT 'NEW',
            -- NEW | DUPLICATE | POSSIBLE_REPLACEMENT
        error_message           NVARCHAR(MAX)       NULL,
        load_run_id             UNIQUEIDENTIFIER    NULL,
        first_seen_at           DATETIME2(3)        NOT NULL
            CONSTRAINT DF_rcni_file_log_first_seen DEFAULT SYSUTCDATETIME(),
        started_at              DATETIME2(3)        NULL,
        completed_at            DATETIME2(3)        NULL,
        loaded_at               DATETIME2(3)        NULL
    );
END;
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_rcni_file_log_hash' AND object_id = OBJECT_ID(N'dbo.rcni_file_log')
)
    CREATE INDEX IX_rcni_file_log_hash
        ON dbo.rcni_file_log (file_hash, processing_status);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_rcni_file_log_logical_identity'
        AND object_id = OBJECT_ID(N'dbo.rcni_file_log')
)
    CREATE INDEX IX_rcni_file_log_logical_identity
        ON dbo.rcni_file_log (issuer_id, document_type, coverage_year, file_timestamp);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_rcni_file_log_run' AND object_id = OBJECT_ID(N'dbo.rcni_file_log')
)
    CREATE INDEX IX_rcni_file_log_run
        ON dbo.rcni_file_log (load_run_id);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_rcni_file_log_disposition' AND object_id = OBJECT_ID(N'dbo.rcni_file_log')
)
    CREATE INDEX IX_rcni_file_log_disposition
        ON dbo.rcni_file_log (file_disposition, processing_status);
GO


-- -----------------------------------------------------------------------------
-- 3. dbo.rcni_stage
-- -----------------------------------------------------------------------------
-- Per-file staging. Same payload as rcni_raw minus rcni_raw_id / loaded_at.
-- Scoped by (load_run_id, file_hash, row_number_in_file).
-- Never TRUNCATE this table. Retry deletes only matching file_hash rows.
-- -----------------------------------------------------------------------------
IF OBJECT_ID(N'dbo.rcni_stage', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.rcni_stage
    (
        stage_id                            BIGINT IDENTITY(1,1) NOT NULL
            CONSTRAINT PK_rcni_stage PRIMARY KEY CLUSTERED,
        load_run_id                         UNIQUEIDENTIFIER    NOT NULL,
        file_hash                           CHAR(64)            NOT NULL,
        issuer_id                           VARCHAR(20)         NOT NULL,
        coverage_year                       INT                 NULL,
        processing_year                     INT                 NULL,
        processing_month                    TINYINT             NULL,
        processing_day                      TINYINT             NULL,
        file_timestamp                      DATETIME2(3)        NULL,
        source_file                         NVARCHAR(500)       NOT NULL,
        source_path                         NVARCHAR(1500)      NOT NULL,
        row_number_in_file                  BIGINT              NOT NULL,
        quality_status                      VARCHAR(30)         NOT NULL,
        exchange_assigned_policy_id         NVARCHAR(100)       NULL,
        plan_id                             NVARCHAR(100)       NULL,
        member_last_name                    NVARCHAR(255)       NULL,
        member_first_name                   NVARCHAR(255)       NULL,
        exchange_assigned_member_id         NVARCHAR(100)       NULL,
        issuer_assigned_member_id           NVARCHAR(100)       NULL,
        subscriber_last_name                NVARCHAR(255)       NULL,
        subscriber_first_name               NVARCHAR(255)       NULL,
        exchange_assigned_subscriber_id     NVARCHAR(100)       NULL,
        issuer_assigned_subscriber_id       NVARCHAR(100)       NULL,
        discrepancy_reason_code             NVARCHAR(100)       NULL,
        discrepancy_reason_text             NVARCHAR(500)       NULL,
        hix_value                           NVARCHAR(1000)      NULL,
        issuer_value                        NVARCHAR(1000)      NULL,
        date_of_discrepancy                 NVARCHAR(50)        NULL,
        recon_file_name                     NVARCHAR(255)       NULL,
        autofixed_by_hix                    NVARCHAR(50)        NULL,
        assignee                            NVARCHAR(100)       NULL,
        enrollment_status                   NVARCHAR(100)       NULL,
        CONSTRAINT UX_rcni_stage_attempt UNIQUE (load_run_id, file_hash, row_number_in_file)
    );
END;
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_rcni_stage_file_hash' AND object_id = OBJECT_ID(N'dbo.rcni_stage')
)
    CREATE INDEX IX_rcni_stage_file_hash
        ON dbo.rcni_stage (file_hash, load_run_id);
GO


-- -----------------------------------------------------------------------------
-- 4. dbo.rcni_raw
-- -----------------------------------------------------------------------------
-- ONE ROW = ONE PARSED RCNI DISCREPANCY RECORD.
-- Identifiers and Date of Discrepancy are text. HIX/Issuer are text.
-- raw_record is NOT stored here; malformed payload is in rcni_data_quality_issue.
-- row_number_in_file is the 1-based source data-record ordinal (header excluded),
-- identical for .OUT.good and .OUT.good.gz of the same logical content.
-- -----------------------------------------------------------------------------
IF OBJECT_ID(N'dbo.rcni_raw', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.rcni_raw
    (
        rcni_raw_id                         BIGINT IDENTITY(1,1) NOT NULL
            CONSTRAINT PK_rcni_raw PRIMARY KEY CLUSTERED,
        issuer_id                           VARCHAR(20)         NOT NULL,
        coverage_year                       INT                 NULL,
        processing_year                     INT                 NULL,
        processing_month                    TINYINT             NULL,
        processing_day                      TINYINT             NULL,
        file_timestamp                      DATETIME2(3)        NULL,
        source_file                         NVARCHAR(500)       NOT NULL,
        source_path                         NVARCHAR(1500)      NOT NULL,
        file_hash                           CHAR(64)            NOT NULL,
        row_number_in_file                  BIGINT              NOT NULL,
        load_run_id                         UNIQUEIDENTIFIER    NOT NULL,
        loaded_at                           DATETIME2(3)        NOT NULL
            CONSTRAINT DF_rcni_raw_loaded_at DEFAULT SYSUTCDATETIME(),
        quality_status                      VARCHAR(30)         NOT NULL,
        exchange_assigned_policy_id         NVARCHAR(100)       NULL,
        plan_id                             NVARCHAR(100)       NULL,
        member_last_name                    NVARCHAR(255)       NULL,
        member_first_name                   NVARCHAR(255)       NULL,
        exchange_assigned_member_id         NVARCHAR(100)       NULL,
        issuer_assigned_member_id           NVARCHAR(100)       NULL,
        subscriber_last_name                NVARCHAR(255)       NULL,
        subscriber_first_name               NVARCHAR(255)       NULL,
        exchange_assigned_subscriber_id     NVARCHAR(100)       NULL,
        issuer_assigned_subscriber_id       NVARCHAR(100)       NULL,
        discrepancy_reason_code             NVARCHAR(100)       NULL,
        discrepancy_reason_text             NVARCHAR(500)       NULL,
        hix_value                           NVARCHAR(1000)      NULL,
        issuer_value                        NVARCHAR(1000)      NULL,
        date_of_discrepancy                 NVARCHAR(50)        NULL,
        recon_file_name                     NVARCHAR(255)       NULL,
        autofixed_by_hix                    NVARCHAR(50)        NULL,
        assignee                            NVARCHAR(100)       NULL,
        enrollment_status                   NVARCHAR(100)       NULL
    );
END;
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'UX_rcni_raw_file_row' AND object_id = OBJECT_ID(N'dbo.rcni_raw')
)
    CREATE UNIQUE INDEX UX_rcni_raw_file_row
        ON dbo.rcni_raw (file_hash, row_number_in_file);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_rcni_raw_issuer_period' AND object_id = OBJECT_ID(N'dbo.rcni_raw')
)
    CREATE INDEX IX_rcni_raw_issuer_period
        ON dbo.rcni_raw (issuer_id, coverage_year, processing_year, processing_month);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_rcni_raw_run' AND object_id = OBJECT_ID(N'dbo.rcni_raw')
)
    CREATE INDEX IX_rcni_raw_run
        ON dbo.rcni_raw (load_run_id);
GO


-- -----------------------------------------------------------------------------
-- 5. dbo.rcni_data_quality_issue
-- -----------------------------------------------------------------------------
-- Technical anomalies. Persisted in their own audit transactions.
-- Unique natural key prevents duplicate issues on retry.
-- File-level issues use row_number_in_file NULL / column_name NULL
-- (computed keys map those to -1 / '').
-- -----------------------------------------------------------------------------
IF OBJECT_ID(N'dbo.rcni_data_quality_issue', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.rcni_data_quality_issue
    (
        quality_issue_id        BIGINT IDENTITY(1,1) NOT NULL
            CONSTRAINT PK_rcni_data_quality_issue PRIMARY KEY CLUSTERED,
        load_run_id             UNIQUEIDENTIFIER    NULL,
        source_file             NVARCHAR(500)       NOT NULL,
        source_path             NVARCHAR(1500)      NOT NULL,
        file_hash               CHAR(64)            NOT NULL,
        issuer_id               VARCHAR(20)         NULL,
        coverage_year           INT                 NULL,
        row_number_in_file      BIGINT              NULL,
        physical_line_number    BIGINT              NULL,
        column_name             NVARCHAR(200)       NULL,
        invalid_value           NVARCHAR(1000)      NULL,
        issue_code              VARCHAR(50)         NOT NULL,
        issue_message           NVARCHAR(1000)      NOT NULL,
        expected_column_count   INT                 NULL,
        observed_column_count   INT                 NULL,
        raw_record              NVARCHAR(MAX)       NULL,
        created_at              DATETIME2(3)        NOT NULL
            CONSTRAINT DF_rcni_dq_created DEFAULT SYSUTCDATETIME(),
        issue_row_key           AS ISNULL(row_number_in_file, CONVERT(BIGINT, -1)) PERSISTED,
        issue_column_key        AS ISNULL(column_name, N'') PERSISTED,
        CONSTRAINT UX_rcni_dq_natural UNIQUE (file_hash, issue_row_key, issue_code, issue_column_key)
    );
END;
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_rcni_dq_file' AND object_id = OBJECT_ID(N'dbo.rcni_data_quality_issue')
)
    CREATE INDEX IX_rcni_dq_file
        ON dbo.rcni_data_quality_issue (file_hash, row_number_in_file);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_rcni_dq_run' AND object_id = OBJECT_ID(N'dbo.rcni_data_quality_issue')
)
    CREATE INDEX IX_rcni_dq_run
        ON dbo.rcni_data_quality_issue (load_run_id, issue_code);
GO
