-- 834 Issuer ETL staging schema

CREATE TABLE IF NOT EXISTS raw_file_inventory (
    file_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    issuer          TEXT NOT NULL,
    year            TEXT NOT NULL,
    month           TEXT NOT NULL,
    file_name       TEXT NOT NULL,
    file_path       TEXT NOT NULL,
    file_hash       TEXT,
    file_size       INTEGER,
    file_type       TEXT,
    source_type     TEXT DEFAULT 'local',
    processed_status TEXT DEFAULT 'pending',
    error_message   TEXT,
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_file_hash ON raw_file_inventory(file_hash);

CREATE TABLE IF NOT EXISTS stg_834_records (
    record_id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id                         INTEGER REFERENCES raw_file_inventory(file_id),
    issuer                          TEXT NOT NULL,
    year                            TEXT NOT NULL,
    month                           TEXT NOT NULL,
    policy_id                       TEXT,
    member_id                       TEXT,
    subscriber_id                   TEXT,
    member_first_name               TEXT,
    member_last_name                TEXT,
    relationship                    TEXT,
    subscriber_flag                 TEXT,
    action_code                     TEXT,
    action_code_description         TEXT,
    maintenance_type_code           TEXT,
    additional_maint_reason_code    TEXT,
    coverage_status                 TEXT,
    benefit_effective_date          TEXT,
    benefit_end_date                TEXT,
    member_maint_effective_date     TEXT,
    total_premium_amount            REAL,
    individual_responsibility_amount REAL,
    aptc_amount                     REAL,
    user_fee_amount                 REAL,
    insurance_type_code             TEXT,
    health_coverage_policy_no       TEXT,
    household_or_employee_case_id   TEXT,
    rating_area                     TEXT,
    source_exchg_id                 TEXT,
    premium_validation_status       TEXT,
    expected_user_fee               REAL,
    days_between_effective_and_cancel INTEGER,
    months_between_effective_and_cancel INTEGER,
    days_between_effective_and_end  INTEGER,
    days_between_transaction_and_end INTEGER,
    reporting_month                 TEXT,
    cancellation_window_status      TEXT,
    refund_eligibility              TEXT,
    transaction_classification      TEXT,
    revenue_at_risk                 REAL,
    withheld_user_fee               REAL,
    non_refundable_user_fee         REAL,
    refund_eligible_user_fee          REAL,
    enrollment_action_code            TEXT,
    enrollee_event_type_code          TEXT,
    enrollee_event_reason_code        TEXT,
    exchg_assigned_enrollee_id        TEXT,
    request_submit_timestamp          TEXT,
    issuer_subscriber_identifier      TEXT,
    issuer_indiv_identifier           TEXT,
    last_premium_paid_date            TEXT,
    qtyn                              TEXT,
    qtyy                              TEXT,
    qtyt                              TEXT,
    raw_xml_path                    TEXT,
    raw_payload                     TEXT,
    created_at                      TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_stg_issuer ON stg_834_records(issuer);
CREATE INDEX IF NOT EXISTS idx_stg_period ON stg_834_records(issuer, year, month);
CREATE INDEX IF NOT EXISTS idx_stg_policy ON stg_834_records(policy_id, member_id);
CREATE INDEX IF NOT EXISTS idx_stg_action ON stg_834_records(action_code_description);

CREATE TABLE IF NOT EXISTS parse_errors (
    error_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    issuer      TEXT,
    year        TEXT,
    month       TEXT,
    file_name   TEXT,
    file_path   TEXT,
    error_message TEXT,
    created_at  TEXT DEFAULT (datetime('now'))
);
