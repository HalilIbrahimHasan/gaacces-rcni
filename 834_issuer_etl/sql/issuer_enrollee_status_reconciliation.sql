-- =============================================================================
-- Issuer enrollee status reconciliation (dbo.inbound_automation)
-- Hari template: issuer | Confirm | Cancel | Term enrollee counts
-- =============================================================================
-- Set folder year filter:
DECLARE @folder_year INT = 2026;   -- use 2025 for prior-year comparison

-- -----------------------------------------------------------------------------
-- 1) Hari template — distinct enrollees (member_id) per status per issuer
--    Best match for business-ready "Enrollee_Count" reconciliation.
-- -----------------------------------------------------------------------------
SELECT
    issuer,
    COUNT(DISTINCT CASE
        WHEN enrolleeStatus = 'CONFIRM' AND member_id IS NOT NULL THEN member_id
    END) AS enrollee_count_confirm,
    COUNT(DISTINCT CASE
        WHEN enrolleeStatus = 'CANCEL' AND member_id IS NOT NULL THEN member_id
    END) AS enrollee_count_cancel,
    COUNT(DISTINCT CASE
        WHEN enrolleeStatus = 'TERM' AND member_id IS NOT NULL THEN member_id
    END) AS enrollee_count_term,
    COUNT(DISTINCT CASE
        WHEN enrolleeStatus NOT IN ('CONFIRM', 'CANCEL', 'TERM')
             AND member_id IS NOT NULL THEN member_id
    END) AS enrollee_count_other_status,
    COUNT(DISTINCT CASE WHEN member_id IS NOT NULL THEN member_id END)
        AS distinct_member_id_any_status,
    COUNT(*) AS total_raw_rows
FROM dbo.inbound_automation
WHERE folder_year = @folder_year
GROUP BY issuer
ORDER BY issuer;

-- -----------------------------------------------------------------------------
-- 2) Hari template — raw transaction rows per status (not distinct enrollees)
-- -----------------------------------------------------------------------------
SELECT
    issuer,
    SUM(CASE WHEN enrolleeStatus = 'CONFIRM' THEN 1 ELSE 0 END) AS raw_row_count_confirm,
    SUM(CASE WHEN enrolleeStatus = 'CANCEL' THEN 1 ELSE 0 END) AS raw_row_count_cancel,
    SUM(CASE WHEN enrolleeStatus = 'TERM' THEN 1 ELSE 0 END) AS raw_row_count_term,
    SUM(CASE WHEN enrolleeStatus NOT IN ('CONFIRM', 'CANCEL', 'TERM') THEN 1 ELSE 0 END)
        AS raw_row_count_other_status,
    COUNT(*) AS total_raw_rows
FROM dbo.inbound_automation
WHERE folder_year = @folder_year
GROUP BY issuer
ORDER BY issuer;

-- -----------------------------------------------------------------------------
-- 3) Issuer × month × status (for detailed reconciliation)
-- -----------------------------------------------------------------------------
SELECT
    issuer,
    folder_year,
    folder_month,
    enrolleeStatus,
    COUNT(*) AS raw_row_count,
    COUNT(DISTINCT member_id) AS distinct_member_id,
    COUNT(DISTINCT policy_id) AS distinct_policy_id,
    COUNT(DISTINCT COALESCE(policy_id, health_coverage_policy_no)) AS distinct_policy_any
FROM dbo.inbound_automation
WHERE folder_year = @folder_year
GROUP BY issuer, folder_year, folder_month, enrolleeStatus
ORDER BY issuer, folder_month, enrolleeStatus;

-- -----------------------------------------------------------------------------
-- 4) All status values per issuer (includes UNMAPPED)
-- -----------------------------------------------------------------------------
SELECT
    issuer,
    enrolleeStatus,
    COUNT(*) AS raw_row_count,
    COUNT(DISTINCT member_id) AS distinct_member_id
FROM dbo.inbound_automation
WHERE folder_year = @folder_year
GROUP BY issuer, enrolleeStatus
ORDER BY issuer, enrolleeStatus;

-- -----------------------------------------------------------------------------
-- 5) Optional: one latest-status row per member per issuer (transaction-level proxy)
--    Note: business-ready final status uses lifecycle/collapse — this is NOT identical.
-- -----------------------------------------------------------------------------
WITH ranked AS (
    SELECT
        issuer,
        member_id,
        enrolleeStatus,
        benefit_effective_date,
        loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY issuer, member_id
            ORDER BY
                benefit_effective_date DESC,
                loaded_at DESC,
                id DESC
        ) AS rn
    FROM dbo.inbound_automation
    WHERE folder_year = @folder_year
      AND member_id IS NOT NULL
)
SELECT
    issuer,
    COUNT(DISTINCT CASE WHEN enrolleeStatus = 'CONFIRM' THEN member_id END)
        AS latest_status_enrollee_count_confirm,
    COUNT(DISTINCT CASE WHEN enrolleeStatus = 'CANCEL' THEN member_id END)
        AS latest_status_enrollee_count_cancel,
    COUNT(DISTINCT CASE WHEN enrolleeStatus = 'TERM' THEN member_id END)
        AS latest_status_enrollee_count_term,
    COUNT(DISTINCT CASE
        WHEN enrolleeStatus NOT IN ('CONFIRM', 'CANCEL', 'TERM') THEN member_id
    END) AS latest_status_enrollee_count_other
FROM ranked
WHERE rn = 1
GROUP BY issuer
ORDER BY issuer;
