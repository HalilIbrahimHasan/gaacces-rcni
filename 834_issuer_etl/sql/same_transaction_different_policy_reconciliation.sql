-- =============================================================================
-- Same-transaction / different-policy reconciliation
-- FFM: dbo.Enrollments_TEST  |  Inbound: dbo.inbound_automation
-- Scope: issuer 37301 FFM enrollee-policy population
--
-- IMPORTANT:
--   Does NOT require policy_id equality as a join predicate.
--   Joins on enrollee ID first, then ranks inbound candidates by:
--     1) exact enrollee+policy
--     2) same issuer + same normalized status + same/near event date
--     3) same issuer + same status + nearest date
--     4) same issuer + nearest date
--     5) different issuer + same status + nearest date
--     6) enrollee only
--
-- Column names below are actual columns observed from:
--   - FFM vs inbound Pairs.xlsx / Enrollments Azure mirror inventory
--   - dbo.inbound_automation DDL (sql/inbound_automation_ddl.sql)
-- =============================================================================

DECLARE @ffm_issuer VARCHAR(20) = '37301';

;WITH ffm AS (
    SELECT
        CAST(e.enrollee_id AS VARCHAR(100)) AS FFM_Enrollee_ID,
        CAST(e.enrollment_id AS VARCHAR(100)) AS FFM_Policy_ID,
        CAST(e.hios_issuer_id AS VARCHAR(20)) AS FFM_Issuer,
        e.coverage_year AS FFM_Coverage_Year,
        e.enrollment_status_description AS FFM_Enrollment_Status_Raw,
        e.enrollee_status_description AS FFM_Enrollee_Status_Raw,
        /* Prefer enrollee_status_description; fall back to enrollment_status_description */
        COALESCE(e.enrollee_status_description, e.enrollment_status_description) AS FFM_Status_Raw,
        CASE
            WHEN UPPER(LTRIM(RTRIM(COALESCE(e.enrollee_status_description, e.enrollment_status_description))))
                 IN ('ENROLLED') THEN 'CONFIRM'
            WHEN UPPER(LTRIM(RTRIM(COALESCE(e.enrollee_status_description, e.enrollment_status_description))))
                 IN ('CANCELLED', 'CANCELED') THEN 'CANCEL'
            WHEN UPPER(LTRIM(RTRIM(COALESCE(e.enrollee_status_description, e.enrollment_status_description))))
                 IN ('TERMINATED') THEN 'TERM'
            ELSE 'STATUS_MAPPING_REVIEW'
        END AS FFM_Status_Norm,
        e.benefit_effective_date,
        e.benefit_end_date,
        e.enrollment_create_date,
        e.enrollment_last_update_date,
        /* Strongest observed correlator vs inbound event date on exact pairs */
        COALESCE(
            e.enrollment_last_update_date,
            CASE
                WHEN UPPER(LTRIM(RTRIM(COALESCE(e.enrollee_status_description, e.enrollment_status_description))))
                     IN ('CANCELLED', 'CANCELED', 'TERMINATED')
                    THEN e.benefit_end_date
            END,
            e.enrollment_create_date,
            e.benefit_effective_date
        ) AS FFM_Event_Date,
        e.household_id,
        e.person_type,
        e.relationship_type,
        e.source AS FFM_Source
    FROM dbo.Enrollments_TEST AS e
    WHERE CAST(e.hios_issuer_id AS VARCHAR(20)) = @ffm_issuer
),
inbound AS (
    SELECT
        CAST(COALESCE(NULLIF(LTRIM(RTRIM(ia.member_id)), ''),
                      NULLIF(LTRIM(RTRIM(ia.exchg_assigned_enrollee_id)), '')) AS VARCHAR(100))
            AS Inbound_Enrollee_ID,
        CAST(COALESCE(NULLIF(LTRIM(RTRIM(ia.policy_id)), ''),
                      NULLIF(LTRIM(RTRIM(ia.health_coverage_policy_no)), '')) AS VARCHAR(100))
            AS Inbound_Policy_ID,
        CAST(ia.issuer AS VARCHAR(20)) AS Inbound_Issuer,
        ia.coverage_year AS Inbound_Coverage_Year,
        ia.enrolleeStatus AS Inbound_Status_Raw,
        CASE
            WHEN UPPER(LTRIM(RTRIM(ia.enrolleeStatus))) = 'CONFIRM' THEN 'CONFIRM'
            WHEN UPPER(LTRIM(RTRIM(ia.enrolleeStatus))) = 'CANCEL' THEN 'CANCEL'
            WHEN UPPER(LTRIM(RTRIM(ia.enrolleeStatus))) = 'TERM' THEN 'TERM'
            ELSE 'STATUS_MAPPING_REVIEW'
        END AS Inbound_Status_Norm,
        /* Business event date: maint date, else file stamp from source_file, else benefit_effective_date.
           loaded_at is ingestion metadata only and is intentionally not preferred. */
        COALESCE(
            ia.member_maint_effective_date,
            TRY_CONVERT(
                date,
                SUBSTRING(
                    ia.source_file,
                    PATINDEX('%[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]%.xml', ia.source_file),
                    8
                ),
                112
            ),
            ia.benefit_effective_date
        ) AS Inbound_Event_Date,
        ia.folder_year AS Folder_Year,
        ia.folder_month AS Folder_Month,
        ia.source_file AS Source_File,
        ia.household_or_employee_case_id,
        ia.relationship,
        ia.loaded_at,
        ia.id AS inbound_row_id
    FROM dbo.inbound_automation AS ia
    WHERE COALESCE(NULLIF(LTRIM(RTRIM(ia.member_id)), ''),
                   NULLIF(LTRIM(RTRIM(ia.exchg_assigned_enrollee_id)), '')) IS NOT NULL
),
candidates AS (
    SELECT
        f.FFM_Enrollee_ID,
        f.FFM_Policy_ID,
        f.FFM_Issuer,
        f.FFM_Coverage_Year,
        f.FFM_Status_Raw,
        f.FFM_Status_Norm,
        f.FFM_Event_Date,
        f.household_id,
        f.person_type,
        f.relationship_type,
        i.Inbound_Enrollee_ID,
        i.Inbound_Policy_ID,
        i.Inbound_Issuer,
        i.Inbound_Coverage_Year,
        i.Inbound_Status_Raw,
        i.Inbound_Status_Norm,
        i.Inbound_Event_Date,
        i.Folder_Year,
        i.Folder_Month,
        i.Source_File,
        CASE WHEN f.FFM_Policy_ID = i.Inbound_Policy_ID THEN 'YES' ELSE 'NO' END AS Policy_Match_Flag,
        CASE WHEN f.FFM_Issuer = i.Inbound_Issuer THEN 'YES' ELSE 'NO' END AS Issuer_Match_Flag,
        CASE
            WHEN f.FFM_Status_Norm = i.Inbound_Status_Norm
             AND f.FFM_Status_Norm <> 'STATUS_MAPPING_REVIEW'
            THEN 'YES' ELSE 'NO'
        END AS Status_Match_Flag,
        CASE
            WHEN f.FFM_Event_Date IS NOT NULL AND i.Inbound_Event_Date IS NOT NULL
            THEN ABS(DATEDIFF(day, f.FFM_Event_Date, i.Inbound_Event_Date))
        END AS Date_Difference_Days,
        /* ranking priority (lower is better) */
        CASE
            WHEN f.FFM_Policy_ID = i.Inbound_Policy_ID THEN 1
            WHEN f.FFM_Issuer = i.Inbound_Issuer
             AND f.FFM_Status_Norm = i.Inbound_Status_Norm
             AND f.FFM_Status_Norm <> 'STATUS_MAPPING_REVIEW'
             AND f.FFM_Event_Date IS NOT NULL
             AND i.Inbound_Event_Date IS NOT NULL
             AND ABS(DATEDIFF(day, f.FFM_Event_Date, i.Inbound_Event_Date)) = 0
                THEN 2
            WHEN f.FFM_Issuer = i.Inbound_Issuer
             AND f.FFM_Status_Norm = i.Inbound_Status_Norm
             AND f.FFM_Status_Norm <> 'STATUS_MAPPING_REVIEW'
             AND f.FFM_Event_Date IS NOT NULL
             AND i.Inbound_Event_Date IS NOT NULL
             AND ABS(DATEDIFF(day, f.FFM_Event_Date, i.Inbound_Event_Date)) <= 7
                THEN 3
            WHEN f.FFM_Issuer = i.Inbound_Issuer
             AND f.FFM_Status_Norm = i.Inbound_Status_Norm
             AND f.FFM_Status_Norm <> 'STATUS_MAPPING_REVIEW'
                THEN 4
            WHEN f.FFM_Issuer = i.Inbound_Issuer THEN 5
            WHEN f.FFM_Status_Norm = i.Inbound_Status_Norm
             AND f.FFM_Status_Norm <> 'STATUS_MAPPING_REVIEW'
             AND f.FFM_Event_Date IS NOT NULL
             AND i.Inbound_Event_Date IS NOT NULL
             AND ABS(DATEDIFF(day, f.FFM_Event_Date, i.Inbound_Event_Date)) <= 30
                THEN 6
            WHEN f.FFM_Status_Norm = i.Inbound_Status_Norm
             AND f.FFM_Status_Norm <> 'STATUS_MAPPING_REVIEW'
                THEN 7
            ELSE 8
        END AS rank_priority,
        i.inbound_row_id
    FROM ffm AS f
    INNER JOIN inbound AS i
        ON f.FFM_Enrollee_ID = i.Inbound_Enrollee_ID
),
ranked AS (
    SELECT
        c.*,
        ROW_NUMBER() OVER (
            PARTITION BY c.FFM_Enrollee_ID, c.FFM_Policy_ID
            ORDER BY
                c.rank_priority ASC,
                CASE WHEN c.Date_Difference_Days IS NULL THEN 999999 ELSE c.Date_Difference_Days END ASC,
                c.inbound_row_id DESC
        ) AS rn
    FROM candidates AS c
),
best AS (
    SELECT *
    FROM ranked
    WHERE rn = 1
),
final AS (
    SELECT
        f.FFM_Enrollee_ID,
        f.FFM_Policy_ID,
        f.FFM_Issuer,
        f.FFM_Coverage_Year,
        f.FFM_Status_Raw,
        f.FFM_Status_Norm,
        f.FFM_Event_Date,
        b.Inbound_Enrollee_ID,
        b.Inbound_Policy_ID,
        b.Inbound_Issuer,
        b.Inbound_Coverage_Year,
        b.Inbound_Status_Raw,
        b.Inbound_Status_Norm,
        b.Inbound_Event_Date,
        b.Folder_Year,
        b.Folder_Month,
        b.Source_File,
        COALESCE(b.Policy_Match_Flag, 'NO') AS Policy_Match_Flag,
        COALESCE(b.Issuer_Match_Flag, 'NO') AS Issuer_Match_Flag,
        COALESCE(b.Status_Match_Flag, 'NO') AS Status_Match_Flag,
        b.Date_Difference_Days,
        CASE
            WHEN b.FFM_Enrollee_ID IS NULL THEN 'NO_INBOUND_ENROLLEE_EVIDENCE'
            WHEN b.Policy_Match_Flag = 'YES' THEN 'EXACT_ENROLLEE_POLICY_MATCH'
            WHEN b.Issuer_Match_Flag = 'YES'
             AND b.Status_Match_Flag = 'YES'
             AND b.Date_Difference_Days IS NOT NULL
             AND b.Date_Difference_Days <= 7
                THEN 'SAME_TRANSACTION_DIFFERENT_POLICY'
            WHEN b.Issuer_Match_Flag = 'YES'
             AND b.Status_Match_Flag = 'YES'
                THEN 'SAME_LIFECYCLE_DIFFERENT_POLICY'
            WHEN b.Issuer_Match_Flag = 'NO'
             AND b.Status_Match_Flag = 'YES'
                THEN 'CROSS_ISSUER_TRANSITION'
            WHEN b.Inbound_Enrollee_ID IS NOT NULL
                THEN 'ENROLLEE_FOUND_DIFFERENT_LIFECYCLE'
            ELSE 'NO_INBOUND_ENROLLEE_EVIDENCE'
        END AS Match_Level,
        CASE
            WHEN b.FFM_Enrollee_ID IS NULL THEN 'NO_MATCH'
            WHEN b.Policy_Match_Flag = 'YES' THEN 'VERY_STRONG'
            WHEN b.Issuer_Match_Flag = 'YES'
             AND b.Status_Match_Flag = 'YES'
             AND b.Date_Difference_Days IS NOT NULL
             AND b.Date_Difference_Days <= 7
                THEN 'VERY_STRONG'
            WHEN b.Issuer_Match_Flag = 'YES'
             AND b.Status_Match_Flag = 'YES'
             AND b.Date_Difference_Days IS NOT NULL
             AND b.Date_Difference_Days <= 30
                THEN 'STRONG'
            WHEN b.Issuer_Match_Flag = 'NO'
             AND b.Status_Match_Flag = 'YES'
                THEN 'CROSS_ISSUER'
            WHEN b.Inbound_Enrollee_ID IS NOT NULL THEN 'WEAK'
            ELSE 'NO_MATCH'
        END AS Match_Score,
        CASE
            WHEN b.FFM_Enrollee_ID IS NULL THEN 'NO_INBOUND_EVIDENCE'
            WHEN b.Policy_Match_Flag = 'YES' THEN 'EXACT_POLICY_MATCH'
            WHEN b.Issuer_Match_Flag = 'YES'
             AND b.Status_Match_Flag = 'YES'
             AND b.Date_Difference_Days IS NOT NULL
             AND b.Date_Difference_Days <= 30
                THEN 'POTENTIAL_POLICY_IDENTIFIER_MISMATCH'
            WHEN b.Issuer_Match_Flag = 'YES'
             AND b.Policy_Match_Flag = 'NO'
                THEN 'SAME_ISSUER_DIFFERENT_POLICY'
            WHEN b.Issuer_Match_Flag = 'NO'
             AND b.Status_Match_Flag = 'YES'
                THEN 'CROSS_ISSUER_TRANSITION'
            WHEN b.Inbound_Enrollee_ID IS NOT NULL THEN 'DIFFERENT_LIFECYCLE'
            ELSE 'NO_INBOUND_EVIDENCE'
        END AS Root_Cause_Category
    FROM ffm AS f
    LEFT JOIN best AS b
        ON f.FFM_Enrollee_ID = b.FFM_Enrollee_ID
       AND f.FFM_Policy_ID = b.FFM_Policy_ID
)
-- Detail rows (best inbound candidate per FFM enrollee-policy)
SELECT *
FROM final
ORDER BY
    CASE Root_Cause_Category
        WHEN 'EXACT_POLICY_MATCH' THEN 1
        WHEN 'POTENTIAL_POLICY_IDENTIFIER_MISMATCH' THEN 2
        WHEN 'SAME_ISSUER_DIFFERENT_POLICY' THEN 3
        WHEN 'CROSS_ISSUER_TRANSITION' THEN 4
        WHEN 'DIFFERENT_LIFECYCLE' THEN 5
        ELSE 6
    END,
    FFM_Enrollee_ID,
    FFM_Policy_ID;

-- -----------------------------------------------------------------------------
-- Summary (run separately or as second result set)
-- -----------------------------------------------------------------------------
/*
;WITH final AS (
    -- paste final CTE above
)
SELECT
    Match_Level,
    COUNT(*) AS Distinct_FFM_Pairs,
    COUNT(DISTINCT FFM_Enrollee_ID) AS Distinct_Enrollees,
    CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS DECIMAL(8,2)) AS Percentage
FROM final
GROUP BY Match_Level
ORDER BY Distinct_FFM_Pairs DESC;

SELECT
    Root_Cause_Category,
    COUNT(*) AS Distinct_FFM_Pairs,
    COUNT(DISTINCT FFM_Enrollee_ID) AS Distinct_Enrollees,
    COUNT(DISTINCT FFM_Policy_ID) AS Distinct_FFM_Policies,
    COUNT(DISTINCT Inbound_Policy_ID) AS Distinct_Inbound_Policies,
    CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS DECIMAL(8,2)) AS Percentage
FROM final
GROUP BY Root_Cause_Category
ORDER BY Distinct_FFM_Pairs DESC;

-- Non-exact population: same enrollee + issuer + status + near date, policy differs
SELECT
    COUNT(*) AS matched_txn_relationships,
    COUNT(DISTINCT FFM_Enrollee_ID) AS distinct_enrollees,
    COUNT(DISTINCT FFM_Policy_ID) AS distinct_ffm_policies,
    COUNT(DISTINCT Inbound_Policy_ID) AS distinct_inbound_policies
FROM final
WHERE Root_Cause_Category = 'POTENTIAL_POLICY_IDENTIFIER_MISMATCH';
*/
