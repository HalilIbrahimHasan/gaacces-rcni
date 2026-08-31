/*
Best Life 2026 — residual enrollee lookup against dbo.inbound_automation.

Read-only SELECT. Uses the user's Azure column names:
  - file_name
  - enrolleeSatatus

Paste the 242 Excel-unmatched enrollee IDs into @candidate_ids, or load from a
temp table / CSV import in SSMS. This script returns:
  1) per-ID lookup result
  2) residual IDs only (not found in any enrollee identifier column)
*/

DECLARE @issuer NVARCHAR(20) = N'83502';

/* Example: single ID probe
SELECT TOP 50
    ia.id,
    ia.issuer,
    ia.folder_year,
    ia.coverage_year,
    ia.file_name,
    ia.member_id,
    ia.issuer_indiv_identifier,
    ia.exchg_assigned_enrollee_id,
    ia.policy_id,
    ia.health_coverage_policy_no,
    ia.enrolleeSatatus,
    ia.benefit_effective_date,
    ia.member_maint_effective_date,
    ia.loaded_at
FROM dbo.inbound_automation AS ia
WHERE ia.issuer = @issuer
  AND (
        NULLIF(LTRIM(RTRIM(ia.member_id)), '') = '1000512465'
     OR NULLIF(LTRIM(RTRIM(ia.issuer_indiv_identifier)), '') = '1000512465'
     OR NULLIF(LTRIM(RTRIM(ia.exchg_assigned_enrollee_id)), '') = '1000512465'
  )
ORDER BY ia.loaded_at DESC, ia.id DESC;
*/

/* Template for batch lookup once candidate IDs are staged. */
/*
WITH candidate_ids AS (
    SELECT enrollee_id
    FROM (VALUES
        ('1000512465'),
        ('1000512462')
    ) AS v(enrollee_id)
),
normalized AS (
    SELECT
        NULLIF(LTRIM(RTRIM(CAST(c.enrollee_id AS NVARCHAR(100)))), '') AS enrollee_id
    FROM candidate_ids AS c
),
matches AS (
    SELECT DISTINCT
        n.enrollee_id,
        ia.id AS inbound_automation_id,
        ia.folder_year,
        ia.coverage_year,
        ia.file_name,
        ia.policy_id,
        ia.health_coverage_policy_no,
        ia.enrolleeSatatus,
        ia.benefit_effective_date,
        ia.member_maint_effective_date,
        ia.loaded_at,
        CASE
            WHEN NULLIF(LTRIM(RTRIM(ia.member_id)), '') = n.enrollee_id THEN 'member_id'
            WHEN NULLIF(LTRIM(RTRIM(ia.issuer_indiv_identifier)), '') = n.enrollee_id THEN 'issuer_indiv_identifier'
            WHEN NULLIF(LTRIM(RTRIM(ia.exchg_assigned_enrollee_id)), '') = n.enrollee_id THEN 'exchg_assigned_enrollee_id'
        END AS matched_on
    FROM normalized AS n
    INNER JOIN dbo.inbound_automation AS ia
        ON ia.issuer = @issuer
       AND (
            NULLIF(LTRIM(RTRIM(ia.member_id)), '') = n.enrollee_id
         OR NULLIF(LTRIM(RTRIM(ia.issuer_indiv_identifier)), '') = n.enrollee_id
         OR NULLIF(LTRIM(RTRIM(ia.exchg_assigned_enrollee_id)), '') = n.enrollee_id
       )
)
SELECT
    n.enrollee_id,
    CASE WHEN m.enrollee_id IS NULL THEN 'NOT_FOUND' ELSE 'FOUND' END AS db_status,
    m.matched_on,
    m.folder_year,
    m.coverage_year,
    m.file_name,
    m.policy_id,
    m.health_coverage_policy_no,
    m.enrolleeSatatus,
    m.benefit_effective_date,
    m.member_maint_effective_date,
    m.loaded_at
FROM normalized AS n
LEFT JOIN (
    SELECT
        enrollee_id,
        MIN(matched_on) AS matched_on,
        MIN(folder_year) AS folder_year,
        MIN(coverage_year) AS coverage_year,
        MIN(file_name) AS file_name,
        MIN(policy_id) AS policy_id,
        MIN(health_coverage_policy_no) AS health_coverage_policy_no,
        MIN(enrolleeSatatus) AS enrolleeSatatus,
        MIN(benefit_effective_date) AS benefit_effective_date,
        MIN(member_maint_effective_date) AS member_maint_effective_date,
        MIN(loaded_at) AS loaded_at
    FROM matches
    GROUP BY enrollee_id
) AS m
    ON m.enrollee_id = n.enrollee_id
ORDER BY db_status DESC, n.enrollee_id;
*/
