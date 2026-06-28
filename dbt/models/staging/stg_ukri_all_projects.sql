-- stg_ukri_all_projects.sql
-- Staging model: clean the all-funders raw project table.
-- One row per project (deduped on project_id, latest ingestion wins).
-- Materialised as VIEW — cheap refresh, always reads latest RAW data.

WITH raw AS (
    SELECT * FROM {{ source('raw', 'UKRI_ALL_PROJECTS') }}
),

deduped AS (
    SELECT
        PROJECT_ID,
        TITLE,
        STATUS,
        LEAD_FUNDER,
        GRANT_CATEGORY,
        DEPARTMENT,
        START_DATE,
        END_DATE,
        GTR_URL,
        INGESTED_AT,
        ROW_NUMBER() OVER (
            PARTITION BY PROJECT_ID
            ORDER BY INGESTED_AT DESC
        ) AS rn
    FROM raw
    WHERE LEAD_FUNDER IS NOT NULL
)

SELECT
    PROJECT_ID,
    TITLE,
    STATUS,
    LEAD_FUNDER,
    GRANT_CATEGORY,
    DEPARTMENT,
    START_DATE,
    END_DATE,
    GTR_URL,
    INGESTED_AT
FROM deduped
WHERE rn = 1
