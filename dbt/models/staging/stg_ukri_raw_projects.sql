-- stg_ukri_raw_projects.sql
-- Staging model: clean and type-cast the RAW MotherDuck table.
--
-- What this does:
--   1. Filters to Innovate UK projects only (leadFunder check)
--   2. The Python pipeline already converts Unix ms → Python date before loading,
--      so START_DATE and END_DATE arrive as proper DATE columns.
--   3. Deduplicate on project_id + keyword (in case of re-runs with overwrite=False)
--
-- Materialised as VIEW — cheap to refresh, always reads latest RAW data.

WITH raw AS (
    SELECT * FROM {{ source('raw', 'UKRI_PROJECTS') }}
),

deduped AS (
    SELECT
        PROJECT_ID,
        TITLE,
        STATUS,
        LEAD_FUNDER,
        GRANT_CATEGORY,
        START_DATE,
        END_DATE,
        CATEGORY,
        KEYWORD,
        FOUND_IN,
        GTR_URL,
        INGESTED_AT,
        ROW_NUMBER() OVER (
            PARTITION BY PROJECT_ID, KEYWORD
            ORDER BY INGESTED_AT DESC
        ) AS rn
    FROM raw
    WHERE LEAD_FUNDER = 'Innovate UK'
)

SELECT
    PROJECT_ID,
    TITLE,
    STATUS,
    LEAD_FUNDER,
    GRANT_CATEGORY,
    START_DATE,
    END_DATE,
    CATEGORY,
    KEYWORD,
    FOUND_IN,
    GTR_URL,
    INGESTED_AT
FROM deduped
WHERE rn = 1
