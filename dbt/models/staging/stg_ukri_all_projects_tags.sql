-- stg_ukri_all_projects_tags.sql
-- Staging model: clean and type-cast the RAW all-funder keyword tag table.
--
-- What this does:
--   1. No funder filter — every UKRI council is included (unlike
--      stg_ukri_raw_projects, which is Innovate UK only).
--   2. Deduplicate on project_id + keyword (in case of re-runs with overwrite=False)
--
-- Materialised as VIEW — cheap to refresh, always reads latest RAW data.

WITH raw AS (
    SELECT * FROM {{ source('raw', 'UKRI_ALL_PROJECTS_TAGS') }}
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
