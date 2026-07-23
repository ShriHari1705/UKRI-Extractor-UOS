-- mart_rpi_keyword_tags.sql
-- Long-format mart: one row per (project × keyword) pair, across ALL UKRI funders.
-- Sibling of mart_keyword_tags, but not restricted to Innovate UK.
--
-- Grain: PROJECT_ID + KEYWORD (unique after staging dedup)

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

    -- Convenience flag: did this keyword come from the more precise tech field?
    CASE
        WHEN FOUND_IN LIKE '%techAbstractText%' THEN TRUE
        ELSE FALSE
    END AS is_tech_abstract_signal

FROM {{ ref('stg_ukri_all_projects_tags') }}
ORDER BY START_DATE DESC NULLS LAST, PROJECT_ID, CATEGORY, KEYWORD
