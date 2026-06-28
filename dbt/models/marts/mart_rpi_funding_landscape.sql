-- mart_rpi_funding_landscape.sql
-- One row per project across ALL UKRI funders.
-- Audience: RPI Growth Managers seeking to maximise UKRI funding income.
--
-- Questions this answers:
--   - Which councils are most active by grant category?
--   - What are the funding trends by year?
--   - Which research areas are growing?

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

    -- Derived time dimensions for trend analysis
    YEAR(START_DATE)                        AS START_YEAR,
    YEAR(END_DATE)                          AS END_YEAR,
    DATEDIFF('month', START_DATE, END_DATE) AS DURATION_MONTHS,

    -- Activity flags
    CASE
        WHEN START_DATE BETWEEN DATEADD('day', -730, CURRENT_DATE) AND CURRENT_DATE
        THEN TRUE ELSE FALSE
    END AS STARTED_LAST_2_YEARS,

    CASE
        WHEN END_DATE >= CURRENT_DATE AND START_DATE <= CURRENT_DATE
        THEN TRUE ELSE FALSE
    END AS CURRENTLY_ACTIVE,

    CASE
        WHEN START_DATE BETWEEN CURRENT_DATE AND DATEADD('day', 180, CURRENT_DATE)
        THEN TRUE ELSE FALSE
    END AS STARTING_SOON

FROM {{ ref('stg_ukri_all_projects') }}
ORDER BY START_DATE DESC NULLS LAST
