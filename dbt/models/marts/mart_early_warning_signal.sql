-- mart_early_warning_signal.sql
-- One row per PROJECT — the primary view for Joe's IT Services team.
--
-- Aggregates keyword tags into a compute_score (0-4), priority tier,
-- and a "starting_soon" flag for projects kicking off within 90 days.
--
-- This is the table a BI dashboard (Tableau / Metabase / Superset) connects to.

WITH tagged AS (
    SELECT * FROM {{ ref('mart_keyword_tags') }}
),

scored AS (
    SELECT
        PROJECT_ID,
        MAX(TITLE)          AS TITLE,
        MAX(STATUS)         AS STATUS,
        MAX(LEAD_FUNDER)    AS LEAD_FUNDER,
        MAX(GRANT_CATEGORY) AS GRANT_CATEGORY,
        MAX(START_DATE)     AS START_DATE,
        MAX(END_DATE)       AS END_DATE,
        MAX(GTR_URL)        AS GTR_URL,
        MAX(INGESTED_AT)    AS INGESTED_AT,

        -- compute_score: how many distinct categories flagged (0–4)
        COUNT(DISTINCT CATEGORY)  AS COMPUTE_SCORE,
        COUNT(DISTINCT KEYWORD)   AS KEYWORD_COUNT,

        -- Aggregated lists for human-readable reporting
        LISTAGG(DISTINCT CATEGORY, ', ')
            WITHIN GROUP (ORDER BY CATEGORY) AS CATEGORIES_FLAGGED,
        LISTAGG(DISTINCT KEYWORD, ', ')
            WITHIN GROUP (ORDER BY KEYWORD)  AS KEYWORDS_MATCHED

    FROM tagged
    GROUP BY PROJECT_ID
)

SELECT
    PROJECT_ID,
    TITLE,
    STATUS,
    LEAD_FUNDER,
    GRANT_CATEGORY,
    START_DATE,
    END_DATE,
    COMPUTE_SCORE,
    KEYWORD_COUNT,
    CATEGORIES_FLAGGED,
    KEYWORDS_MATCHED,
    GTR_URL,
    INGESTED_AT,

    -- Priority tier based on how many categories were triggered
    CASE
        WHEN COMPUTE_SCORE = 4 THEN 'Critical'   -- all four categories
        WHEN COMPUTE_SCORE = 3 THEN 'High'
        WHEN COMPUTE_SCORE = 2 THEN 'Medium'
        WHEN COMPUTE_SCORE = 1 THEN 'Low'
        ELSE                        'None'
    END AS PRIORITY,

    -- Forward-looking alert: project starts within the next 90 days
    CASE
        WHEN START_DATE BETWEEN CURRENT_DATE
                            AND DATEADD('day', 90, CURRENT_DATE)
        THEN TRUE
        ELSE FALSE
    END AS STARTING_SOON,

    -- Days until project start (negative = already started)
    DATEDIFF('day', CURRENT_DATE, START_DATE) AS DAYS_TO_START

FROM scored
ORDER BY COMPUTE_SCORE DESC, START_DATE DESC NULLS LAST
