-- mart_category_growth_trends.sql
-- One row per (category, quarter). Growth rate compares each quarter's
-- funded-project volume to that category's own trailing 4-quarter baseline —
-- deliberately not cross-category, see plan doc for why.

WITH quarterly AS (
    SELECT
        CATEGORY,
        DATE_TRUNC('quarter', START_DATE) AS QUARTER,
        COUNT(DISTINCT PROJECT_ID) AS PROJECTS_FUNDED
    FROM {{ ref('mart_rpi_keyword_tags') }}
    WHERE START_DATE IS NOT NULL
    GROUP BY CATEGORY, DATE_TRUNC('quarter', START_DATE)
),

with_baseline AS (
    SELECT
        CATEGORY,
        QUARTER,
        PROJECTS_FUNDED,
        AVG(PROJECTS_FUNDED) OVER (
            PARTITION BY CATEGORY ORDER BY QUARTER
            ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
        ) AS TRAILING_BASELINE,
        COUNT(*) OVER (
            PARTITION BY CATEGORY ORDER BY QUARTER
            ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
        ) AS BASELINE_QUARTERS_AVAILABLE
    FROM quarterly
)

SELECT
    CATEGORY,
    QUARTER,
    PROJECTS_FUNDED,
    TRAILING_BASELINE,
    BASELINE_QUARTERS_AVAILABLE,

    -- NULL until there's at least 2 quarters of real history —
    -- a single-quarter "baseline" is just noise, not a trend.
    CASE
        WHEN BASELINE_QUARTERS_AVAILABLE < 2 OR TRAILING_BASELINE IS NULL OR TRAILING_BASELINE = 0
            THEN NULL
        ELSE ROUND((PROJECTS_FUNDED - TRAILING_BASELINE) / TRAILING_BASELINE * 100, 1)
    END AS GROWTH_RATE_PCT,

    -- Simple flag, not a full quadrant model (see out-of-scope) —
    -- threshold is a starting point, tune once real numbers are visible.
    CASE
        WHEN BASELINE_QUARTERS_AVAILABLE >= 2
             AND TRAILING_BASELINE > 0
             AND (PROJECTS_FUNDED - TRAILING_BASELINE) / TRAILING_BASELINE > 0.25
            THEN TRUE
        ELSE FALSE
    END AS IS_ACCELERATING

FROM with_baseline
ORDER BY CATEGORY, QUARTER
