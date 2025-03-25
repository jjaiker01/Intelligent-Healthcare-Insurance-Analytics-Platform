-- sql/queries/accident_frequency_trend.sql

-- Explanation:

-- This query calculates the monthly accident count.
-- It then uses a window function AVG(...) OVER (...) to calculate the 12-month rolling average of accident counts, providing a trend analysis.

SELECT
    accident_year,
    accident_month,
    accident_count,
    AVG(accident_count) OVER (ORDER BY accident_year, accident_month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_12_month_avg
FROM (
    SELECT
        YEAR(accident_date) AS accident_year,
        MONTH(accident_date) AS accident_month,
        COUNT(*) AS accident_count
    FROM
        Accidents
    GROUP BY
        YEAR(accident_date), MONTH(accident_date)
) AS monthly_accidents
ORDER BY
    accident_year, accident_month;