-- sql/queries/most_common_accident_causes.sql
SELECT
    a.accident_cause,
    COUNT(*) AS cause_count
FROM
    Accidents a
GROUP BY
    a.accident_cause
ORDER BY
    cause_count DESC
LIMIT 10;