-- sql/queries/accident_severity_analysis.sql
SELECT
    severity,
    COUNT(*) AS accident_count
FROM
    Accidents
GROUP BY
    severity
ORDER BY
    accident_count DESC;