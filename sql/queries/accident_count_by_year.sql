-- sql/queries/accident_count_by_year.sql
SELECT
    YEAR(a.accident_date) AS accident_year,
    COUNT(*) AS accident_count
FROM
    Accidents a
GROUP BY
    YEAR(a.accident_date)
ORDER BY
    accident_year;