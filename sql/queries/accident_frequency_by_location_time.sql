-- sql/queries/accident_frequency_by_location_time.sql
SELECT
    l.city,
    YEAR(a.accident_date) AS accident_year,
    MONTH(a.accident_date) AS accident_month,
    COUNT(*) AS accident_count
FROM
    Accidents a
JOIN
    Locations l ON a.location_id = l.location_id
GROUP BY
    l.city, YEAR(a.accident_date), MONTH(a.accident_date)
ORDER BY
    accident_year, accident_month, accident_count DESC;