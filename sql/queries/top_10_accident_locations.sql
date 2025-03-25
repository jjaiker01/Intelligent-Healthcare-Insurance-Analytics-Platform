-- sql/queries/top_10_accident_locations.sql
SELECT
    l.city,
    COUNT(*) AS accident_count
FROM
    Accidents a
JOIN
    Locations l ON a.location_id = l.location_id
GROUP BY
    l.city
ORDER BY
    accident_count DESC
LIMIT 10;