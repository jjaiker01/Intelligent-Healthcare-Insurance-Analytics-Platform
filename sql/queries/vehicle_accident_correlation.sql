-- sql/queries/vehicle_accident_correlation.sql
SELECT
    v.vehicle_type,
    COUNT(a.accident_id) AS accident_count
FROM
    Vehicles v
JOIN
    Accidents a ON v.vehicle_id = a.vehicle_id
GROUP BY
    v.vehicle_type
ORDER BY
    accident_count DESC;