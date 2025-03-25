-- sql/queries/soat_validity_accident_rate.sql
-- Explanation:

-- This query calculates the percentage of accidents where the involved vehicle had a valid SOAT policy.
-- It uses conditional aggregation (CASE WHEN ... THEN ... ELSE ... END) to count valid SOAT cases.

SELECT
    YEAR(a.accident_date) AS accident_year,
    COUNT(CASE WHEN s.is_valid = TRUE THEN 1 ELSE NULL END) * 100.0 / COUNT(a.accident_id) AS validity_rate
FROM
    Accidents a
LEFT JOIN
    SOAT s ON a.vehicle_id = s.vehicle_id
GROUP BY
    YEAR(a.accident_date)
ORDER BY
    accident_year;