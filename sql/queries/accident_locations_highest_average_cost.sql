-- sql/queries/accident_locations_highest_average_cost.sql
-- Explanation:

-- This query calculates the average cost per accident location.
-- It uses DENSE_RANK() to rank the locations based on the average cost.

SELECT
    l.city,
    AVG(sc.cost_amount) AS avg_cost,
    DENSE_RANK() OVER (ORDER BY AVG(sc.cost_amount) DESC) as cost_rank
FROM
    SURA_Costs sc
JOIN
    Accidents a ON sc.accident_id = a.accident_id
JOIN
    Locations l on a.location_id = l.location_id
GROUP BY l.city
ORDER BY avg_cost DESC;