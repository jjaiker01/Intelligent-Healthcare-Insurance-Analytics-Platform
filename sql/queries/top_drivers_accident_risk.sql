-- sql/queries/top_drivers_accident_risk.sql
-- Explanation:

-- This query ranks drivers based on their accident risk scores.
-- It uses the RANK() window function to assign a rank to each driver.
-- It limits the result to the top 5 drivers.

SELECT
    driver_id,
    risk_score,
    RANK() OVER (ORDER BY risk_score DESC) AS risk_rank
FROM
    Accident_Risk
ORDER BY
    risk_score DESC
LIMIT 5;