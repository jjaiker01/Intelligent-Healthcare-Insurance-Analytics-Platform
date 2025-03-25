-- sql/queries/payment_amount_severity_distribution.sql

-- Explanation:

-- This query calculates the 25th, 50th (median), and 75th percentiles of payment amounts for each accident severity.
-- It uses the PERCENTILE_CONT window function for percentile calculations.

SELECT
    severity,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY payment_amount) AS percentile_25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY payment_amount) AS percentile_50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY payment_amount) AS percentile_75
FROM
    Payments p
JOIN
    Accidents a ON p.accident_id = a.accident_id
GROUP BY
    severity;