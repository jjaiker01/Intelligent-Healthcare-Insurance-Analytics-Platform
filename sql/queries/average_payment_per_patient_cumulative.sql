-- sql/queries/average_payment_per_patient_cumulative.sql
-- Explanation:

-- This query calculates the cumulative average payment amount for each patient over time.
-- It uses AVG(...) OVER (PARTITION BY ... ORDER BY ...) to partition the data by patient and calculate the cumulative average.

SELECT
    p.patient_id,
    AVG(p.payment_amount) OVER (PARTITION BY p.patient_id ORDER BY p.payment_date ROWS UNBOUNDED PRECEDING) AS cumulative_avg_payment
FROM
    Payments p
ORDER BY
    p.patient_id, p.payment_date;