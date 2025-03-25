-- sql/queries/soat_claim_analysis.sql
SELECT
    s.insurance_provider,
    COUNT(p.payment_id) AS claim_count,
    SUM(p.payment_amount) AS total_claims_amount
FROM
    SOAT s
JOIN
    Payments p ON s.vehicle_id = (SELECT vehicle_id from Accidents where accident_id = p.accident_id)
WHERE
    s.is_valid = TRUE
GROUP BY
    s.insurance_provider
ORDER BY
    claim_count DESC;