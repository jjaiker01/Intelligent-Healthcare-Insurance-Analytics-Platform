-- sql/queries/driver_accident_risk.sql
SELECT
    d.driver_id,
    d.driver_name,
    ar.risk_score,
    ar.risk_label
FROM
    Drivers d
JOIN
    Accident_Risk ar ON d.driver_id = ar.driver_id
ORDER BY
    ar.risk_score DESC;