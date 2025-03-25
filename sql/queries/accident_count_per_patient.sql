-- sql/queries/accident_count_per_patient.sql
SELECT
    a.patient_id,
    COUNT(*) AS accident_count
FROM
    Accidents a
GROUP BY
    a.patient_id
ORDER BY
    accident_count DESC;