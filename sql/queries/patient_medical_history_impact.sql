-- sql/queries/patient_medical_history_impact.sql
SELECT
    p.patient_id,
    p.name,
    p.medical_history,
    COUNT(a.accident_id) AS accident_count
FROM
    Patients p
LEFT JOIN
    Accidents a ON p.patient_id = a.patient_id
GROUP BY
    p.patient_id, p.name, p.medical_history
ORDER BY
    accident_count DESC;