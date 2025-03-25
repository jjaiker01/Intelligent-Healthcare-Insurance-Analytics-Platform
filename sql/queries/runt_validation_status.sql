-- sql/queries/runt_validation_status.sql
SELECT
    rv.validation_status,
    COUNT(*) AS validation_count
FROM
    RUNT_Validation rv
GROUP BY
    rv.validation_status
ORDER BY
    validation_count DESC;