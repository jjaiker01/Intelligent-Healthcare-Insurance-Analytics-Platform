-- sql/queries/soat_validity_rate.sql
SELECT
    (SUM(CASE WHEN s.is_valid = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS validity_rate
FROM
    SOAT s;