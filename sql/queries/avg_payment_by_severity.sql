-- sql/queries/avg_payment_by_severity.sql
SELECT
    a.severity,
    AVG(p.payment_amount) AS average_payment
FROM
    Accidents a
JOIN
    Payments p ON a.accident_id = p.accident_id
GROUP BY
    a.severity
ORDER BY
    average_payment DESC;