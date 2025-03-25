-- sql/queries/payment_tracking.sql
SELECT
    p.payment_id,
    p.accident_id,
    p.patient_id,
    p.payment_date,
    p.payment_amount,
    p.payment_type,
    p.payment_description
FROM
    Payments p
ORDER BY
    p.payment_date DESC;