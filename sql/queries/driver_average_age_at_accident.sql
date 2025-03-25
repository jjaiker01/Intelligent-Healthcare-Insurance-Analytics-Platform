-- sql/queries/driver_average_age_at_accident.sql
SELECT
    AVG(d.driver_age) AS average_driver_age
FROM
    Drivers d
JOIN
    Accidents a ON d.driver_id = a.driver_id;