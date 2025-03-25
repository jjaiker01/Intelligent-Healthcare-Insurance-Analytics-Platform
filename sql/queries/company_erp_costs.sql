-- sql/queries/company_erp_costs.sql
SELECT
    sc.accident_id,
    SUM(sc.cost_amount) AS total_cost
FROM
    COMPANY_Costs sc
GROUP BY
    sc.accident_id
ORDER BY
    total_cost DESC;