-- Count the number of records in the fct_monthly_zone_revenue table
SELECT COUNT(*) AS cnt
FROM `kestra-sandbox-493016.dbt_penny_prod.fct_monthly_zone_revenue`;


-- Find the pickup zone with the highest total revenue for Green taxis in 2020
SELECT
  pickup_zone,
  SUM(revenue_monthly_total_amount) AS total_revenue
FROM `kestra-sandbox-493016.dbt_penny_prod.fct_monthly_zone_revenue`
WHERE service_type = 'Green'
  AND EXTRACT(YEAR FROM revenue_month) = 2020
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 1;