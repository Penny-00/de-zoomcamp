SELECT *
FROM yellow_taxi_trips
LIMIT 10;


SELECT *
FROM zones
LIMIT 10;


SELECT
    t."PULocationID",
    SUM(t.total_amount) AS total_revenue,
    zpu."Zone" AS pickup_zone
FROM yellow_taxi_trips t
JOIN zones zpu
	ON t."PULocationID" = zpu."LocationID"
WHERE
    t.lpep_pickup_datetime = '2025-11-18'
GROUP BY zpu."Zone"
ORDER BY total_revenue DESC
