
SELECT
	*
FROM
	yellow_taxi_trips t,
	zones zpu,
	zones zdo
WHERE
	t."PULocationID" = zpu."LocationID"
	AND t."DOLocationID" = zdo."LocationID"
LIMIT 100;

# Implicit join is not recommened, it is better to use explicit JOIN syntax.

SELECT
	t.tpep_pickup_datetime,
	t.tpep_dropoff_datetime,
	total_amount,
	CONCAT(zpu."Borough", ' | ', zpu."Zone") AS "pickup_loc",
	CONCAT(zdo."Borough", ' | ', zdo."Zone") AS "dropoff_loc"
FROM
	yellow_taxi_trips t,
	zones zpu,
	zones zdo
WHERE
	t."PULocationID" = zpu."LocationID"
	AND t."DOLocationID" = zdo."LocationID"
LIMIT 100;

# The same query but with explicit JOIN syntax, which is more readable and less error-prone:
SELECT
	t.tpep_pickup_datetime,
	t.tpep_dropoff_datetime,
	total_amount,
	CONCAT(zpu."Borough", ' | ', zpu."Zone") AS "pickup_loc",
	CONCAT(zdo."Borough", ' | ', zdo."Zone") AS "dropoff_loc"
FROM
	yellow_taxi_trips t 
	JOIN 
		zones zpu
	ON t."PULocationID" = zpu."LocationID"
	JOIN 
		zones zdo
	ON t."DOLocationID" = zdo."LocationID"
LIMIT 100;

#Data Quality Checks

## Check for NULL Value in LocationID columns
SELECT
	t.tpep_pickup_datetime,
	t.tpep_dropoff_datetime,
	total_amount,
	t."PULocationID",
	t."DOLocationID"
FROM
	yellow_taxi_trips t 
WHERE
	t."PULocationID" is NULL OR
	t."DOLocationID" is NULL
	
LIMIT 100;

## Check for Location IDs NOT IN Zones Table
SELECT
	t.tpep_pickup_datetime,
	t.tpep_dropoff_datetime,
	total_amount,
	t."PULocationID",
	t."DOLocationID"
FROM
	yellow_taxi_trips t 
WHERE
	t."PULocationID" NOT IN (SELECT "LocationID" from zones) OR
	t."DOLocationID" NOT IN (SELECT "LocationID" from zones)
	
LIMIT 100;

# LEFT JOIN and RIGHT JOIN

SELECT
	t.tpep_pickup_datetime,
	t.tpep_dropoff_datetime,
	total_amount,
	CONCAT(zpu."Borough", ' | ', zpu."Zone") AS "pickup_loc",
	CONCAT(zdo."Borough", ' | ', zdo."Zone") AS "dropoff_loc"
FROM
	yellow_taxi_trips t 
	LEFT JOIN 
		zones zpu
	ON t."PULocationID" = zpu."LocationID"
	LEFT JOIN 
		zones zdo
	ON t."DOLocationID" = zdo."LocationID"
LIMIT 100;

SELECT
	t.tpep_pickup_datetime,
	t.tpep_dropoff_datetime,
	total_amount,
	CONCAT(zpu."Borough", ' | ', zpu."Zone") AS "pickup_loc",
	CONCAT(zdo."Borough", ' | ', zdo."Zone") AS "dropoff_loc"
FROM
	yellow_taxi_trips t 
	RIGHT JOIN 
		zones zpu
	ON t."PULocationID" = zpu."LocationID"
	RIGHT JOIN 
		zones zdo
	ON t."DOLocationID" = zdo."LocationID"
LIMIT 100;


SELECT
	CAST(tpep_dropoff_datetime AS DATE) AS "day",
	COUNT(1) AS "trip_count"
FROM
	yellow_taxi_trips t
GROUP BY 1
LIMIT 100;

SELECT
	CAST(tpep_dropoff_datetime AS DATE) AS "day",
	COUNT(1) AS "trip_count"
FROM
	yellow_taxi_trips t
GROUP BY 1
ORDER BY "day" ASC
LIMIT 100;

SELECT
    CAST(tpep_dropoff_datetime AS DATE) AS "day",
    COUNT(1) AS "count",
    MAX(total_amount) AS "total_amount",
    MAX(passenger_count) AS "passenger_count"
FROM
    yellow_taxi_trips
GROUP BY
    CAST(tpep_dropoff_datetime AS DATE)
ORDER BY
    "count" DESC
LIMIT 100;

SELECT
    CAST(tpep_dropoff_datetime AS DATE) AS "day",
    "DOLocationID",
    COUNT(1) AS "count",
    MAX(total_amount) AS "total_amount",
    MAX(passenger_count) AS "passenger_count"
FROM
    yellow_taxi_trips
GROUP BY
    1, 2
ORDER BY
    "day" ASC,
    "DOLocationID" ASC
LIMIT 100;