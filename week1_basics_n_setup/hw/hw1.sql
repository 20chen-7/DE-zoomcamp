
-- example
SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    total_amount,
    CONCAT(zpu."Borough", ' / ' , zpu."Zone") AS "pickup_loc",
    CONCAT(zdo."Borough", ' / ', zdo."Zone") AS "dropoff_loc"
FROM
    yellow_taxi_trips t 
    JOIN zones zpu
        ON t."PULocationID" = zpu."LocationID"
    JOIN zones zdo
        ON t."DOLocationID" = zdo."LocationID"
LIMIT 100;

-- q3
SELECT 
	CAST(lpep_pickup_datetime AS DATE) AS lpd,
	CAST(lpep_dropoff_datetime AS DATE) AS ldd,
	COUNT(1)
FROM green_taxi_data 
WHERE CAST(lpep_pickup_datetime AS DATE) = '2019-09-18'
	AND
	CAST(lpep_dropoff_datetime AS DATE) = '2019-09-18'
GROUP BY lpd, ldd

-- q4
SELECT 
	CAST(lpep_pickup_datetime AS DATE) AS "day",
	trip_distance
FROM green_taxi_data
ORDER BY trip_distance desc, CAST(lpep_pickup_datetime AS DATE)


-- q5
SELECT 
	CAST(g.lpep_pickup_datetime AS DATE) as "day",
	SUM(g.total_amount) AS "sum",
	z."Borough"
FROM 
	green_taxi_data g
	JOIN
	zones z ON g."PULocationID" = z."LocationID"
WHERE
	z."Borough" IS NOT NULL
	AND
	CAST(g.lpep_pickup_datetime AS DATE) = '2019-09-18'
GROUP BY 
	CAST(g.lpep_pickup_datetime AS DATE), z."Borough"
ORDER BY
	"sum" DESC;


-- q6
SELECT
g.tip_amount,
TO_CHAR(CAST(g.lpep_pickup_datetime AS TIMESTAMP), 'YYYY-MM') AS "yearMonth",
zpu."Zone" AS "puZone",
zdo."Zone" AS "doZone"
FROM
green_taxi_data g
JOIN
zones zpu
ON
g."PULocationID" = zpu."LocationID"
JOIN
zones zdo
ON
g."DOLocationID" = zdo."LocationID"
WHERE
TO_CHAR(CAST(g.lpep_pickup_datetime AS TIMESTAMP), 'YYYY-MM') = '2019-09'
ORDER BY g.tip_amount DESC
-- SELECT * FROM zones
-- LIMIT 100;
