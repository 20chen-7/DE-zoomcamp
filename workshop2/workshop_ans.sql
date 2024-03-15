-----q1------
CREATE MATERIALIZED VIEW highest_avg_trip_time AS
WITH avg_trip_times AS (
    SELECT 
        trip_data.PULocationID, 
        trip_data.DOLocationID, 
        AVG(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/3600) AS avg_time_interval
    FROM 
        trip_data
    GROUP BY 
        trip_data.PULocationID, trip_data.DOLocationID
)
SELECT 
    CONCAT(puZ.Zone,'/',doZ.Zone) AS pairs,
    avg_trip_times.avg_time_interval
FROM 
    avg_trip_times
JOIN 
    taxi_zone puZ ON avg_trip_times.PULocationID = puZ."location_id"
JOIN 
    taxi_zone doZ ON avg_trip_times.DOLocationID = doZ."location_id"
ORDER BY 
    avg_time_interval DESC;

select * from highest_avg_trip_time LIMIT 1;


-----q2------
CREATE MATERIALIZED VIEW highest_avg_trip_cnts AS
WITH avg_trip_times AS (
    SELECT 
        trip_data.PULocationID, 
        trip_data.DOLocationID, 
        AVG(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/3600) AS avg_time_interval
    FROM 
        trip_data
    GROUP BY 
        trip_data.PULocationID, trip_data.DOLocationID
)
SELECT 
    CONCAT(puZ.Zone,'/',doZ.Zone) AS pairs,
    COUNT(*) AS count,
    AVG(avg_trip_times.avg_time_interval) AS avg_time_interval
FROM 
    avg_trip_times
JOIN 
    taxi_zone puZ ON avg_trip_times.PULocationID = puZ."location_id"
JOIN 
    taxi_zone doZ ON avg_trip_times.DOLocationID = doZ."location_id"
GROUP BY
    pairs
ORDER BY 
    avg_time_interval DESC, count;

select * from highest_avg_trip_cnts limit 1;


-----q3------
CREATE MATERIALIZED VIEW latest_pu17_busiest_zones AS
WITH latest_times AS (
    SELECT 
        MAX(tpep_pickup_datetime) AS latestPUtime
    FROM 
        trip_data), 
    recent_puz AS (
        SELECT 
            PULocationID,
            COUNT(1) as cnt
        FROM 
            trip_data,latest_times
        WHERE
            trip_data.tpep_pickup_datetime > latest_times.latestPUtime - INTERVAL '17 hours'
        GROUP BY 
            PULocationID
    )
SELECT 
    recent_puz.cnt AS cnt,
    puZ.Zone AS pickup_zone  
FROM 
    recent_puz
JOIN 
    taxi_zone puZ ON recent_puz.PULocationID = puZ."location_id";

SELECT * FROM latest_pu17_busiest_zones ORDER BY cnt DESC LIMIT 5;

