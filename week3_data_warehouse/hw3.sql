-- external_table
CREATE OR REPLACE EXTERNAL TABLE taxi_rides_ny.external_green_tripdata
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mage-zoomcamp-matt-palmer-7/taxi-rides-ny/*.parquet']
);

--non partitioned table from external table 
CREATE OR REPLACE TABLE taxi_rides_ny.green_tripdata_non_partitioned AS
SELECT * FROM taxi_rides_ny.external_green_tripdata;


-- question 1 get the total rows
SELECT COUNT(*) FROM taxi_rides_ny.external_green_tripdata;

-- question 2
-- for external table
SELECT DISTINCT PULocationID FROM taxi_rides_ny.external_green_tripdata;
-- for materialized table
SELECT DISTINCT PULocationID FROM taxi_rides_ny.green_tripdata_non_partitioned;


-- question 3
SELECT COUNT(*) FROM taxi_rides_ny.external_green_tripdata
WHERE fare_amount = 0;


-- question 5
-- non partitioned
SELECT DISTINCT PULocationID
FROM taxi_rides_ny.green_tripdata_non_partitioned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

-- create partitioned table 
CREATE OR REPLACE TABLE taxi_rides_ny.green_tripdata_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime) 
CLUSTER BY PULocationID AS
SELECT * FROM taxi_rides_ny.external_green_tripdata;
-- get the distinct
SELECT DISTINCT PULocationID
FROM taxi_rides_ny.green_tripdata_partitoned_clustered
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';