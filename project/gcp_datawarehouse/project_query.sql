SELECT * FROM `de-bootcamp-final-project.college_data.filed_study` LIMIT 100;

-- external_table
CREATE OR REPLACE EXTERNAL TABLE de-bootcamp-final-project.college_data.external_field_data
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://college-scorecard-data-2024-04/*.parquet']
);


--non partitioned table from external table physical 6.84 MB
CREATE OR REPLACE TABLE college_data.external_field_data_non_partitioned AS
SELECT * FROM college_data.external_field_data;

--partitioned
CREATE OR REPLACE TABLE college_data.external_field_data_partitioned
PARTITION BY RANGE_BUCKET(DISTANCE, GENERATE_ARRAY(0, 4, 1))
AS
SELECT * FROM college_data.external_field_data;
  
-- CLUSTER BY DISTANCE;

-- -- for external table, bytes processed(8.55 MB)
SELECT DISTINCT(UNITID) FROM college_data.external_field_data_partitioned;
-- -- for materialized table 7,952 rows
SELECT DISTINCT(UNITID)FROM college_data.external_field_data_non_partitioned;

SELECT table_name, partition_id, total_rows 
FROM `college_data.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'external_field_data_partitioned'
ORDER BY total_rows DESC;

CREATE OR REPLACE TABLE college_data.external_field_data_partitioned_clustered
PARTITION BY RANGE_BUCKET(DISTANCE, GENERATE_ARRAY(0, 4, 1))
CLUSTER BY CONTROL AS
SELECT * FROM college_data.external_field_data;

-- -- 
-- -- non partitioned ,0
SELECT COUNT(*) FROM college_data.external_field_data_partitioned_clustered
WHERE DISTANCE = 1
AND CONTROL = 'Private, nonprofit';


-- -- non partitioned,15.89 MB
SELECT COUNT(*) FROM college_data.external_field_data_partitioned
WHERE DISTANCE = 1
AND CONTROL = 'Private, nonprofit';

