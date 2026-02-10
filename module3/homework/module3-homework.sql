CREATE OR REPLACE EXTERNAL TABLE
`kestra_dataset.ny_taxi_external_table`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://kestra-bucket-dezoomcamp2026-485910/yellow_tripdata_2024-*.parquet']
);

CREATE OR REPLACE TABLE
`kestra_dataset.ny_taxi`
AS
SELECT * FROM `kestra_dataset.ny_taxi_external_table`;

SELECT DISTINCT(PULocationID) FROM `kestra_dataset.ny_taxi_external_table`;

SELECT DISTINCT(PULocationID) FROM `kestra_dataset.ny_taxi`;


SELECT PULocationID FROM `kestra_dataset.ny_taxi`;
SELECT PULocationID, DOLocationID FROM `kestra_dataset.ny_taxi`;


SELECT COUNT(1) FROM `kestra_dataset.ny_taxi`
WHERE fare_amount = 0;


CREATE OR REPLACE TABLE `kestra_dataset.ny_taxi_partitioned`
PARTITION BY DATE(
   tpep_dropoff_datetime
)
CLUSTER BY VendorID
AS 
SELECT * FROM `kestra_dataset.ny_taxi_external_table`;


SELECT DISTINCT(VendorID) from `kestra_dataset.ny_taxi_partitioned`
WHERE tpep_dropoff_datetime between '2024-03-01' AND '2024-03-15';




