/* @bruin
name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: tpep_pickup_datetime
  time_granularity: timestamp

columns:
  - name: tpep_pickup_datetime
    type: TIMESTAMP
    description: Pickup date/time (truncated to day for aggregation)
    primary_key: true
  - name: taxi_type
    type: VARCHAR
    description: Taxi type (yellow or green)
    primary_key: true
  - name: payment_type_name
    type: VARCHAR
    description: Human-readable payment type name
    primary_key: true
  - name: trip_count
    type: BIGINT
    description: Total number of trips
    checks:
      - name: non_negative
  - name: total_fare_amount
    type: DOUBLE
    description: Sum of fare amounts
    checks:
      - name: non_negative
  - name: total_passenger_count
    type: BIGINT
    description: Sum of passenger counts
    checks:
      - name: non_negative
  - name: avg_trip_distance
    type: DOUBLE
    description: Average trip distance in miles
    checks:
      - name: non_negative

@bruin */

SELECT 
    CAST(tpep_pickup_datetime AS DATE) as tpep_pickup_datetime,
    taxi_type,
    payment_type_name,
    COUNT(*) as trip_count,
    SUM(fare_amount) as total_fare_amount,
    SUM(passenger_count) as total_passenger_count,
    AVG(trip_distance) as avg_trip_distance
FROM staging.trips
WHERE tpep_pickup_datetime >= '{{ start_datetime }}'
  AND tpep_pickup_datetime < '{{ end_datetime }}'
GROUP BY 1, 2, 3
