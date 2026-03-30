/* @bruin
name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: tpep_pickup_datetime
  time_granularity: timestamp

columns:
  - name: vendor_id
    type: BIGINT
    description: Vendor ID
  - name: tpep_pickup_datetime
    type: TIMESTAMP
    description: Pickup datetime
    checks:
      - name: not_null
  - name: tpep_dropoff_datetime
    type: TIMESTAMP
    description: Dropoff datetime
    checks:
      - name: not_null
  - name: passenger_count
    type: DOUBLE
    description: Passenger count
  - name: trip_distance
    type: DOUBLE
    description: Trip distance
  - name: ratecode_id
    type: DOUBLE
    description: Rate code ID
  - name: store_and_fwd_flag
    type: VARCHAR
    description: Store and forward flag
  - name: pu_location_id
    type: BIGINT
    description: Pickup location ID
  - name: do_location_id
    type: BIGINT
    description: Dropoff location ID
  - name: payment_type
    type: BIGINT
    description: Raw payment type ID
  - name: payment_type_name
    type: VARCHAR
    description: Human-readable payment type name
    checks:
      - name: not_null
  - name: fare_amount
    type: DOUBLE
    description: Fare amount
  - name: extra
    type: DOUBLE
    description: Extra amount
  - name: mta_tax
    type: DOUBLE
    description: MTA tax
  - name: tip_amount
    type: DOUBLE
    description: Tip amount
  - name: tolls_amount
    type: DOUBLE
    description: Tolls amount
  - name: improvement_surcharge
    type: DOUBLE
    description: Improvement surcharge
  - name: total_amount
    type: DOUBLE
    description: Total amount
  - name: congestion_surcharge
    type: DOUBLE
    description: Congestion surcharge
  - name: airport_fee
    type: DOUBLE
    description: Airport fee
  - name: taxi_type
    type: VARCHAR
    description: Taxi type (yellow or green)
    checks:
      - name: not_null
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp of ingestion

custom_checks:
  - name: trip_duration_positive
    description: Dropoff should be after pickup
    query: |
      SELECT count(*)
      FROM staging.trips
      WHERE tpep_dropoff_datetime <= tpep_pickup_datetime
    value: 0

@bruin */

WITH raw_trips AS (
    SELECT 
        *,
        -- Deduplicate using a composite key
        ROW_NUMBER() OVER (
            PARTITION BY 
                vendor_id, 
                tpep_pickup_datetime, 
                tpep_dropoff_datetime, 
                pu_location_id, 
                do_location_id, 
                fare_amount 
            ORDER BY extracted_at DESC
        ) as row_num
    FROM ingestion.trips
    WHERE tpep_pickup_datetime >= '{{ start_datetime }}'
      AND tpep_pickup_datetime < '{{ end_datetime }}'
      -- Filter out invalid data (trips where duration is zero or negative)
      AND tpep_dropoff_datetime > tpep_pickup_datetime
      -- Filter out invalid fares and distances (negative values are likely refunds or errors)
      AND fare_amount >= 0  
      AND trip_distance >= 0
)

SELECT 
    t.vendor_id,
    t.tpep_pickup_datetime,
    t.tpep_dropoff_datetime,
    t.passenger_count,
    t.trip_distance,
    t.ratecode_id,
    t.store_and_fwd_flag,
    t.pu_location_id,
    t.do_location_id,
    t.payment_type,
    p.payment_type_name,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.total_amount,
    t.congestion_surcharge,
    t.airport_fee,
    t.taxi_type,
    t.extracted_at
FROM raw_trips t
LEFT JOIN ingestion.payment_lookup p 
    ON t.payment_type = p.payment_type_id
WHERE t.row_num = 1
