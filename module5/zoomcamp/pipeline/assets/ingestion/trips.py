"""@bruin

name: ingestion.trips
connection: duckdb-default

materialization:
  type: table
  strategy: append
image: python:3.11

secrets:
  - key: duckdb-default
    inject_as: duckdb-default

columns:
  - name: VendorID
    type: integer
  - name: tpep_pickup_datetime
    type: TIMESTAMP
  - name: tpep_dropoff_datetime
    type: TIMESTAMP
  - name: passenger_count
    type: DOUBLE
  - name: trip_distance
    type: DOUBLE
  - name: RatecodeID
    type: integer
  - name: store_and_fwd_flag
    type: VARCHAR
  - name: PULocationID
    type: integer
  - name: DOLocationID
    type: integer
  - name: payment_type
    type: BIGINT
  - name: fare_amount
    type: DOUBLE
  - name: extra
    type: DOUBLE
  - name: mta_tax
    type: DOUBLE
  - name: tip_amount
    type: DOUBLE
  - name: tolls_amount
    type: DOUBLE
  - name: improvement_surcharge
    type: DOUBLE
  - name: total_amount
    type: DOUBLE
  - name: congestion_surcharge
    type: DOUBLE
  - name: airport_fee
    type: DOUBLE
  - name: taxi_type
    type: VARCHAR
  - name: extracted_at
    type: TIMESTAMP
  - name: vendor_id
    type: BIGINT
  - name: ratecode_id
    type: DOUBLE
  - name: pu_location_id
    type: BIGINT
  - name: do_location_id
    type: BIGINT

@bruin"""

import os
import json
import requests
import pandas as pd
from io import BytesIO
from datetime import datetime
from dateutil.relativedelta import relativedelta

def materialize():
    # Fetch environment and pipeline variables explicitly provided by Bruin 
    start_date = os.environ.get("BRUIN_START_DATE")
    end_date = os.environ.get("BRUIN_END_DATE")
    
    bruin_vars = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = bruin_vars.get("taxi_types", ["yellow"])
    
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # Data is partitioned by month. Align start and end dates to first day of month.
    current_dt = start_dt.replace(day=1)
    end_month = end_dt.replace(day=1)

    dfs = []
    
    # Iterate through the monthly partitions and taxi types
    while current_dt <= end_month:
        year_str = current_dt.strftime("%Y")
        month_str = current_dt.strftime("%m")
        
        for taxi_type in taxi_types:
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year_str}-{month_str}.parquet"
            try:
                response = requests.get(url, timeout=60)
                if response.status_code == 200:
                    df = pd.read_parquet(BytesIO(response.content))
                    
                    # Keep raw format but add some basic lineage columns
                    df['taxi_type'] = taxi_type
                    df['extracted_at'] = pd.Timestamp.now()
                    
                    dfs.append(df)
                    print(f"Successfully fetched for {taxi_type} - {year_str}-{month_str}.")
                else:
                    print(f"Failed to fetch {url}: Status {response.status_code}")
            except Exception as e:
                print(f"Error fetching {url}: {e}")
                
        # Advance by 1 month
        current_dt += relativedelta(months=1)
        
    # Return dataframe to be written into the DB by Bruin Python materialization layer
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame()
