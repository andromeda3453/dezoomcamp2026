

import pandas as pd
from sqlalchemy import create_engine


df_green = pd.read_parquet(
    "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet")
df_zones = pd.read_csv(
    "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv")

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

df_green.to_sql(name="green_tripdata_2025-11", con=engine, if_exists="replace")
df_zones.to_sql(name="taxi_zones", con=engine, if_exists="replace")
