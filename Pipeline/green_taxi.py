import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
df = pd.read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet')
df.to_sql('green_taxi_trips_2025_11', engine, if_exists='replace', index=False)
print(f"Loaded {len(df)} green taxi trips")