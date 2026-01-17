import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
df = pd.read_csv('https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv')
df.to_sql('taxi_zone_lookup', engine, if_exists='replace', index=False)
print(f"Loaded {len(df)} taxi zones")