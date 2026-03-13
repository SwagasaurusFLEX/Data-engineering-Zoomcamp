import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

import time
import pandas as pd
from kafka import KafkaProducer

from models import ride_from_row, ride_serializer


url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet"

columns = [
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "total_amount",
    "tpep_pickup_datetime",
]

df = pd.read_parquet(url, columns=columns).head(1000)

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=ride_serializer
)

topic_name = "rides"

t0 = time.time()

for _, row in df.iterrows():
    ride = ride_from_row(row)
    producer.send(topic_name, value=ride)
    print(f"Sent: {ride}")
    time.sleep(0.01)

producer.flush()

t1 = time.time()
print(f"took {(t1 - t0):.2f} seconds")