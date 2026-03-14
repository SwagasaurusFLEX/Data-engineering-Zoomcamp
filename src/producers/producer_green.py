import json
from time import time

import pandas as pd
from kafka import KafkaProducer


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


def row_to_dict(row):
    return {
        "lpep_pickup_datetime": str(row["lpep_pickup_datetime"]),
        "lpep_dropoff_datetime": str(row["lpep_dropoff_datetime"]),
        "PULocationID": int(row["PULocationID"]),
        "DOLocationID": int(row["DOLocationID"]),
        "passenger_count": int(row["passenger_count"]) if pd.notna(row["passenger_count"]) else None,
        "trip_distance": float(row["trip_distance"]) if pd.notna(row["trip_distance"]) else None,
        "tip_amount": float(row["tip_amount"]) if pd.notna(row["tip_amount"]) else None,
        "total_amount": float(row["total_amount"]) if pd.notna(row["total_amount"]) else None,
    }


def main():
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"

    columns = [
        "lpep_pickup_datetime",
        "lpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "passenger_count",
        "trip_distance",
        "tip_amount",
        "total_amount",
    ]

    df = pd.read_parquet(url, columns=columns)

    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=json_serializer,
    )

    topic_name = "green-trips"

    t0 = time()

    for _, row in df.iterrows():
        producer.send(topic_name, value=row_to_dict(row))

    producer.flush()

    t1 = time()
    print(f"took {(t1 - t0):.2f} seconds")


if __name__ == "__main__":
    main()