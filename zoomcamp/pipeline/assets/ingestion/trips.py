"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: create+replace

columns:
  - name: pickup_datetime
    type: VARCHAR
    description: When the meter was engaged
  - name: dropoff_datetime
    type: VARCHAR
    description: When the meter was disengaged
  - name: pickup_location_id
    type: BIGINT
  - name: dropoff_location_id
    type: BIGINT
  - name: fare_amount
    type: DOUBLE
  - name: payment_type
    type: BIGINT
@bruin"""

import json
import os
from datetime import datetime

import pandas as pd

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month:02d}.parquet"


def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    taxi_types = json.loads(os.environ.get("BRUIN_VARS", "{}")).get("taxi_types", ["yellow"])

    start = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
    end = datetime.fromisoformat(end_date.replace("Z", "+00:00"))

    frames = []
    year_month = (start.year, start.month)
    end_ym = (end.year, end.month)
    while year_month <= end_ym:
        y, m = year_month
        for taxi_type in taxi_types:
            url = BASE_URL.format(taxi_type=taxi_type, year=y, month=m)
            try:
                df = pd.read_parquet(url)
                col_map = {}
                pickup_col = "tpep_pickup_datetime" if "tpep_pickup_datetime" in df.columns else "pickup_datetime"
                dropoff_col = "tpep_dropoff_datetime" if "tpep_dropoff_datetime" in df.columns else "dropoff_datetime"
                col_map[pickup_col] = "pickup_datetime"
                col_map[dropoff_col] = "dropoff_datetime"

                pul_col = "PULocationID" if "PULocationID" in df.columns else ("pickup_location_id" if "pickup_location_id" in df.columns else None)
                dol_col = "DOLocationID" if "DOLocationID" in df.columns else ("dropoff_location_id" if "dropoff_location_id" in df.columns else None)
                if pul_col:
                    col_map[pul_col] = "pickup_location_id"
                if dol_col:
                    col_map[dol_col] = "dropoff_location_id"

                if "fare_amount" in df.columns:
                    col_map["fare_amount"] = "fare_amount"
                if "payment_type" in df.columns:
                    col_map["payment_type"] = "payment_type"

                out = df[list(col_map.keys())].rename(columns=col_map)
                frames.append(out)
            except Exception:
                pass

        year_month = (y + 1, 1) if m == 12 else (y, m + 1)

    if frames:
        final_dataframe = pd.concat(frames, ignore_index=True)
        required_cols = [
            "pickup_datetime",
            "dropoff_datetime",
            "pickup_location_id",
            "dropoff_location_id",
            "fare_amount",
            "payment_type",
        ]
        for col in required_cols:
            if col not in final_dataframe.columns:
                final_dataframe[col] = None

        for col in ("pickup_datetime", "dropoff_datetime"):
            ser = final_dataframe[col]
            if hasattr(ser.dtype, "tz") and ser.dtype.tz is not None:
                ser = pd.to_datetime(ser.astype("int64"), unit="ns")
            final_dataframe[col] = ser.astype("datetime64[ns]").dt.strftime("%Y-%m-%d %H:%M:%S.%f")

        final_dataframe = final_dataframe[required_cols]
    else:
        final_dataframe = pd.DataFrame(
            {
                "pickup_datetime": pd.Series(dtype="object"),
                "dropoff_datetime": pd.Series(dtype="object"),
                "pickup_location_id": pd.Series(dtype="Int64"),
                "dropoff_location_id": pd.Series(dtype="Int64"),
                "fare_amount": pd.Series(dtype="float64"),
                "payment_type": pd.Series(dtype="Int64"),
            }
        )

    return final_dataframe
