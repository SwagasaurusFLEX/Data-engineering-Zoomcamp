"""NYC Taxi data pipeline using dlt"""

import dlt
import requests


@dlt.resource(name="trips", write_disposition="replace")
def taxi_trips():
    base_url = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"
    page = 1

    while True:
        response = requests.get(base_url, params={"page": page})
        response.raise_for_status()
        data = response.json()

        if not data:
            break

        yield data
        page += 1


def load_taxi_data() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="nyc_taxi_data",
    )

    load_info = pipeline.run(taxi_trips())
    print(load_info)


if __name__ == "__main__":
    load_taxi_data()