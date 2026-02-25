"""NYC Taxi data pipeline using dlt"""

import dlt
from dlt.sources.rest_api import rest_api_source

def load_taxi_data() -> None:
    """
    Load NYC taxi trip data from custom API into DuckDB
    """
    
    # Define the REST API source
    source = rest_api_source({
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
        },
        "resources": [
            {
                "name": "trips",
                "endpoint": {
                    "path": "",  # Base URL is the endpoint
                    "params": {
                        "page": {
                            "type": "incremental",
                            "start": 1,
                        }
                    },
                    "paginator": "json_response",  # Auto-detect pagination
                },
            }
        ],
    })

    # Create a dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="nyc_taxi_data",
    )

    # Run the pipeline
    load_info = pipeline.run(source)
    
    # Print load statistics
    print(f"Pipeline run completed!")
    print(f"Loaded {len(load_info.loads_ids)} load packages")
    print(load_info)


if __name__ == "__main__":
    load_taxi_data()