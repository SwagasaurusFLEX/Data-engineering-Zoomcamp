import requests
import json
import base64
import os
from google.cloud import storage

BUCKET_NAME = "kestra_zoomcamp_bucket6942"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
FILES = [
    "yellow_tripdata_2024-01.parquet",
    "yellow_tripdata_2024-02.parquet",
    "yellow_tripdata_2024-03.parquet",
    "yellow_tripdata_2024-04.parquet",
    "yellow_tripdata_2024-05.parquet",
    "yellow_tripdata_2024-06.parquet",
]

def download_and_upload():
    sa_base64 = os.environ["SECRET_GCP_SERVICE_ACCOUNT"]
    sa_json = json.loads(base64.b64decode(sa_base64))
    client = storage.Client.from_service_account_info(sa_json)
    bucket = client.bucket(BUCKET_NAME)

    for file in FILES:
        print(f"Downloading {file}...")
        response = requests.get(f"{BASE_URL}/{file}")
        
        if response.status_code == 200:
            print(f"Uploading {file} to GCS...")
            blob = bucket.blob(file)
            blob.upload_from_string(response.content)
            print(f"✅ {file} uploaded successfully")
        else:
            print(f"❌ Failed to download {file}: {response.status_code}")

if __name__ == "__main__":
    download_and_upload()
