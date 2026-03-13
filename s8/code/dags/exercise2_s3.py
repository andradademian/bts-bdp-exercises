import json
import os
from datetime import datetime

import boto3
import requests
from airflow import DAG
from airflow.decorators import task

BRONZE_BUCKET = "bronze"

with DAG(
    dag_id="exercise2_s3",
    start_date=datetime(2026, 1, 1),
    schedule="0 14 * * *",
    catchup=False,
    default_args={
        "retries": 1,
    },
) as dag:

    @task()
    def fetch_api():
        response = requests.get("https://api.chucknorris.io/jokes/search?query=computer")
        data = response.json()

        local_path = "/tmp/chuck_jokes.json"
        with open(local_path, "w") as f:
            json.dump(data["result"], f)

        return local_path

    @task()
    def upload_to_s3(local_path, ds=None):
        s3 = boto3.client("s3", endpoint_url=os.environ.get("AWS_ENDPOINT_URL"))
        s3_key = f"chuck_jokes/_created_date={ds}/dump.json"

        s3.upload_file(local_path, BRONZE_BUCKET, s3_key)

        return s3_key

    local_file = fetch_api()
    upload_to_s3(local_file)
