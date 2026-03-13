import json
import os
from datetime import datetime

import boto3
import pandas as pd
import requests
import s3fs
from airflow import DAG
from airflow.decorators import task

BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"
S3_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL")

with DAG(
    dag_id="exercise3_silver",
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
        s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT)
        s3_key = f"chuck_jokes/_created_date={ds}/dump.json"
        s3.upload_file(local_path, BRONZE_BUCKET, s3_key)

        return s3_key

    @task()
    def bronze_to_silver(s3_key, ds=None):
        fs = s3fs.S3FileSystem(
            endpoint_url=S3_ENDPOINT,
            key="minioadmin",
            secret="minioadmin",
        )

        with fs.open(f"{BRONZE_BUCKET}/{s3_key}") as f:
            df = pd.read_json(f)

        silver_key = f"{SILVER_BUCKET}/chuck_jokes/_created_date={ds}/data.snappy.parquet"
        with fs.open(silver_key, "wb") as f:
            df.to_parquet(f, compression="snappy", index=False)

        return f"s3://{silver_key}"

    @task()
    def fetch_universities():
        response = requests.get("http://universities.hipolabs.com/search?country=Spain")
        data = response.json()

        local_path = "/tmp/universities_spain.json"
        with open(local_path, "w") as f:
            json.dump(data, f)

        return local_path

    @task()
    def upload_universities_to_s3(local_path, ds=None):
        s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT)
        s3_key = f"universities/_created_date={ds}/dump.json"
        s3.upload_file(local_path, BRONZE_BUCKET, s3_key)

        return s3_key

    @task()
    def universities_bronze_to_silver(s3_key, ds=None):
        fs = s3fs.S3FileSystem(
            endpoint_url=S3_ENDPOINT,
            key="minioadmin",
            secret="minioadmin",
        )

        with fs.open(f"{BRONZE_BUCKET}/{s3_key}") as f:
            df = pd.read_json(f)

        silver_key = f"{SILVER_BUCKET}/universities/_created_date={ds}/data.snappy.parquet"
        with fs.open(silver_key, "wb") as f:
            df.to_parquet(f, compression="snappy", index=False)

        return f"s3://{silver_key}"

    local_file = fetch_api()
    s3_key = upload_to_s3(local_file)
    bronze_to_silver(s3_key)

    uni_file = fetch_universities()
    uni_s3_key = upload_universities_to_s3(uni_file)
    universities_bronze_to_silver(uni_s3_key)
