import json
import os
from datetime import datetime
from pathlib import Path

import boto3
import pandas as pd
import requests
import s3fs
import yaml
from airflow import DAG
from airflow.decorators import task

BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"
S3_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL")

sources_path = Path(__file__).parent / "sources.yaml"
with open(sources_path) as f:
    sources = yaml.safe_load(f)

with DAG(
    dag_id="exercise4_dynamic",
    start_date=datetime(2026, 1, 1),
    schedule="0 14 * * *",
    catchup=False,
    default_args={
        "retries": 1,
    },
) as dag:

    for source_name, config in sources.items():

        @task(task_id=f"fetch_{source_name}")
        def fetch_and_upload(url, name, result_key=None, ds=None):
            response = requests.get(url)
            data = response.json()
            records = data[result_key] if result_key else data

            s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT)
            s3_key = f"{name}/_created_date={ds}/dump.json"

            s3.put_object(
                Bucket=BRONZE_BUCKET,
                Key=s3_key,
                Body=json.dumps(records),
            )

            return s3_key

        @task(task_id=f"to_silver_{source_name}")
        def to_silver(s3_key, name, ds=None):
            fs = s3fs.S3FileSystem(
                endpoint_url=S3_ENDPOINT,
                key="minioadmin",
                secret="minioadmin",
            )

            with fs.open(f"{BRONZE_BUCKET}/{s3_key}") as f:
                df = pd.read_json(f)

            silver_key = f"{SILVER_BUCKET}/{name}/_created_date={ds}/data.snappy.parquet"
            with fs.open(silver_key, "wb") as f:
                df.to_parquet(f, compression="snappy", index=False)

            return f"s3://{silver_key}"

        bronze_key = fetch_and_upload(config["url"], source_name, config.get("result_key"))
        to_silver(bronze_key, source_name)
