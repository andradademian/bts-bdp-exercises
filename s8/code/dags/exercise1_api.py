import json
from datetime import datetime

import requests
from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="exercise1_api",
    start_date=datetime(2026, 1, 1),
    schedule="0 14 * * *",
    catchup=False,
) as dag:

    @task()
    def fetch_api():
        response = requests.get("https://api.chucknorris.io/jokes/search?query=computer")
        data = response.json()

        with open("/tmp/chuck_jokes.json", "w") as f:
            json.dump(data["result"], f)

        return "/tmp/chuck_jokes.json"

    fetch_api()
