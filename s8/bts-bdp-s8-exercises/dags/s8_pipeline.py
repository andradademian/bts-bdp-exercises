import json
import os
import gzip
import requests
from datetime import datetime
from airflow import DAG
from airflow.decorators import task

BASE_URL = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"
BRONZE_DIR = "/tmp/bronze/aircraft"
SILVER_DIR = "/tmp/silver/aircraft"
DB_PATH = "/tmp/aircraft.db"

os.makedirs(BRONZE_DIR, exist_ok=True)
os.makedirs(SILVER_DIR, exist_ok=True)

with DAG(
    dag_id="s8_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    @task()
    def download_bronze():
        """Download raw tracking JSON files and save to bronze layer."""

        files_to_download = [f"{hour:02d}0000Z.json.gz" for hour in range(3)]

        downloaded = []
        for filename in files_to_download:
            url = BASE_URL + filename
            dest = os.path.join(BRONZE_DIR, filename)

            print(f"Downloading {url}...")
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            with open(dest, "wb") as f:
                f.write(response.content)

            print(f"Saved to {dest}")
            downloaded.append(dest)

        return downloaded

    @task()
    def process_silver(bronze_files: list):
        """Parse bronze JSON files and save clean parquet to silver layer."""
        import pandas as pd

        all_aircraft = []

        for filepath in bronze_files:
            print(f"Processing {filepath}...")
            try:
                with gzip.open(filepath, "rt", encoding="utf-8") as f:
                    data = json.load(f)
            except (OSError, gzip.BadGzipFile):
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)

            if not data or "aircraft" not in data:
                continue

            for ac in data["aircraft"]:
                icao = ac.get("hex")
                if not icao:
                    continue
                all_aircraft.append({
                    "icao": icao,
                    "registration": ac.get("r"),
                    "type": ac.get("t"),
                })

        df = pd.DataFrame(all_aircraft)
        df = df.drop_duplicates(subset="icao", keep="last")
        df = df.sort_values("icao")

        output_path = os.path.join(SILVER_DIR, "aircraft.parquet")
        df.to_parquet(output_path, index=False)
        print(f"Saved {len(df)} aircraft to {output_path}")

        return output_path


    @task()
    def enrich_data(silver_path: str):
        """Enrich aircraft data with owner, manufacturer, model from aircraft database."""
        import pandas as pd
        import io

        # Download aircraft database CSV
        csv_url = "https://opensky-network.org/datasets/metadata/aircraftDatabase.csv"
        print("Downloading aircraft database...")
        response = requests.get(csv_url, timeout=60)
        response.raise_for_status()

        # Parse CSV
        db_df = pd.read_csv(io.StringIO(response.text), low_memory=False)
        db_df = db_df.rename(columns={
            "icao24": "icao",
            "owner": "owner",
            "manufacturername": "manufacturer",
            "model": "model",
        })
        db_df = db_df[["icao", "owner", "manufacturer", "model"]]
        db_df["icao"] = db_df["icao"].str.lower().str.strip()

        # Load silver data
        silver_df = pd.read_parquet(silver_path)
        silver_df["icao"] = silver_df["icao"].str.lower().str.strip()

        # Merge
        enriched_df = silver_df.merge(db_df, on="icao", how="left")
        enriched_df = enriched_df.sort_values("icao")

        # Save enriched parquet
        output_path = os.path.join(SILVER_DIR, "aircraft_enriched.parquet")
        enriched_df.to_parquet(output_path, index=False)
        print(f"Saved {len(enriched_df)} enriched aircraft to {output_path}")

        return output_path


    @task()
    def load_to_db(enriched_path: str):
        """Load enriched aircraft data into SQLite database."""
        import pandas as pd
        import sqlite3

        print("Loading enriched data into SQLite...")
        df = pd.read_parquet(enriched_path)

        conn = sqlite3.connect(DB_PATH)
        df.to_sql("aircraft", conn, if_exists="replace", index=False)
        conn.close()

        print(f"Loaded {len(df)} aircraft into {DB_PATH}")
        return DB_PATH


    #task chain
    bronze_files = download_bronze()
    silver_path = process_silver(bronze_files)
    enriched_path = enrich_data(silver_path)
    load_to_db(enriched_path)