from airflow.sdk import dag, task, asset
from pendulum import datetime
import os

@asset(
    schedule="@daily",
    uri = "/opt/airflow/logs/data/data_extract.txt",
    name = "fetch_data"
)
def fetch_data(self):
    os.makedirs(os.path.dirname(fetch_data.uri), exist_ok=True)
    with open(fetch_data.uri, "w") as f:
        f.write(f"Data extracted successfully!\n")

        print(f"Data successfully witten to {self.uri}")