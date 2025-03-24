from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from io import BytesIO
import gzip
import pandas as pd
from datetime import datetime

BUCKET_NAME = "modelisation-bucket"
PREFIX = "Air Traffic/actual"

def check_files(ti):
    valid_files = []
    invalid_files = []
    correct_schema = "flight_leg_operating_carrier,flight_leg_international,flight_leg_origin_airport,flight_leg_origin_terminal,flight_leg_origin_city,flight_leg_origin_region,flight_leg_origin_country,flight_leg_destination_airport,flight_leg_destination_city,flight_leg_destination_region,flight_leg_destination_country,flight_leg_departure_date,extraction_date,pax_profile,pax_transferring_leg_origin,pax_nationality,pax,processingyear,processingmonth,processingday"
    storage_client = GCSHook()
    files = ti.xcom_pull(task_ids="list_actual_folder")
    if files:
        for file in files:
            blob = storage_client.download(bucket_name=BUCKET_NAME, object_name=file)
            with BytesIO(blob) as f:
                with gzip.open(f, "rt") as gz_file:
                    df = pd.read_csv(gz_file, nrows=0)
            string_schema = ",".join(df.columns)
            if (string_schema == correct_schema):
                valid_files.append(file)
            else:
                invalid_files.append(file)
        ti.xcoms_push(key="valid_files", value=valid_files)
        ti.xcoms_push(key="invalid_files", value=invalid_files)
    else:
        raise ValueError("no file found in the bucket")



default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}
with DAG(
    "achille_test_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:
    get_list_actual = GCSListObjectsOperator(
        task_id = "list_actual_folder",
        bucket=BUCKET_NAME,
        prefix = PREFIX
    )
    
    check_files = PythonOperator(
        task_id = "check_files",
        python_callable = check_files
    )

    move_files = 
get_list_actual >> check_files >> 