from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import storage
import pandas as pd
import gcsfs

import json
from io import BytesIO
import gzip
import pandas as pd
from datetime import datetime

BUCKET_NAME = "modelisation-bucket"
PREFIX = "Air Traffic/actual/"
INVALID_PREFIX = "not_ready_to_load_yet_but_its_a_question_of_minutes/actual/"
VALID_PREFIX = "ready_to_load/actual/"

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
        
        ti.xcom_push(key="valid_files", value=valid_files)
        ti.xcom_push(key="invalid_files", value=invalid_files)
        print(invalid_files, valid_files)
    else:
        raise ValueError("no file found in the bucket")

def move_files(**kwargs):
    ti = kwargs["ti"]
    valid_files = ti.xcom_pull(task_ids="check_files", key="valid_files")
    invalid_files = ti.xcom_pull(task_ids = "check_files", key="invalid_files")
    
    client = storage.Client()
    source_bucket = client.bucket(BUCKET_NAME)
    destination_bucket = client.bucket(BUCKET_NAME)
    
    valid_files = [file.split("/")[-1] for file in valid_files]
    invalid_files = [file.split("/")[-1] for file in invalid_files]
        
    for file_name in valid_files:
        source_blob = source_bucket.blob(PREFIX + file_name)
        source_bucket.copy_blob(source_blob, destination_bucket, VALID_PREFIX + file_name)
        # source_blob.delete()
        print(f"Fichier valide {file_name} déplacé vers {destination_bucket}/{VALID_PREFIX + file_name}")

    for file_name in invalid_files:
        source_blob = source_bucket.blob(PREFIX + file_name)
        source_bucket.copy_blob(source_blob, destination_bucket, INVALID_PREFIX + file_name)
        print(f"Fichier invalide {file_name} déplacé vers {destination_bucket}/{INVALID_PREFIX + file_name}")
        
def modifie_wrong_files(**kwargs):
    ti = kwargs["ti"]
    invalid_files = ti.xcom_pull(task_ids = "list_invalid_folder")
    missing_cols = ["processingyear","processingmonth","processingday"]
    col_to_swap = ["pax_transferring_leg_origin","pax_nationality"]
    
    
    # fs = gcsfs.GCSFileSystem()
    # test_file = invalid_files[0]
    
    # with fs.open(f"{BUCKET_NAME}/{test_file}", 'rb') as f:
    #     df = pd.read_csv(f, compression="gzip")
    
    # for column in missing_cols:
    #     df[column] = None
    
    # df[col_to_swap[0]], df[col_to_swap[1]] = df[col_to_swap[1]], df[col_to_swap[0]]
    
    # valid_file_path = f"{VALID_PREFIX}{test_file}"
    
    # with fs.open(f"{BUCKET_NAME}/{valid_file_path}", 'wb') as f:
    #     df.to_csv(f, index=False, compression="gzip")
    
    # -----------------------------------------------------
    
    gcs_hook = GCSHook()
    invalid_files = invalid_files[1:]
    test_file = invalid_files[0]
    print(invalid_files)
    print()
    print(test_file)
    
    file_content = gcs_hook.download(bucket_name=BUCKET_NAME, object_name=test_file)
    print("download ok")
    
    with BytesIO(file_content) as f:
        with gzip.open(f, "rt") as gz_file:
            df = pd.read_csv(gz_file)
    print("stream ok and load df ok")
    for column in missing_cols:
        df[column] = None
    df[col_to_swap[0]], df[col_to_swap[1]] = df[col_to_swap[1]], df[col_to_swap[0]]
    valid_file_path = f"{VALID_PREFIX}{test_file}"
    print("transfo ok")
    output_stream = BytesIO()
    df.to_csv(output_stream, index=False, compression="gzip")
    print("df to csv ok")
    output_stream.seek(0)
    gcs_hook.upload(bucket_name=BUCKET_NAME, object_name=valid_file_path, data=output_stream)
    print("upload ok")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}
with DAG(
    "amt_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    
    get_list_actual = GCSListObjectsOperator(
        task_id = "list_actual_folder",
        bucket=BUCKET_NAME,
        prefix = PREFIX
    )
    
    get_list_invalid = GCSListObjectsOperator(
        task_id = "list_invalid_folder",
        bucket=BUCKET_NAME,
        prefix = INVALID_PREFIX
    )
    
    check_files = PythonOperator(
        task_id = "check_files",
        python_callable = check_files,
        provide_context = True
    )
    
    move_files = PythonOperator(
        task_id = "move_files",
        provide_context = True,
        python_callable = move_files,
        dag=dag
    )
    
    
    modifie_and_load_invalid_files = PythonOperator(
        task_id = "modify_invalid_file_and_move_them",
        provide_context = True,
        python_callable = modifie_wrong_files,
        dag = dag
    )

get_list_actual >> check_files >> move_files >> get_list_invalid >> modifie_and_load_invalid_files