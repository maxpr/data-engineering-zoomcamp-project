import os

from airflow import DAG
from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from million_song_processor import million_to_parquet, upload_to_gcs


AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow') 
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# Schedule ingestion every 2nd day of the month.
local_workflow = DAG(
    "MillionSong-DAG",
    start_date=days_ago(1),
    catchup=True,
    max_active_runs=2
)

URL_BASE = 'http://labrosa.ee.columbia.edu/~dpwe/tmp/millionsongsubset.tar.gz'
FILE_NAME = 'millionsongsubset.tar.gz'

OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + "/" + FILE_NAME
FOLDER_NAME = 'millionsongsubset'

with local_workflow:

    # download 
    download_archive = BashOperator(
        task_id="wget_data",
        bash_command=f"wget {URL_BASE} -O {OUTPUT_FILE_TEMPLATE}"
    )
    
    # Unpack
    unpack_archive = BashOperator(
        task_id="unzip_data",
        bash_command=f"mkdir {AIRFLOW_HOME + '/' + FOLDER_NAME} && tar -xvf {OUTPUT_FILE_TEMPLATE} -C {AIRFLOW_HOME + '/' + FOLDER_NAME}"
    )
    
    # Read and extract
    read_data_as_parquet = PythonOperator(
        task_id="process_data_to_parquet",
        python_callable=million_to_parquet,
        op_kwargs={
            "original_path": f"{AIRFLOW_HOME + '/' + FOLDER_NAME}",
            "output_path": f"{OUTPUT_FILE_TEMPLATE.replace('.tar.gz','.parquet')}"
        }
    )

    # Upload it to GCS
    local_to_gcs_task = PythonOperator(
        task_id="save_to_gcp_datalake",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{OUTPUT_FILE_TEMPLATE.split('/')[-1].replace('.tar.gz','.parquet')}",
            "local_file": f"{OUTPUT_FILE_TEMPLATE.replace('.tar.gz','.parquet')}",
        },
    )

download_archive >> unpack_archive >> read_data_as_parquet >> local_to_gcs_task