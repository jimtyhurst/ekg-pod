from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests


def download_file():
    indata = requests.get(
        'https://raw.githubusercontent.com/tadinve/EKG-Foundations/master/04A-postgress/create_world.sql')
    with open('/opt/airflow/dags/files/create_world.sql', 'w') as outfile:
        outfile.write(indata.text)


with DAG(
    'ty-process-files',
    start_date=datetime(2022, 7, 14, 18, 30),
    tags=['EKG', 'jimtyhurst'],
) as dag:

    check_for_file = HttpSensor(
        task_id='check_for_file',
        http_conn_id='http_conn',
        method='GET',
        endpoint='create_world.sql',
        response_check=lambda response: "CREATE" in response.text,
        poke_interval=5,
        timeout=20
    )

    download_file = PythonOperator(
        task_id='download_file',
        python_callable=download_file,
    )

    upload_file_to_postgres = EmptyOperator(task_id='upload_file_to_postgres')

    move_file_to_processed_folder = EmptyOperator(
        task_id='move_file_to_processed_folder')

    end_dag = EmptyOperator(task_id='end_dag')

    check_for_file >> download_file >> upload_file_to_postgres >> move_file_to_processed_folder >> end_dag
