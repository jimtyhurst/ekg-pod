from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    'ty-process-files',
    start_date=datetime(2022, 7, 14, 18, 30),
    tags=['EKG', 'jimtyhurst'],
) as dag:

    check_for_files = EmptyOperator(task_id='check_for_files')

    download_files = EmptyOperator(task_id='download_files')

    upload_to_postgres = EmptyOperator(task_id='upload_to_postgres')

    move_files = EmptyOperator(task_id='move_files')

    end = EmptyOperator(task_id='end')

    check_for_files >> download_files >> upload_to_postgres >> move_files >> end
