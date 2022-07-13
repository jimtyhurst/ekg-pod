from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_airflow():
    print ('Hello, Airflow! (DAG with Python task)')

with DAG(
    dag_id="ty_hello_python_dag",
    start_date=datetime(2022,7,1),
    schedule_interval="@hourly",
    catchup=False,
    tags=['EKG', 'jimtyhurst'],
) as dag:

    t1 = PythonOperator(
        task_id="hello_python",
        python_callable=hello_airflow
    )

t1

