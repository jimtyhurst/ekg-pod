from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

GREETING_KEY = 'greeting'
NAME_KEY = 'name'

DEFAULT_GREETING = 'Hello'
DEFAULT_NAME = 'Airflow'

def greet(**kwargs):
    greeting = kwargs.get(GREETING_KEY, DEFAULT_GREETING)
    recipient = kwargs.get(NAME_KEY, DEFAULT_NAME)
    print(f'{greeting}, {recipient}! (DAG with Python task)')

with DAG(
    dag_id="ty_hello_python_dag",
    start_date=datetime(2022,7,12),
    schedule_interval="@hourly",
    catchup=False,
    tags=['EKG', 'jimtyhurst'],
) as dag:

    t1 = PythonOperator(
        task_id="hello_python",
        python_callable=greet,
        op_kwargs={NAME_KEY: 'Nadeesh'}
    )

    # No kwargs
    t2 = PythonOperator(
        task_id="conversational_python",
        python_callable=greet
    )

    t3 = PythonOperator(
        task_id="goodbye_python",
        python_callable=greet,
        op_kwargs={GREETING_KEY: 'Goodbye', NAME_KEY: 'Nadeesh'}
    )

    t1 >> t2 >> t3

