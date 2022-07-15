from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'ty_first_dag',
    description='Hello, DAG.',
    start_date=datetime(2022, 7, 11, 19, 30),
    tags=['EKG', 'jimtyhurst'],
) as dag:
    t1 = BashOperator(
        task_id='print_greeting',
        bash_command='echo "Hello, DAG!"',
    )

    t2 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    t3 = BashOperator(
        task_id='print_path',
        bash_command='ls -al',
    )

    t4 = BashOperator(
        task_id='print_environment_variables',
        bash_command='env | /bin/grep -e "AIRFLOW"',
    )

    t5 = BashOperator(
        task_id='print_airflow_version',
        bash_command='echo "*** Airflow version ***"; env | grep -e "AIRFLOW_VERSION"; echo "***********************"',
    )

    t6 = BashOperator(
        task_id='print_goodbye',
        bash_command='echo "You finished the DAG successfully!"',
        trigger_rule="all_success"
    )

    t1 >> [t2, t3]
    t3 >> t4
    t2 >> t5
    [t4, t5] >> t6

