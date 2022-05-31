import random
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator


default_args = {
        'owner': 'cs',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    }


with DAG(dag_id='dynamic_task_mapping_mixed_operators',
         start_date=datetime(2022, 5, 1),
         schedule_interval='0 6 * * Mon',  # At 06:00 on Monday
         max_active_runs=3,
         default_args=default_args,
         tags=['dynamic tak mapping', 'XComs'],
         description='''
             This DAG demonstrates dynamic task mapping based on the result of the upstream task, 
             and combining both TaskFlow API and non-TaskFlow API operators.
         ''',
         ) as dag:

    @task
    def get_files():
        return [f"file_{nb}" for nb in range(random.randint(2, 4))]


    @task
    def download_file(folder: str, file: str):
        return f"{folder}/{file}"

    # What if you just want to collect the output of your mapped tasks into a classic operator
    # instead of using expand again?
    # With the Jinja Template Engine, you can leverage the filter list for this.
    # Note the " | list " at the end of the bash command:
    print_files = BashOperator(
        task_id='print_files',
        bash_command="echo '{{ ti.xcom_pull(task_ids='download_file', \
        dag_id='dynamic_task_mapping_mixed_operators', key='return_value') | list }}'"
    )

    download_file.partial(folder='/usr/local').expand(file=get_files()) >> print_files
