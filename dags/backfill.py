from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
        'owner': 'cs',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    }


with DAG(dag_id='backfill',
         start_date=datetime(2022, 5, 1),
         schedule_interval=None,
         default_args=default_args,
         tags=['backfill', 'REST API'],
         description='''
            This DAG demonstrates backfilling using the stable REST API. 
         ''',
         ) as dag:

    # Method 1: Trigger a backfill using hard-coded dates & dag_id:
    # trigger_backfill = BashOperator(
    #     task_id='trigger_backfill',
    #     bash_command='airflow dags backfill -s 20220501 -e 20220531 branching',
    # )

    # Method 2: Use the REST API to trigger a backfill, passing in start/end dates, dag_id, etc.:
    trigger_backfill = BashOperator(
        task_id='trigger_backfill',
        bash_command="airflow dags backfill \
        -s {{ dag_run.conf['date_start'] }} \
        -e {{ dag_run.conf['date_end'] }} \
        {{ dag_run.conf['dag_id'] }}"
    )

    trigger_backfill
