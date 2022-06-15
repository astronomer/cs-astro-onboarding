import time
import sys
from functools import partial

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import include.slack_callbacks as slack_callbacks


from datetime import datetime, timedelta

def task_that_fails():
    time.sleep(5)
    sys.exit()

def task_that_succeeds():
    time.sleep(60)
    print("Done.")

default_args = {
        'owner': 'cs',
        "on_success_callback": partial(
            slack_callbacks.success_callback,
            http_conn_id='slack_demo'
        ),
        "on_failure_callback": partial(
            slack_callbacks.failure_callback,
            http_conn_id='slack_demo'
        ),
        "on_retry_callback": partial(
            slack_callbacks.retry_callback,
            http_conn_id='slack_demo'
        ),
        "sla": timedelta(seconds=10),
    }

with DAG(
    dag_id="slack_callbacks_example_dag",
    start_date=datetime(2022, 6, 13),
    end_date=datetime(2022, 6, 15),
    schedule_interval="@daily",
    default_args=default_args,
    # sla_miss only applies to scheduled DAG runs, it does not work for manually triggered runs
    # If a DAG is running for the first time and sla is missed, sla_miss will not fire on that first run
    sla_miss_callback=partial(
        slack_callbacks.sla_miss_callback,
        http_conn_id="slack_demo",
    ),
    description='''
        This DAG demonstrates how to use various callbacks with slack
    ''',
    catchup=True,
    tags=["slack"]
) as dag:

    start, finish = [EmptyOperator(task_id=tid, on_success_callback=None, trigger_rule="all_done") for tid in ['start', 'finish']]

    success = PythonOperator(
        task_id='long_running_task',
        python_callable=task_that_succeeds,
        sla=timedelta(seconds=30)
    )

    # Task will sleep to showcase sla_miss callback
    bash_sleep = BashOperator(
        task_id="bash_sleep",
        bash_command="sleep 30",
    )

    fail = PythonOperator(
        task_id="task_that_fails",
        python_callable=task_that_fails,
        retries=1,
        retry_delay=timedelta(seconds=30)
    )

    start >> [success, bash_sleep, fail] >> finish




