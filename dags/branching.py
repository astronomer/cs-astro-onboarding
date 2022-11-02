import pendulum
import random
from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.edgemodifier import Label


def sleep_fun():
    import time

    print("Task is sleeping.")
    time.sleep(10)


default_args = {
    'owner': 'cs',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(seconds=5),
}


with DAG(
    dag_id='branching',
    start_date=pendulum.datetime(2022, 5, 1, tz='UTC'),
    schedule='0 7 * * Wed',  # At 07:00 on Wednesday
    max_active_runs=3,
    default_args=default_args,
    tags=['branching', 'sla'],
    description='This DAG demonstrates the usage of the BranchPythonOperator and SLAs.',
):

    start = EmptyOperator(
        task_id='start',
    )

    options = ['branch_a', 'branch_b', 'branch_c', 'branch_d']

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=lambda: random.choice(options),
    )

    start >> branching

    sla_task = PythonOperator(
        task_id='sla_task',
        python_callable=sleep_fun,
        trigger_rule='all_done',  # By default, trigger_rule is set to all_success.
        # Skip caused by the branching operation would cascade down to skip this task as well.
        # all_done: all upstream tasks are done with their execution.
    )

    end = EmptyOperator(
        task_id='end',
        trigger_rule='none_failed_min_one_success',  # All upstream tasks have not failed or upstream_failed,
        # and at least one upstream task has succeeded.
        sla=timedelta(seconds=50),
    )

    for option in options:

        empty_follow = EmptyOperator(
            task_id=option,
        )

        # Label() is used in order to label the dependency edges between different tasks in the Graph view.
        # It can be especially useful for branching,
        # so you can label the conditions under which certain branches might run.
        branching >> Label(option) >> empty_follow >> sla_task >> end
