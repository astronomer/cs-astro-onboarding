from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup


default_args = {
        'owner': 'cs',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    }


with DAG(dag_id='task_groups',
         start_date=datetime(2022, 5, 1),
         schedule_interval='30 7 * * Thu',
         default_args=default_args,
         tags=['task groups'],
         description='''
            This DAG demonstrates task groups - a UI grouping concept.
         ''',
         ) as dag:

    groups = []

    for g_id in range(1, 4):
        tg_id = f'group_{g_id}'
        with TaskGroup(group_id=tg_id) as tg1:
            t1 = EmptyOperator(task_id='task1')
            t2 = EmptyOperator(task_id='task2')

            t1 >> t2

            if tg_id == 'group_3':
                t3 = EmptyOperator(task_id='task3')

                t1 >> t3

            groups.append(tg1)

    [groups[0], groups[1]] >> groups[2]
