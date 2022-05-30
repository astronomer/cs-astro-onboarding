import random
import time
from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def random_sum():
    r = random.randint(0, 100)
    add_random = 2 + r
    print(add_random)
    time.sleep(15)


with DAG(dag_id='pools',
         start_date=datetime(2022, 5, 1),
         schedule_interval=None,
         default_args={'owner': 'cs'},
         tags=['pools', 'chain'],
         description='''
             This DAG demonstrates pools which allow you to limit parallelism
             for an arbitrary set ot tasks.
         ''',
         ) as dag:

    sum1 = PythonOperator(task_id='sum1', python_callable=random_sum, pool='pool_2')  # pool_2 is set to 1

    sum2 = PythonOperator(task_id='sum2', python_callable=random_sum, pool='pool_2')

    complete = EmptyOperator(
        task_id='complete',
        pool='default_pool',  # If a task is not given a pool, it is assigned to default_pool (128 slots).
    )

    chain([sum1, sum2], complete)  # This is equivalent to [sum1, sum2] >> complete
    # chain() is useful when setting single direction relationships to many operators
    # https://airflow.apache.org/docs/apache-airflow/1.10.6/concepts.html?highlight=branch#relationship-helper
