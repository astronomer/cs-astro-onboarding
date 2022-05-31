from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable


default_args = {
        'owner': 'cs',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    }


with DAG(dag_id='dynamic_task_mapping_known_values',
         start_date=datetime(2022, 5, 1),
         schedule_interval='@once',
         default_args=default_args,
         tags=['dynamic tak mapping', 'aws parameter store', 'secrets backend'],
         description='''
            This DAG demonstrates dynamic task mapping with a constant parameter based on the known values, 
            and retrieving variables from AWS Parameter Store.
         ''',
         ) as dag:

    @task
    def add(x, y):
        return x + y

    CONSTANT_VALUE = int(Variable.get("my_test_variable"))  # Variable comes from the AWS Parameter Store

    # partial(): allows you to pass parameters that remain constant for all tasks
    # expand(): allows you to pass parameters to map over
    added_values = add.partial(x=CONSTANT_VALUE).expand(y=[0, 1, 2])
