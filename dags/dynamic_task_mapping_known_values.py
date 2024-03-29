import pendulum
from datetime import timedelta

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


with DAG(
    dag_id='dynamic_task_mapping_known_values',
    start_date=pendulum.datetime(2022, 5, 1, tz='UTC'),
    schedule='@once',
    default_args=default_args,
    tags=['dynamic task mapping', 'aws parameter store', 'secrets backend'],
    description='''
        This DAG demonstrates dynamic task mapping with a constant parameter based on the known values, 
        and retrieving variables from AWS Parameter Store.
    ''',
):

    @task
    def add(x, y):
        return x + y

    # partial(): allows you to pass parameters that remain constant for all tasks
    # expand(): allows you to pass parameters to map over
    added_values = add.partial(x=int(Variable.get('my_test_variable', 50))).expand(y=[0, 1, 2])
    # Variable comes from the AWS Parameter Store
