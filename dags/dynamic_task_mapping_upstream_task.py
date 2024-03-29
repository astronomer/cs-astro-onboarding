import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


default_args = {
    'owner': 'cs',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='dynamic_task_mapping_upstream_task',
    start_date=pendulum.datetime(2022, 5, 1, tz='UTC'),
    end_date=pendulum.datetime(2022, 5, 5, tz='UTC'),
    schedule='@daily',  # Run once a day at midnight (same as 0 0 * * *)
    max_active_runs=3,
    default_args=default_args,
    tags=['dynamic task mapping', 'aws s3', 'env vars in Astro UI'],
    description='''
        This DAG demonstrates dynamic task mapping based on the result of the upstream task, 
        and storing connections in the Astro UI. 
    ''',
):

    @task
    def get_s3_files(current_prefix):
        s3_hook = S3Hook(aws_conn_id='s3')  # The connection is stored in the Astro UI (with prefix AIRFLOW_CONN_)
        current_files = s3_hook.list_keys(bucket_name='astro-onboarding',
                                          prefix=current_prefix + "/",
                                          start_after_key=current_prefix + "/")
        return [[file] for file in current_files]

    @task
    def print_keys(s3_key):
        print(s3_key)

    # expand(): allows you to pass parameters to map over
    the_keys = print_keys.expand(s3_key=get_s3_files(current_prefix='{{ ds_nodash }}'))
