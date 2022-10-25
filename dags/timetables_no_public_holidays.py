import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.decorators import task

from no_holidays import NoPublicHolidaysTimetable


default_args = {
    'owner': 'cs',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=15),
}


with DAG(
        dag_id='timetables_no_public_holidays',
        start_date=pendulum.datetime(2022, 12, 1, tz='UTC'),
        schedule=NoPublicHolidaysTimetable(),
        default_args=default_args,
        max_active_runs=7,
        catchup=True,
        tags=['timetables'],
        description='''
           This DAG demonstrates timetables. https://www.astronomer.io/guides/scheduling-in-airflow/#timetables
        ''',
):

    @task(task_id='print_the_context')
    def print_context(ds=None):
        print(ds)
        return 'Today is not a public holiday.'

    run_this = print_context()
