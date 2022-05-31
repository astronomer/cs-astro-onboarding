from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
        'owner': 'cs',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    }


with DAG(dag_id='external_task_sensor',
         start_date=datetime(2022, 5, 1),
         schedule_interval='30 6 * * Fri',  # At 06:30 on Friday
         max_active_runs=3,
         default_args=default_args,
         tags=['cross dag dependencies'],
         description='''
             This DAG demonstrates the usage of ExternalTaskSensor. The example holds 2 DAGs:
             1. external_task_sensor (this DAG): it waits until a task is completed in the upstream DAG
             1. trigger_controller_dag: the upstream DAG.
             
         ''',
         ) as dag:

    check_task_completion = ExternalTaskSensor(
        task_id="check_task_completion",
        external_dag_id='trigger_controller_dag',
        external_task_id='complete',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode="reschedule",  # If the criteria is not met, the sensor releases its worker slot and reschedules the next check for a later time.
        poke_interval=60 * 30,  # Time in seconds that the sensor waits before checking the condition again.
        timeout=60 * 60 * 4,  # The maximum amount of time in seconds that the sensor should check the condition for.
    )

    success = EmptyOperator(task_id='success')

    check_task_completion >> success
