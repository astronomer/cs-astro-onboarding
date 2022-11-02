import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.weekday import BranchDayOfWeekOperator


default_args = {
    'owner': 'cs',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
        dag_id='trigger_controller_dag',
        start_date=pendulum.datetime(2022, 5, 1, tz='UTC'),
        schedule='0 8 * * 2,5',  # At 08:00 on Tuesday and Friday
        max_active_runs=3,
        default_args=default_args,
        tags=['cross dag dependencies', 'branching'],
        description='''
            This DAG demonstrates the usage of TriggerDagRunOperator. The example holds 2 DAGs:
            1. trigger_controller_dag (this DAG): it hold TriggerDagRunOperator which triggers the 2nd DAG
            2. trigger_target_dag: is triggered by the TriggerDagRunOperator in the 1st DAG.
        ''',
):

    # BranchDayOfWeekOperator branches into one of two lists of tasks depending on the current day
    branching = BranchDayOfWeekOperator(
        task_id='branching',
        follow_task_ids_if_true='trigger_another_dag',
        follow_task_ids_if_false='skip',
        week_day='Tuesday',
    )

    trigger_another_dag = TriggerDagRunOperator(
        task_id='trigger_another_dag',
        trigger_dag_id='trigger_target_dag',  # Ensure this equals the dag_id of the DAG to trigger
        conf={'message': 'Sending our greetings!'},
    )

    skip = EmptyOperator(task_id='skip')

    complete = EmptyOperator(task_id='complete', trigger_rule='none_failed_min_one_success')

    branching >> [trigger_another_dag, skip] >> complete
