from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def print_the_message(**context):
    """
    Print the payload "message" passed to the DagRun conf attribute.
    :param context: The execution context
    :type context: dict
    """
    print("Remotely received value of {} for key=message".format(context["dag_run"].conf["message"]))


default_args = {
        'owner': 'cs',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    }


with DAG(dag_id='trigger_target_dag',
         start_date=datetime(2022, 5, 1),
         schedule_interval=None,
         default_args=default_args,
         tags=['cross dag dependencies'],
         description='''
             This DAG demonstrates the usage of TriggerDagRunOperator. The example holds 2 DAGs:
             1. trigger_controller_dag: it hold TriggerDagRunOperator which triggers the 2nd DAG
             2. trigger_target_dag (this DAG): is triggered by the TriggerDagRunOperator in the 1st DAG.
         ''',
         ) as dag:

    print_with_python = PythonOperator(
        task_id="print_with_python",
        python_callable=print_the_message,
    )

    print_with_bash = BashOperator(
        task_id="print_with_bash",
        bash_command='echo "Here is the message: $message"',
        env={'message': '{{ dag_run.conf["message"] if dag_run else "" }}'},
    )

    complete = EmptyOperator(task_id='complete')

    [print_with_python, print_with_bash] >> complete
