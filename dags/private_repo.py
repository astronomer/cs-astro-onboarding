from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from toolbox.functions import helloworld

with DAG(
    dag_id='run_package_from_private_repo',
    start_date=datetime(2022, 8, 11),
    schedule_interval=None,
    template_searchpath="/usr/local/airflow/include/private_repo/",
    description=f"""
        Runs a PyPi installed function from a private GitHub repo.
    """,
    catchup=False
) as dag:

    start, finish = [EmptyOperator(task_id=tid) for tid in ['start', 'finish']]

    test = PythonOperator(
        task_id='test',
        python_callable=helloworld
    )

    start >> test >> finish