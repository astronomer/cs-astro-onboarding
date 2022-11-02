import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from astronomer.providers.http.sensors.http import HttpSensorAsync
from include.external_deployment.xd_external_task_sensor import ExternalDeploymentTaskSensorWrapper

with DAG(
    dag_id='xdeployment_externaltasksensor',
    start_date=pendulum.datetime(2022, 7, 27, tz='UTC'),
    schedule="0 13-22 * * 1-5",
    template_searchpath="/usr/local/airflow/include/xdeployment_externaltasksensor/",
    tags=["REST API", "Astronomer", "Deferrable Operator"],
    description=f"""
        Checks a seperate astronomer deployment to see if a task has completed
    """,
    catchup=False
):

    start, finish = [EmptyOperator(task_id=tid) for tid in ['start', 'finish']]

    t = ExternalDeploymentTaskSensorWrapper(
        external_dag_id="_04__shared",
        external_task_id="finish"
    )

    start >> t >> finish
