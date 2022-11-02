docs = """
Notes
- This example dag is using all known operators and hook methods for Databricks as of 11/1/21
- DatabricksSubmitRunOperator = DatabricksHook.submit_run & DatabricksRunNowOperator = DatabricksHook.run_now 
  (essentially) see next bullet point for a key difference
- The DatabricksSubmitRunOperator gets marked as success once the actual job on Databricks succeeds while the hook 
  method submit_run gets marked as success once the job is kicked off (not finished in Databricks)
- To use this demo, you'll need to start a trial Databricks on Azure Portal. And create a hello world notebook that has
  one simple cell: print("Hello World") (you could also add time.sleep(60) if you wanted to more easily demonstrate the
  cancel_run method on the hook)
- The Operators and Hook default to the databricks_default conn_id in the Airflow UI, so you'll need to add that as well
  params are as follows:
    -  conn_id: databricks_default
    -  connection_type: Databricks
    -  extra: {"host": "https://adb-xxxx.azuredatabricks.net/", "token": "<databricks_token>"}
    (on extra replace the xxxx in host with the url in your databricks instance and generate a token through the 
    databricks UI under Settings >> User Settings
"""

import pendulum

from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from astronomer.providers.databricks.operators.databricks import (
    DatabricksRunNowOperatorAsync,
    DatabricksSubmitRunOperatorAsync
)

from include.databricks_tools import DatabricksUtil


new_cluster = {
    "num_workers": 0,
    "spark_version": "10.4.x-scala2.12",
    "spark_conf": {
        "spark.master": "local[*, 4]",
        "spark.databricks.cluster.profile": "singleNode"
    },
    "azure_attributes": {
        "first_on_demand": 1,
        "availability": "ON_DEMAND_AZURE",
        "spot_bid_max_price": -1
    },
    "node_type_id": "Standard_F4",
    "driver_node_type_id": "Standard_F4",
    "ssh_public_keys": [],
    "custom_tags": {
        "ResourceClass": "SingleNode"
    },
    "spark_env_vars": {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
    },
    "enable_elastic_disk": True,
    "cluster_source": "UI",
    "init_scripts": [],
    "runtime_engine": "STANDARD"
}


# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG(
    dag_id="databricks_example_dag",
    start_date=pendulum.datetime(2022, 6, 1, tz='UTC'),
    default_args={
        "owner": "cs"
    },
    max_active_runs=3,
    schedule=None,
    catchup=False,
    tags=["databricks"],
    doc_md=docs,
):

    # starts a terminated cluster
    python_start_cluster = PythonOperator(
        task_id="python_start_cluster",
        python_callable=DatabricksUtil().start_cluster,
        op_kwargs={
            "cluster_id": "{{var.value.databricks_cluster_id}}"
        }
    )

    start, finish = [EmptyOperator(task_id=tid) for tid in ["start", "finish"]]

    with TaskGroup("dependencies") as g4:
        # restarts a cluster
        python_restart_cluster = PythonOperator(
            task_id="python_restart_cluster",
            python_callable=DatabricksUtil().restart_cluster,
            op_kwargs={
                "cluster_id": "{{var.value.databricks_cluster_id}}"
            }
        )

        # install package dependencies on a cluster
        python_install = PythonOperator(
            task_id="python_install",
            python_callable=DatabricksUtil().install,
            op_kwargs={
                "json": {
                    "cluster_id": "{{var.value.databricks_cluster_id}}",
                    "libraries": [{
                        "pypi": {
                            "package": "requests"
                        }
                    }]
                }
            }
        )

        # #uninstall package dependencies on a cluster
        # python_uninstall = PythonOperator(
        #     task_id="python_uninstall",
        #     python_callable=DatabricksUtil().uninstall,
        #     op_kwargs={
        #         "json": {
        #             "cluster_id": "{{var.value.databricks_cluster_id}}",
        #             "libraries": [{
        #                     "pypi": {
        #                         "package": "requests"
        #                     }
        #                 }]
        #         }
        #     }
        # )

        python_install >> python_restart_cluster

    with TaskGroup(group_id="jobs") as g1:
        # runs a job that is already assigned to an existing all purpose cluster
        opr_run_now = DatabricksRunNowOperatorAsync(
            task_id="databricks_run_now",
            job_id="{{ var.value.databricks_job_id }}",
        )

        #spins up a new cluster and runs a job - to see the cluster look at the "Job Clusters" tab in Databricks UI
        opr_submit_run = DatabricksSubmitRunOperatorAsync(
            task_id='databricks_submit_run',
            json={
                'new_cluster': new_cluster,
                'notebook_task': {
                    'notebook_path': "{{ var.value.databricks_notebook_path }}"
                }
            }
        )

    with TaskGroup(group_id="job_interaction") as g3:
        #returns url for the specified run given the run_id (see xcom)
        python_get_run_page_url = PythonOperator(
            task_id="python_get_run_page_url",
            python_callable=DatabricksUtil().get_run_page_url,
            op_kwargs={
                "run_id": "{{ var.value.databricks_run_id }}"
            }
        )

        #returns job_id given a run_id (see xcom)
        python_get_job_id = PythonOperator(
            task_id="python_get_job_id",
            python_callable=DatabricksUtil().get_job_id,
            op_kwargs={
                "run_id": "{{ var.value.databricks_run_id }}"
            }
        )

        #returns the state of a job run given the run id
        #Please note, to use this task, you must set the following environment variable:
        #   AIRFLOW__CORE__ENABLE_XCOM_PICKLING=TRUE
        python_get_run_state = PythonOperator(
            task_id="python_get_run_state",
            python_callable=DatabricksUtil().get_run_state,
            op_kwargs={
                "run_id": "{{ var.value.databricks_run_id }}"
            }
        )

        # #cancels a specified run for a job given a run id
        # python_cancel_run = PythonOperator(
        #     task_id="python_cancel_run",
        #     python_callable=DatabricksUtil().cancel_run,
        #     op_kwargs={
        #         "run_id": str(run_id)
        #     }
        # )

    python_terminate_cluster = PythonOperator(
        task_id="python_terminate_cluster",
        python_callable=DatabricksUtil().terminate_cluster,
        op_kwargs={
            "cluster_id": "{{var.value.databricks_cluster_id}}"
        }
    )

    python_start_cluster >> start >> g4 >> g1 >> g3 >> finish >> python_terminate_cluster
