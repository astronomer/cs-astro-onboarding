'''
## Smoketest DAG
This DAG is used to test connections to external services
___
## Steps to Use
1. Create default connections for external services (i.e. `snowflake_default`) see connection documentation links below
to learn more about default connection id naming
2. Unpause the DAG and test connections that you need.

Services included:
- [databricks](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/connections/databricks.html): conn_id is `databricks_default'
- [salesforce](https://airflow.apache.org/docs/apache-airflow-providers-salesforce/stable/connections/salesforce.html): conn_id is 'salesforce_default'
- [snowflake](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html): conn_id is 'snowflake_default'
'''
import sys
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.salesforce.hooks.salesforce import SalesforceHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from datetime import datetime

with DAG(
    dag_id="smoketest_dag",
    start_date=datetime(2022, 11, 30),
    schedule=None,
    template_searchpath="/usr/local/airflow/include/smoketest_dag/",
    doc_md=__doc__,
    tags=["snowflake", "databricks", "salesforce"],
):

    ### Snowflake
    SnowflakeOperator(
        task_id='test_snowflake_connection',
        sql='SELECT 1;',
        snowflake_conn_id="snowflake_default"
    )

    ### Databricks
    @task()
    def test_databricks_connection(conn_id) -> [bool, str]:
        hook = DatabricksHook(databricks_conn_id=conn_id)
        try:
            hook._do_api_call(endpoint_info=('GET', 'api/2.0/jobs/list'))
            status = True
            message = 'Connection successfully tested'
        except Exception as e:
            status = False
            message = str(e)

        if status is False:
            logging.info(message)
            sys.exit()
        else:
            logging.info(message)
            return status, message

    test_databricks_connection(conn_id="databricks_default")

    ### Salesforce
    @task()
    def test_salesforce_connection(conn_id):
        hook = SalesforceHook(salesforce_conn_id=conn_id)
        try:
            hook.describe_object("Account")
            status = True
            message = "Connection successfully tested"
        except Exception as e:
            status = False
            message = str(e)

        if status is False:
            logging.info(message)
            sys.exit()
        else:
            logging.info(message)
            return status, message

    test_salesforce_connection(conn_id="salesforce_default")