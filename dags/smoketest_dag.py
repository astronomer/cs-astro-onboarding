'''
## Smoketest DAG
This DAG is used to test connections to external services
___
## Steps to Use
1. Create connections for external services (i.e. `snowflake_default`) see connection documentation links below
to learn more about how to set up different connection types
2. Unpause the DAG and test connections that you need.

Services included:
- [databricks](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/connections/databricks.html)
- [salesforce](https://airflow.apache.org/docs/apache-airflow-providers-salesforce/stable/connections/salesforce.html)
- [snowflake](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html)
- [postgres](https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/connections/postgres.html)
'''

import logging
import sys

from airflow import DAG
from airflow.decorators import task

from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.salesforce.hooks.salesforce import SalesforceHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from datetime import datetime
from include.smoketest_dag.utils import get_conns_by_conn_type

with DAG(
    dag_id="smoketest_dag",
    start_date=datetime(2022, 11, 30),
    schedule=None,
    template_searchpath="/usr/local/airflow/include/smoketest_dag/",
    doc_md=__doc__,
    tags=["snowflake", "databricks", "salesforce", "postgres"],
):

    ### Snowflake
    SnowflakeOperator.partial(
        task_id='test_snowflake_connection',
        sql='SELECT 1;',
    ).expand(
        snowflake_conn_id=get_conns_by_conn_type(conn_type='snowflake')
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

    test_databricks_connection.expand(conn_id=get_conns_by_conn_type(conn_type='databricks'))

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

    test_salesforce_connection.expand(conn_id=get_conns_by_conn_type(conn_type='salesforce'))

    ### Postgres
    PostgresOperator.partial(
        task_id='test_posgres_connection',
        sql='SELECT 1;'
    ).expand(
        postgres_conn_id=get_conns_by_conn_type(conn_type='postgres')
    )
