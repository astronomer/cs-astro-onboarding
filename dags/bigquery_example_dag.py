from datetime import datetime
from airflow.models import DAG

from airflow.operators.dummy import DummyOperator

from airflow.providers.google.cloud.operators.bigquery import (
        BigQueryCheckOperator,
        BigQueryCreateEmptyDatasetOperator,
        BigQueryDeleteDatasetOperator,
        BigQueryDeleteTableOperator,
        BigQueryExecuteQueryOperator,
        BigQueryGetDatasetOperator,
        BigQueryGetDatasetTablesOperator
)

from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator

project_id = "astronomer-success"
dataset_id = "public_data"

with DAG(
    dag_id=f"bigquery_example_dag",
    schedule_interval="@daily",
    start_date=datetime(2016, 12, 31), #data starts 1/1/2014, but just using a week for quick poc
    end_date=datetime(2016, 12, 31),
    max_active_runs=1,
    template_searchpath="/usr/local/airflow/include/bigquery_example_dag/",
    tags=["bigquery"],
    catchup=True
) as dag:

    start, finish = [DummyOperator(task_id=tid) for tid in ['start', 'finish']]

    # Deletes existing Dataset
    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id=f"delete_dataset_{dataset_id}",
        dataset_id=dataset_id,
        delete_contents=True
    )

    # Creates an empty Dataset
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id=f"create_dataset_{dataset_id}",
        project_id=project_id,
        dataset_id=dataset_id,
        location="US"
    )

    # Copies BigQuery public dataset into your GCP
    get_data = BigQueryToBigQueryOperator(
        task_id="get_data",
        source_project_dataset_tables="bigquery-public-data:austin_crime.crime",
        destination_project_dataset_table=f"{project_id}:{dataset_id}.crime"
    )

    # Check for unique values in unique_key column
    check_unique_values = BigQueryCheckOperator(
        task_id='check_unique_values',
        sql='sql/check_unique_values.sql',
        use_legacy_sql=False,
        params={
            "project_id": project_id,
            "dataset_id": dataset_id,
            "table": "crime",
            "unique_col": "unique_key"
        }
    )

    # Creates an empty dataset
    create_fct_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id=f"create_dataset_fct",
        project_id=project_id,
        dataset_id="fct",
        location="US"
    )

    # Execute SQL statements
    create_fct_crime_district = BigQueryExecuteQueryOperator(
        task_id="fct.crime-district",
        sql="sql/crime-district.sql",
        use_legacy_sql=False,
        params={
            "project_id": project_id,
            "dataset_id": "fct",
            "table": "crime-district"
        }
    )

    '''Get metadata for BigQuery objects'''
    # t = BigQueryGetDatasetOperator(
    #     task_id="get_dataset",
    #     dataset_id="fct",
    #     project_id=project_id,
    # )

    # t = BigQueryGetDatasetTablesOperator(
    #     task_id="get_dataset_tables",
    #     dataset_id="jira",
    #     project_id=project_id
    # )

    start >> delete_dataset >> create_dataset >> get_data >> check_unique_values
    check_unique_values >> create_fct_dataset >> create_fct_crime_district >> finish