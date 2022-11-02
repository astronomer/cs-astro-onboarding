import json
import io
import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def upload_json_to_s3(xcom_task_id, bucket_name, key="out", replace=True, **context):
    json_dict = context['ti'].xcom_pull(task_ids=f'{xcom_task_id}')
    s3_hook = S3Hook()

    with io.BytesIO() as in_mem_file:
        in_mem_file.write(json.dumps(json_dict).encode())
        in_mem_file.seek(0)
        s3_hook._upload_file_obj(
            file_obj=in_mem_file,
            key=key,
            bucket_name=bucket_name,
            replace=replace
        )

with DAG(
    dag_id="http_api_example_dag",
    start_date=pendulum.datetime(2022, 6, 14, tz='UTC'),
    max_active_runs=3,
    schedule=None,
    template_searchpath="/usr/local/airflow/include/http_api_example_dag/",
    tags=["REST API", "aws s3", "snowflake", "ELT"],
    description=f"""
        Extracts data from calendarific api to s3 and then from s3 to snowflake as a json. 
        Data is transformed in Snowflake from json to sql table.
    """
):

    start, finish = [EmptyOperator(task_id=tid) for tid in ["start", "finish"]]

    t1 = SimpleHttpOperator(
        # https://calendarific.com/api-documentation
        # Host: https://calendarific.com/api/v2
        task_id="rest_api_call",
        http_conn_id="calendarific_default",
        endpoint="/holidays",
        method="GET",
        data={
            "api_key": "{{ conn.calendarific_default.password }}",
            "country": "US",
            "year": "2019"
        },
        response_filter=lambda response: response.json()["response"]["holidays"]
    )

    t2 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_json_to_s3,
        op_kwargs={
            "xcom_task_id": "rest_api_call",
            "bucket_name": "astro-onboarding",
            "key": "http_api_example_dag/holidays.json",
            "replace": True
        }
    )

    t3 = SnowflakeOperator(
        task_id="copy_full_holidays_to_snowflake",
        sql="sql/calendarific_holidays_2019.sql",
        params={
            "schema_name": "demo",
            "table_name": "calendarific_holidays_2019"
        }
    )

    t4 = SnowflakeOperator(
        task_id="calendarific_holidays_2019_transformed",
        sql="sql/calendarific_holidays_2019_transformed.sql"
    )

    start >> t1 >> t2 >> t3 >> t4 >> finish
