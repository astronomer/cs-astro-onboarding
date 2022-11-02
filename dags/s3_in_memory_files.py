import io
import pandas as pd
import pendulum
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


S3_KEY = "in_memory_to_s3/test.csv.gz"
S3_BUCKET = "astro-onboarding"

def upload_to_s3(key, bucket_name):
    s3 = S3Hook()

    # create a dataframe for testing purposes
    data = [['tom', 10], ['nick', 15], ['juli', 14]]
    df = pd.DataFrame(data, columns=['Name', 'Age'])

    with io.BytesIO() as in_mem_file:
        df.to_csv(in_mem_file, compression='gzip')
        in_mem_file.seek(0)
        s3._upload_file_obj(
            file_obj=in_mem_file,
            key=key,
            bucket_name=bucket_name,
            replace=True
        )

def download_from_s3(key, bucket_name):
    s3 = S3Hook()
    try:
        s3_obj = s3.get_key(
            key,
            bucket_name
        )
    except ClientError as e:
        if e.response.get('Error', {}).get('Code') == 404:
            raise AirflowException(
                f'The source file in Bucket {bucket_name} with path {key} does not exist'
            )
        else:
            raise e

    df = pd.read_csv(s3_obj.get()['Body'], compression='gzip', header=0, sep=',')

    print(df.head())

default_args = {
    'owner': 'cs',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
    dag_id="s3_in_memory_files",
    start_date=pendulum.datetime(2021, 12, 1, tz='UTC'),
    schedule=None,
    default_args=default_args,
    description="This DAG demonstrates how to upload & download S3 objects in memory.",
    catchup=False,
    tags=["aws s3"]
):

    start, finish = [EmptyOperator(task_id=tid) for tid in ["start", "finish"]]

    upload = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        op_kwargs={
            "key": S3_KEY,
            "bucket_name": S3_BUCKET
        }
    )

    download = PythonOperator(
        task_id="download_from_s3",
        python_callable=download_from_s3,
        op_kwargs={
            "key": S3_KEY,
            "bucket_name": S3_BUCKET
        }
    )


    start >> upload >> download >> finish


