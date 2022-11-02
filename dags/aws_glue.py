docs = '''
    The operators use `aws_default` connection - make sure to add it, see 
    [docs](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/aws.html).
    
    Additionally, you need to create a role with putobject/getobject access to the bucket,
    as well as the glue service role, 
    see docs [here](https://docs.aws.amazon.com/glue/latest/dg/create-an-iam-role.html).
'''

import pendulum
from datetime import timedelta
from os import getenv

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor


GLUE_EXAMPLE_S3_BUCKET = getenv('GLUE_EXAMPLE_S3_BUCKET', 'astro-onboarding')
GLUE_DATABASE_NAME = getenv('GLUE_DATABASE_NAME', 'demo_database')
GLUE_JOB_NAME = 'demo_glue_job'
GLUE_CRAWLER_ROLE = getenv('GLUE_CRAWLER_ROLE', 'glue-demo-full-access')
GLUE_CRAWLER_NAME = 'demo_crawler'
GLUE_CRAWLER_CONFIG = {
    'Name': GLUE_CRAWLER_NAME,
    'Role': GLUE_CRAWLER_ROLE,
    'DatabaseName': GLUE_DATABASE_NAME,
    'Targets': {
        'S3Targets': [
            {
                'Path': f'{GLUE_EXAMPLE_S3_BUCKET}/glue_demo/data/input',
            }
        ]
    },
}

EXAMPLE_CSV = '''
adam,5.5
chris,5.0
frank,4.5
fritz,4.0
magda,3.5
phil,3.0
'''

# Example CSV file and  Spark script to operate on the CSV data.
EXAMPLE_SCRIPT = f'''
from pyspark.context import SparkContext
from awsglue.context import GlueContext

glueContext = GlueContext(SparkContext.getOrCreate())
datasource = glueContext.create_dynamic_frame.from_catalog(
             database='{GLUE_DATABASE_NAME}', 
             table_name='input'
)

print('There are %s items in the table' % datasource.count())

datasource.toDF().write.format('csv').mode("append").save('s3://{GLUE_EXAMPLE_S3_BUCKET}/glue_demo/data/output/')
'''


default_args = {
    'owner': 'cs',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(seconds=15),
}


@task(task_id='setup_upload_artifacts_to_s3')
def upload_artifacts_to_s3():
    """
    Upload example Spark script to be used by the Glue Job
    """
    s3_hook = S3Hook()
    s3_load_kwargs = {"replace": True, "bucket_name": GLUE_EXAMPLE_S3_BUCKET}
    s3_hook.load_string(string_data=EXAMPLE_CSV, key='glue_demo/data/input/input.csv', **s3_load_kwargs)
    s3_hook.load_string(string_data=EXAMPLE_SCRIPT, key='glue_demo/scripts/etl_script.py', **s3_load_kwargs)


with DAG(
    dag_id='aws_glue',
    schedule=None,
    start_date=pendulum.datetime(2022, 6, 28, tz='UTC'),
    default_args=default_args,
    tags=['aws glue', 'aws s3'],
    catchup=False,
    doc_md=docs,
):

    setup_upload_artifacts_to_s3 = upload_artifacts_to_s3()

    crawl_s3 = GlueCrawlerOperator(
        task_id='crawl_s3',
        config=GLUE_CRAWLER_CONFIG,
        wait_for_completion=False,
    )

    wait_for_crawl = GlueCrawlerSensor(
        task_id='wait_for_crawl',
        crawler_name=GLUE_CRAWLER_NAME,
        mode='reschedule',
    )

    submit_glue_job = GlueJobOperator(
        task_id='submit_glue_job',
        job_name=GLUE_JOB_NAME,
        wait_for_completion=False,
        s3_bucket=GLUE_EXAMPLE_S3_BUCKET,
        script_location=f's3://{GLUE_EXAMPLE_S3_BUCKET}/glue_demo/scripts/etl_script.py',
        iam_role_name=GLUE_CRAWLER_ROLE.split('/')[-1],
        concurrent_run_limit=3,
    )

    wait_for_job = GlueJobSensor(
        task_id='wait_for_job',
        job_name=GLUE_JOB_NAME,
        run_id=submit_glue_job.output,
        mode='reschedule',
    )

    copy_file = S3CopyObjectOperator(
        task_id='copy_file',
        source_bucket_key=f's3://{GLUE_EXAMPLE_S3_BUCKET}/glue_demo/data/input/input.csv',
        dest_bucket_key=f's3://{GLUE_EXAMPLE_S3_BUCKET}/glue_demo/data/processed/{{{{ ds_nodash }}}}/input.csv',
    )

    delete_file = S3DeleteObjectsOperator(
        task_id='delete_file',
        bucket=GLUE_EXAMPLE_S3_BUCKET,
        keys='glue_demo/data/input/input.csv',
    )

    setup_upload_artifacts_to_s3 >> crawl_s3 >> wait_for_crawl >> submit_glue_job >> wait_for_job >> copy_file >> delete_file
