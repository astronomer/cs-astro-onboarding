from airflow.models import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from datetime import datetime

'''clusters'''
from airflow.providers.amazon.aws.operators.redshift_cluster import RedshiftPauseClusterOperator
from airflow.providers.amazon.aws.operators.redshift_cluster import RedshiftResumeClusterOperator
from airflow.providers.amazon.aws.sensors.redshift_cluster import RedshiftClusterSensor

'''sql'''
# from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from include.redshift_custom import RedshiftSQLOperator #temporary until we get bug 22391 fixed

'''transfers'''
from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

'''
these variables are here as top-level code for this demonstration which is against Airflow Best Practices
'''
cluster_identifier = 'astronomer-success-redshift'
s3_bucket = 'astro-onboarding'

# Add test comment to test code promotion process.

with DAG(
    dag_id="redshift_example_dag",
    schedule_interval=None,
    start_date=datetime(2022, 6, 13),
    max_active_runs=1,
    template_searchpath='/usr/local/airflow/include/redshift_example_dag',
    tags=["aws redshift"],
    catchup=True,
    default_args={
        "owner": "cs"
    },
) as dag:

    start, finish = [EmptyOperator(task_id=tid) for tid in ['start', 'finish']]

    '''
    resuming and pausing clusters requires the following permissions on aws
    - redshift:DescribeClusters
    - redshift:PauseCluster
    - redshift:ResumeCluster
    '''
    with TaskGroup(group_id="cluster_start") as cluster_start:
        resume = RedshiftResumeClusterOperator(
            task_id='resume_redshift',
            cluster_identifier=cluster_identifier
        )

        wait = RedshiftClusterSensor(
            task_id='wait_for_cluster',
            cluster_identifier=cluster_identifier,
            target_status='available'
        )

        resume >> wait

    create_table_schemas = RedshiftSQLOperator(
        task_id='create_table_schemas',
        sql='/sql/create_table_schemas.sql'
    )

    '''
    ensure your aws_default connection has read/write access to the s3 bucket that you are referencing
    '''
    files = [
        {"table": "users", "s3_key": "allusers_pipe.txt", "delimiter": "'|'", "timeformat": "YYYY-MM-DD HH:MI:SS"},
        {"table": "venue", "s3_key": "venue_pipe.txt", "delimiter": "'|'", "timeformat": "YYYY-MM-DD HH:MI:SS"},
        {"table": "category", "s3_key": "category_pipe.txt", "delimiter": "'|'", "timeformat": "YYYY-MM-DD HH:MI:SS"},
        {"table": "date", "s3_key": "date2008_pipe.txt", "delimiter": "'|'", "timeformat": "YYYY-MM-DD HH:MI:SS"},
        {"table": "event", "s3_key": "allevents_pipe.txt", "delimiter": "'|'", "timeformat": "YYYY-MM-DD HH:MI:SS"},
        {"table": "listing", "s3_key": "listings_pipe.txt", "delimiter": "'|'", "timeformat": "YYYY-MM-DD HH:MI:SS"},
        {"table": "sales", "s3_key": "sales_tab.txt", "delimiter": "'\\t'", "timeformat": "MM/DD/YYYY HH:MI:SS"}
    ]
    with TaskGroup(group_id='s3_to_redshift') as s3_to_redshift:
        for file in files:
            S3ToRedshiftOperator(
                task_id=f"copy_{file['table']}_to_redshift",
                schema="public",
                table=file["table"],
                s3_bucket='astro-onboarding',
                s3_key=f"redshift_example_dag/{file['s3_key']}",
                copy_options=[
                    f"DELIMITER {file['delimiter']}",
                    f"REGION 'us-east-2'",
                    f"TIMEFORMAT '{file['timeformat']}'"
                ],
                method='REPLACE'
            )

    top_ten_buyers_by_quantity = RedshiftSQLOperator(
        task_id='top_ten_buyers_by_quantity',
        sql='/sql/top_ten_buyers_by_quantity.sql'
    )

    top_ten_to_s3 = RedshiftToS3Operator(
        task_id='top_ten_to_s3',
        s3_bucket='astro-onboarding',
        s3_key=f'redshift_example_dag/reporting/top_ten_buyers_by_quantity_{{{{ ds }}}}_',
        schema='public',
        table='top_ten_buyers_by_quantity',
        table_as_file_name=False,
        unload_options=[
            "DELIMITER AS ','",
            "FORMAT AS CSV",
            "ALLOWOVERWRITE",
            "PARALLEL OFF",
            "HEADER"
        ]
    )

    with TaskGroup(group_id="reset_cluster") as reset_cluster:

        drop_all_tables = RedshiftSQLOperator(
            task_id='drop_all_tables',
            sql='/sql/drop_all_tables.sql'
        )

        pause_redshift = RedshiftPauseClusterOperator(
            task_id='pause_redshift',
            cluster_identifier=cluster_identifier,
        )

        wait = RedshiftClusterSensor(
            task_id='wait_for_cluster',
            cluster_identifier=cluster_identifier,
            target_status='paused'
        )

        drop_all_tables >> pause_redshift

    start >> cluster_start >> create_table_schemas >> s3_to_redshift >> top_ten_buyers_by_quantity >> top_ten_to_s3 >> reset_cluster >> finish
