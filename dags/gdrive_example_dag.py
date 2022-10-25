import json
import pendulum
import re
from datetime import timedelta

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
# from airflow.providers.google.suite.hooks.drive import GoogleDriveHook


def list_directory_contents(folder_id, ti, gcp_conn_id="google_cloud_default"):
    # I don't think you can pass scopes to this hook, so I just stole the auth process from it
    # drive = GoogleDriveHook()
    # service = drive.get_conn()

    # create authenticated service
    scopes = ['https://www.googleapis.com/auth/drive']
    extras = BaseHook.get_connection(conn_id=gcp_conn_id).extra_dejson
    long_f = 'extra__google_cloud_platform__keyfile_dict'
    if long_f in extras:
        keyfile_dict = extras[long_f]
    else:
        keyfile_dict = None
    keyfile_dict = json.loads(keyfile_dict)
    keyfile_dict['private_key'] = keyfile_dict['private_key'].replace('\\n', '\n')
    creds = Credentials.from_service_account_info(keyfile_dict, scopes=scopes)
    service = build('drive', 'v3', credentials=creds)

    #get folder name for xcom
    file = service.files().get(fileId=folder_id).execute()
    folder_name = re.sub('[^A-Za-z0-9]+', '', file['name']).lower() #<-replace any special characters so the xcom doesn't get confused

    #use service to query parent object for list of items
    results = service.files().list(
        q=f"'{folder_id}' in parents", # <- this is the query
        pageSize=10,
        fields="nextPageToken, files(id, name)"
    ).execute()
    items = results.get('files', [])

    ti.xcom_push(key=f'items_in_{folder_name}', value=items)


default_args = {
    'owner': 'cs',
    'depends_on_past': False
}


with DAG(
    dag_id='gdrive_example_dag',
    start_date=pendulum.datetime(2022, 5, 1, tz='UTC'),
    schedule=None,
    max_active_runs=3,
    default_args=default_args,
    tags=['gcp', 'google drive'],
    description='This DAG demonstrates the usage of the GoogleDriveHook.',
):

    start, finish = [EmptyOperator(task_id=tid) for tid in ['start', 'finish']]

    t = PythonOperator(
        task_id='list_directory_contents',
        python_callable=list_directory_contents,
        op_kwargs={
            'folder_id': '1o4l7lmCTeIOXRhWItK1mmrbz5WgGiOYq'
        }
    )

    start >> t >> finish

