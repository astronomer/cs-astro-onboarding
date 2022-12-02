import os
import json
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.utils.session import provide_session
from airflow.models import Connection
from sqlalchemy.orm import Session


@provide_session
@task()
def get_conns_by_conn_type(conn_type: str, session: Session):
    conn_ids = []
    get_connections = session.query(Connection).all()
    local_connections = {conn.conn_id: conn for conn in get_connections}
    for key, value in local_connections.items():
        if value.conn_type == conn_type:
            conn_ids.append(key)
    return conn_ids

@task()
def get_conns_by_conn_type_ssm(uri_prefix: str):
    '''
    :param uri_prefix: beginning of URI string to determine connection type i.e. snowflake://, aws://, etc
    '''

    conn_ids = []

    #parameter store
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv("AWS_DEFAULT_REGION")
    backend_kwargs = json.loads(os.getenv("AIRFLOW__SECRETS__BACKEND_KWARGS"))
    connections_prefix = backend_kwargs["connections_prefix"]


    os.environ["AIRFLOW_CONN_AWS_PARAM_STORE"] = f'' \
        f'aws://{aws_access_key_id}:{aws_secret_access_key}@/?region_name={aws_region}'
    client = AwsGenericHook(aws_conn_id='aws_param_store', client_type='ssm')
    ssm = client.get_client_type()

    parameters = ssm.get_parameters_by_path(
        Path=connections_prefix,
        Recursive=True,
        WithDecryption=True
    )

    for param in parameters['Parameters']:
        name = param['Name']
        if param['Value'].startswith(uri_prefix):
            conn_ids.append(name.replace(f"{connections_prefix}/", ''))
    return conn_ids