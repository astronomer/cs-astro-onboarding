import json
import requests

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.models.variable import Variable
from airflow.utils.context import Context
from astronomer.providers.http.sensors.http import HttpSensorAsync
from airflow.utils.context import Context

from typing import Any, Dict, Iterable, Optional

from include.external_deployment.http import CustomHttpTrigger

def ExternalDeploymentTaskSensorWrapper(external_dag_id, external_task_id, http_conn_id='astronomer_default'):
    '''
    Upstream DAG in separate deployment needs to have the SAME schedule as the dag this task runs in or it wont work.
    Astronomer Default connection should be set up like:
    - Host: https://companyname.astronomer.run/<some-str>
    - Extra: {"key_id": "<your-key-id>", "key_secret": "your-key-secret"}
    '''
    return ExternalDeploymentTaskSensor(
        task_id=f"wait_for_{external_dag_id}",
        method="GET",
        endpoint=f"/api/v1/dags/{external_dag_id}/dagRuns/{{{{ run_id }}}}/taskInstances/{external_task_id}",
        headers={
          "cache-control": "no-cache",
          "content-type": "application/json",
          "accept": "application/json"
        },
        http_conn_id=http_conn_id
    )

class ExternalDeploymentTaskSensor(HttpSensorAsync):
    '''
    If the DAGs are on the same schedule, then this will return a deferrable operator checking if upstream task in an
    upstream deployment has returned a success state
    '''

    def __init__(
        self,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)

    def _get_conn(self):
        conn = BaseHook.get_connection(conn_id=self.http_conn_id)
        extras = conn.extra_dejson
        headers = {
            "content-type": "application/json"
        }
        data = json.dumps({
            "client_id": extras["key_id"],
            "client_secret": extras["key_secret"],
            "audience": "astronomer-ee",
            "grant_type": "client_credentials"
        })
        #TODO: token is masked in the triggerer logs
        r = requests.post(url=f"https://auth.astronomer.io/oauth/token", data=data, headers=headers).json()
        return {"host": conn.host, "token": r["access_token"]}

    def execute(self, context: Context) -> None:
        conn = self._get_conn()
        authorized_headers = self.headers
        authorized_headers['authorization'] = f"Bearer {conn['token']}"
        self.defer(
            timeout=self.execution_timeout,
            trigger=CustomHttpTrigger(
                method=self.hook.method,
                endpoint=self.endpoint,
                data=self.request_params,
                headers=authorized_headers,
                http_conn_id=self.http_conn_id,
                extra_options=self.extra_options,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Dict[str, Any], event: Optional[Dict[Any, Any]] = None) -> None:
        if event:
            self.log.info("Task succeeded. Response: %s", event)
            return True
        else:
            self.log.info("Task did not succeed. Response: %s", event)
        raise ValueError(f"Response: {event}")
