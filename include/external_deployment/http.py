import asyncio
from typing import Any, AsyncIterator, cast, Dict, Optional, Tuple, Union

from airflow.exceptions import AirflowException
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.state import State

from astronomer.providers.http.hooks.http import HttpHookAsync


class CustomHttpTrigger(BaseTrigger):
    """
    A trigger that fires when the request to a URL returns a non-404 status code

    :param endpoint: The relative part of the full url
    :type endpoint: str
    :param http_conn_id: The HTTP Connection ID to run the sensor against
    :type http_conn_id: str
    :param method: The HTTP request method to use
    :type method: str
    :param data: payload to be uploaded or aiohttp parameters
    :type data: dict
    :param headers: The HTTP headers to be added to the GET request
    :type headers: a dictionary of string key/value pairs
    :param extra_options: Additional kwargs to pass when creating a request.
        For example, ``run(json=obj)`` is passed as ``aiohttp.ClientSession().get(json=obj)``
    :type extra_options: dict
    :param poll_interval: Time to sleep using asyncio
    :type poll_interval: float
    """

    def __init__(
        self,
        endpoint: str,
        http_conn_id: str = "http_default",
        method: str = "GET",
        data: Optional[Union[Dict[str, Any], str]] = None,
        headers: Optional[Dict[str, Any]] = None,
        extra_options: Optional[Dict[str, Any]] = None,
        poll_interval: float = 5.0,
    ):
        super().__init__()
        self.endpoint = endpoint
        self.method = method
        self.data = data
        self.headers = headers
        self.extra_options = extra_options or {}
        self.http_conn_id = http_conn_id
        self.poll_interval = poll_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes HttpTrigger arguments and classpath."""
        return (
            # TODO: Use self.__class__ to retrieve the next line
            # self.__class__.name,
            "include.external_deployment.http.CustomHttpTrigger",
            {
                "endpoint": self.endpoint,
                "data": self.data,
                "headers": self.headers,
                "extra_options": self.extra_options,
                "http_conn_id": self.http_conn_id,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """
        """
        hook = self._get_async_hook()
        while True:
            try:
                response = await hook.run(
                    endpoint=self.endpoint,
                    data=self.data,
                    headers=self.headers,
                    extra_options=self.extra_options,
                )
                _response = cast(Dict[str, Any], await response.json())
                if _response["state"] in State.finished:
                    yield TriggerEvent(_response)
                else:
                    await asyncio.sleep(self.poll_interval)
            except AirflowException as exc:
                if str(exc).startswith("404"):
                    await asyncio.sleep(self.poll_interval)

    def _get_async_hook(self) -> HttpHookAsync:
        return HttpHookAsync(
            method=self.method,
            http_conn_id=self.http_conn_id,
        )
