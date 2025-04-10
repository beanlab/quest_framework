import asyncio
import functools
import json
from websockets.asyncio.client import connect

from quest.utils import deserialize_exception


def deserialize_response(response):
    data = json.loads(response)
    if 'exception' in data:
        exception = deserialize_exception(data['exception'])
        raise exception
    if 'result' in data:
        return data['result']
    if 'resources' in data:
        return deserialize_resources(data['resources'])


# TODO: Wrap resources on this end to send_event
def deserialize_resources(raw_data):
    resources = {}
    for key, value in raw_data.items():
        key0, key1 = key.split('||')
        if key1 == '':
            key1 = None
        tuple_key = (key0, key1)
        resources[tuple_key] = value
    return resources


def forward(func):
    @functools.wraps(func)
    async def new_func(self, *args, **kwargs):
        if not self._call_ws:
            ws = connect(self._url + '/call', additional_headers=self._headers)
            self._call_ws = await ws.__aenter__()
        call = {
            'method': func.__name__,
            'args': args,
            'kwargs': kwargs
        }
        await self._call_ws.send(json.dumps(call))
        response = await self._call_ws.recv()
        return deserialize_response(response)

    return new_func


class Client:
    def __init__(self, url, headers: dict[str, str] = None):
        self._call_ws = None
        self._url = url
        self._headers = headers

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._call_ws:
            await self._call_ws.__aexit__(exc_type, exc_val, exc_tb)

    @forward
    async def start_workflow(self, workflow_type: str, workflow_id: str, *workflow_args, **workflow_kwargs):
        ...

    @forward
    async def start_workflow_background(self, workflow_type: str, workflow_id: str, *workflow_args, **workflow_kwargs):
        ...

    @forward
    async def has_workflow(self, workflow_id: str) -> bool:
        ...

    @forward
    async def get_workflow(self, workflow_id: str) -> asyncio.Task:
        ...

    @forward
    async def suspend_workflow(self, workflow_id: str):
        ...

    @forward
    async def get_resources(self, workflow_id: str, identity):
        ...

    async def stream_resources(self, workflow_id: str, identity: str | None):
        async with connect(self._url + '/stream', additional_headers=self._headers) as ws:
            first_message = {
                'wid': workflow_id,
                'identity': identity,
            }
            await ws.send(json.dumps(first_message))
            async for message in ws:
                response = deserialize_response(message)
                yield response

    @forward
    async def send_event(self, workflow_id: str, name: str, identity, action, *args, **kwargs):
        ...

    @forward
    async def get_queue(self, workflow_id: str, name: str, identity):
        ...

    @forward
    async def get_state(self, workflow_id: str, name: str, identity: str | None):
        ...

    @forward
    async def get_event(self, workflow_id: str, name: str, identity: str | None):
        ...

    @forward
    async def get_identity_queue(self, workflow_id: str, name: str, identity: str | None):
        ...
