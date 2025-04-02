import asyncio
import functools
import json
# TODO: Update websockets to use latest version
import websockets


def forward(func):
    @functools.wraps(func)
    async def new_func(self, *args, **kwargs):
        if not self._call_ws:
            self._call_ws = await websockets.connect(self._url + '/call', extra_headers=self._headers)
        call = {
            'method': func.__name__,
            'args': args,
            'kwargs': kwargs
        }
        await self._call_ws.send(json.dumps(call))
        response = await self._call_ws.recv()
        response_data = json.loads(response)
        # TODO: Deserialize the error.
        if 'error' in response:
            raise Exception(response_data['error'])
        else:
            return response_data['result']

    return new_func


class Client:
    def __init__(self, url, headers: dict[str, str]):
        self._url = url
        self._call_ws = None
        self._headers = headers

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._call_ws:
            await self._call_ws.close()

    def _deserialize_resources(self, message):
        raw_resources = json.loads(message)
        modified_resources = {}
        for key, value in raw_resources.items():
            key0, key1 = key.split('||')
            if key1 == '':
                key1 = None
            tuple_key = (key0, key1)
            modified_resources[tuple_key] = value
        return modified_resources

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

    async def stream_resources(self, workflow_id: str, identity: str):
        async with websockets.connect(f'{self._url}/stream', extra_headers=self._headers) as ws:
            first_message = {
                'wid': workflow_id,
                'identity': identity,
            }
            await ws.send(json.dumps(first_message))
            async for message in ws:
                yield self._deserialize_resources(message)

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
