import asyncio
import functools
import json
import jwt
import websockets


def forward(func):
    @functools.wraps(func)
    async def new_func(self, *args, **kwargs):
        if not self.call_ws:
            self.call_ws = await websockets.connect(self.url + '/call', extra_headers=self._get_headers())

        call = {
            'method': func.__name__,
            'args': args,
            'kwargs': kwargs
        }
        await self.call_ws.send(json.dumps(call))
        response = await self.call_ws.recv()
        response_data = json.loads(response)
        if 'error' in response:
            raise Exception(response_data['error'])
        else:
            return response_data['result']

    return new_func


class Client:
    def __init__(self, url, secret):
        self.url = url
        self.call_ws = None
        self.secret = secret

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.call_ws:
            await self.call_ws.close()

    def _get_headers(self):
        payload = {}
        token = jwt.encode(payload, self.secret, algorithm='HS256')
        return {
            'Authorization': f'Bearer {token}'
        }

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
        async with websockets.connect(f'{self.url}/stream', extra_headers=self._get_headers()) as ws:
            first_message = {
                'wid': workflow_id,
                'identity': identity,
            }
            await ws.send(json.dumps(first_message))
            async for message in ws:
                yield json.loads(message)

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
