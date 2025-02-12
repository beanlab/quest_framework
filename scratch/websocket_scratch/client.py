import asyncio
import json
import websockets


class Client:
    def __init__(self, uri):
        self.uri = uri
        self.ws = None

    @classmethod
    async def connect(cls, uri: str):
        instance = cls(uri)
        return instance

    async def __aenter__(self):
        self.ws = await websockets.connect(self.uri)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.ws:
            await self.ws.close()

    async def _make_call(self, method_name: str, args, kwargs):
        call = {
            'method': method_name,
            'args': args,
            'kwargs': kwargs
        }

        await self.ws.send(json.dumps(call))
        response = await self.ws.recv()
        response_data = json.loads(response)
        if 'error' in response_data:
            raise Exception(response_data['error'])
        else:
            return response_data['result']

    async def start_workflow(self, workflow_type: str, workflow_id: str, *workflow_args, **workflow_kwargs):
        method = 'start_workflow'
        args = [workflow_type, workflow_id, *workflow_args]
        kwargs = workflow_kwargs
        return await self._make_call(method, args, kwargs)

    async def start_workflow_background(self, workflow_type: str, workflow_id: str, *workflow_args, **workflow_kwargs):
        method = 'start_workflow_background'
        args = [workflow_type, workflow_id, *workflow_args]
        kwargs = workflow_kwargs
        return await self._make_call(method, args, kwargs)

    async def has_workflow(self, workflow_id: str) -> bool:
        method = 'has_workflow'
        args = [workflow_id]
        kwargs = {}
        return await self._make_call(method, args, kwargs)

    async def get_workflow(self, workflow_id: str) -> asyncio.Task:
        method = 'get_workflow'
        args = [workflow_id]
        kwargs = {}
        return await self._make_call(method, args, kwargs)

    async def suspend_workflow(self, workflow_id: str):
        method = 'suspend_workflow'
        args = [workflow_id]
        kwargs = {}
        return await self._make_call(method, args, kwargs)

    async def get_resources(self, workflow_id: str, identity):
        method = 'get_resources'
        args = [workflow_id, identity]
        kwargs = {}
        return await self._make_call(method, args, kwargs)

    def get_resource_stream(self, workflow_id: str, identity):
        pass

    async def wait_for_completion(self, workflow_id: str, identity):
        method = 'wait_for_completion'
        args = [workflow_id, identity]
        kwargs = {}
        return await self._make_call(method, args, kwargs)

    async def send_event(self, workflow_id: str, name: str, identity, action, *args, **kwargs):
        method = 'send_event'
        args = [workflow_id, name, identity, action, *args]
        kwargs = kwargs
        return await self._make_call(method, args, kwargs)

    async def get_queue(self, workflow_id: str, name: str, identity):
        method = 'get_queue'
        args = [workflow_id, name, identity]
        kwargs = {}
        return await self._make_call(method, args, kwargs)

    async def get_state(self, workflow_id: str, name: str, identity: str | None):
        method = 'get_state'
        args = [workflow_id, name, identity]
        kwargs = {}
        return await self._make_call(method, args, kwargs)

    async def get_event(self, workflow_id: str, name: str, identity: str | None):
        method = 'get_event'
        args = [workflow_id, name, identity]
        kwargs = {}
        return await self._make_call(method, args, kwargs)

    async def get_identity_queue(self, workflow_id: str, name: str, identity: str | None):
        method = 'get_identity_queue'
        args = [workflow_id, name, identity]
        kwargs = {}
        return await self._make_call(method, args, kwargs)
