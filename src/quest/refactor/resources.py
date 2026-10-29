import asyncio
import uuid
from typing import Any

from .decorators import resource, action, step, State, Action, Resource

# allow users to import State and Action from here also
State = State  # "use" to avoid code cleanup removing the import
Action = Action


@resource
class Queue:
    def __init__(self, identity, name):
        self._identity = identity
        self._name = name
        self._queue = asyncio.Queue()

    @action(external=True)
    async def put(self, item):
        return await self._queue.put(item)

    @action
    async def get(self):
        return await self._queue.get()


@step
async def _generate_id():
    return str(uuid.uuid4())


class IdentityQueue(Resource):
    def __init__(self, identity, name):
        super().__init__(identity, name)
        self._queue = asyncio.Queue()

    @action(external=True)
    async def put(self, item) -> str:
        touch_id = await _generate_id()
        await self._queue.put((touch_id, item))
        return touch_id

    @action
    async def get(self) -> tuple[str, Any]:
        return await self._queue.get()
