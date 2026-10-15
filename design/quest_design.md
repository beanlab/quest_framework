- `@step` - routes to `historian.run_step`
- `@task` - routes to `historian.start_task`
- `with value('identity', 'name', value) as v:` - `__aenter__`/`__aexit__` -> `historian.add_state`/`historian.remove_state`
- `with action(...)` - `__aenter__`/`__aexit__` -> `historian.add_action`/`historian.remove_action`
- `with namespace('identity', 'name')` - do we need this?

```python
from quest import identity_queue, state, queue

async def workflow():
    with identity_queue(None, 'name') as name_queue:
        identity, name = await name_queue.get()
    
    with state(identity, 'message', f'Hello {name}. Enter 3 ideas. (0/3 provided)') as message,\
         queue(identity, 'ideas') as idea_queue:

        ideas = []
        for _ in range(3):
            ideas.append(await idea_queue.get())
            message.set(f'Hello {name}. Enter 3 ideas. ({len(ideas)}/3 provided)')

```

```python
import asyncio
import random
from quest import task, state, action, step, identity_queue, queue, these


@task
async def play(identity: str, target_score: int):
    score_changed = asyncio.Event()
    score_changed.clear()
    
    score = state(identity, 'score', 0)

    async def increment_score():
        score.set(score.get() + 1)
        score_changed.set()

    with score, action(identity, 'increment', increment_score), \
            state(identity, 'message', 'Increment your score!') as message:
        while True:
            await score_changed.wait()
            score_changed.clear()
            if score.get() >= target_score:
                break
            else:
                message.set('Keep going!')
    
    return score.get()


@step
async def get_target_score():
    return random.randint(3, 6)


async def workflow():
    users = {}
    with identity_queue(None, 'name') as name_queue:
        while len(users) < 3:
            identity, name = await name_queue.get()
            users[identity] = name

    plays = [play(ident, await get_target_score()) for ident in users]
    scores = {identity: score for identity, score in zip(users, asyncio.gather(*plays))}
    final_message = '\n'.join(f'{users[identity]} has {scores[identity]}' for identity in users)
    
    with state(None, 'message', final_message), \
            these({identity: queue(identity, 'confirm') for identity in users}) as confirm_queues:
        for q in confirm_queues.values():
            await q.get()
```

```python
import asyncio
import uuid

from quest import find_historian, step


class IdentityQueue:
    def __init__(self, identity, name, historian):
        self._queue = asyncio.Queue()
        self._historian = historian
        self._identity = identity
        self._name = name

    async def get(self):
        return await self._queue.get()

    @step
    async def get_uuid(self):
        return str(uuid.uuid4())

    async def put(self, item):
        uuid = await self.get_uuid()
        await self._queue.put((uuid, item))
        return uuid

    async def __aenter__(self):
        await self._historian.add_action(self._identity, self._name, self.put)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._historian.remove_action(self._identity, self._name)


def identity_queue(ident, name):
    return IdentityQueue(ident, name, find_historian())


class State:
    def __init__(self, identity, name, value, historian):
        self._identity = identity
        self._name = name
        self._value = value
        self._historian = historian

    def get(self):
        return self._value

    def set(self, value):
        self._value = value

    async def __aenter__(self):
        await self._historian.add_state(self._identity, self._name, self.get)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._historian.remove_state(self._identity, self._name)


def state(ident, name, value):
    return State(ident, name, value, find_historian())

```

```python
class Ledger:
    def __init__(self):
        self._history = []
        self._gates = {}
        # Indicate when all gates have been cleared, i.e. replay is complete
        
    async def get_next_record(self, task_id):
        ...
        # await gates until getting to next task_id record

    async def add_record(self, task_id, record):
        ...
        # add record to ledger

```

Do we include `task_id` in the API, or do we always record the task ID of the caller by looking it up in context?
If you look it up, the caller can't mess it up, but it is more magical and harder to understand.

```python
class Historian:
    """This class embodies how the Ledger is used to create the Quest experience"""
    
    def __init__(self, ledger: Ledger, entrypoint: Callable):
        self._ledger = ledger

    # Versioning of code
    async def when(self): ... # Return the timestamp of the currently held record 

    # Steps
    async def run_step(self, step_func): ...

    # Resources
    async def add_state(self, ident, name, get_func): ...

    async def remove_state(self, ident, name): ...

    async def add_action(self, ident, name, action_func): ...

    async def remove_action(self, ident, name): ...

    # Tasks
    async def start_task(self, task_name, callable): ...

    # Workflow entrypoint
    async def run(self): ...
    
```

Do we split out resource stuff? No - I think it makes sense for the Historian to be **the** class where logic about how to use the ledger is embodied. Historian represents the fundamental quest interface.

Task group stuff might be extractable. We might also extract out the resource inventory. But the historian will control whether calls are made to these classes based on the records it has.

The Ledger stores records and returns them in locked order.

The Historian decides what records are created and interprets records in the context of replay.

The ResourceManager keeps track of which resources exist for which identities. It handles the snapshots and the stream.

The TaskManager handles the TaskGroup and task lifecycle concerns.