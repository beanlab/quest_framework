import asyncio
from dataclasses import dataclass
from typing import Any, Callable, AsyncGenerator

from quest.refactor.core.suspension import raise_suspension

from .ledger import Ledger
from .replay_stream import ReplayStream
from .utils import get_function_name

# Two kinds of actions:
# 1) Ledger-monitored action on state. For when multiple tasks touch the same state.
# 2) Externally-visible actions. A subset of (1). Displayed in get_resources().

# Why not have state be handled by synchronous getters?
# Because we won't know when the state has changed in order to publish another get_resources().
# We need to store the values ourselves so we can monitor their change.


IDENTITY = str | None
NAME = str


def _create_resource_id(name: NAME, identity: IDENTITY) -> str:
    return f'{name}|{identity}' if identity is not None else name


@dataclass
class Action:
    func: Callable
    external: bool


@dataclass
class State:
    value: Any
    external: bool


@dataclass
class Resources:
    state: list[tuple[IDENTITY, NAME, Any]]
    actions: list[tuple[IDENTITY, NAME]]


class ResourceStream:
    def __init__(self, resource_iterator: AsyncGenerator[Resources]):
        self._resource_iterator = resource_iterator

    async def __aenter__(self):
        return self._resource_iterator

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._resource_iterator.aclose()


class ResourceManager:
    def __init__(self, ledger: Ledger, replay_stream: ReplayStream):
        self._state: dict[IDENTITY, dict[NAME, State]] = {}
        self._actions: dict[IDENTITY, dict[NAME, Action]] = {}

        self._ledger = ledger
        self._replay_stream = replay_stream

        self._resource_streams: dict[IDENTITY, set[asyncio.Event]] = {}
        self._fatal_exception = asyncio.Future()
        self._workflow_suspended = False
        self._workflow_complete = False  # TODO - wire this to historian

    #
    # Workflow lifecycle
    #
    def signal_suspension(self):
        # When the workflow suspends, we need to signal suspension on all resource streams
        self._workflow_suspended = True
        self._trigger_last_round_of_resources()

    def signal_workflow_complete(self):
        self._workflow_complete = True
        self._fatal_exception.set_result(None)
        self._trigger_last_round_of_resources()

    def signal_workflow_error(self, exc):
        self._workflow_complete = True
        self._fatal_exception.set_exception(exc)
        self._trigger_last_round_of_resources()

    def _trigger_last_round_of_resources(self):
        for events in self._resource_streams.values():
            for event in events:
                event.set()

    #
    # Resource streaming
    #
    def resource_stream(self, identities: list[IDENTITY]) -> ResourceStream:
        return ResourceStream(self._resource_stream(identities))

    async def _resource_stream(self, identities: list[IDENTITY]) -> AsyncGenerator[Resources, None]:
        # Whenever a resource is changed for an identity,
        #  all events listening to that identity are set.
        # Whenever resources for any identity listened to here are changed,
        #  we issue an updated snapshot of all the resources visible to this stream

        resources_updated = asyncio.Event()
        for ident in identities:
            if ident not in self._resource_streams:
                self._resource_streams[ident] = set()
            self._resource_streams[ident].add(resources_updated)

        try:
            while True:
                await resources_updated.wait()

                if self._workflow_complete:
                    return
                elif self._workflow_suspended:
                    raise_suspension()

                resources_updated.clear()
                yield await self._get_resources(identities)

        finally:
            # `yield` will raise GeneratorExit when .aclose() is called on it.
            # This happens if the consumer of the stream exits the `async for` loop early
            #  (this isn't a feature of for loops-it's a feature of the ResourceStream context).
            # When the loop ends (through GeneratorExit or normally)
            #  we clean up the listeners
            for ident in identities:
                self._resource_streams[ident].remove(resources_updated)

    async def _get_resources(self, identities: list[IDENTITY]) -> Resources:
        """Return a snapshot of external state and actions"""

        # Wait until the replay is complete before returning resources
        # We rely on the replay to build up the resource state,
        #  so calling this before replay is complete will produce incorrect results
        await self._replay_stream.replay_complete()

        # When the workflow fails, it fails this future with the exception
        # By waiting on this failed future, the exception propagates to resource observers
        if self._fatal_exception.done():
            await self._fatal_exception

        state = [
            (ident, name, state)
            for ident in identities
            for name, state, in self._state.get(ident, {}).items()
            if state.external
        ]

        actions = [
            (ident, name)
            for ident in identities
            for name, action in self._actions.get(ident, {}).items()
            if action.external
        ]

        return Resources(state, actions)

    #
    # State and Actions
    #
    async def _with_single_record(self, record_type, task_id, identity, name, value):
        if (next_record := await self._replay_stream.get_next_record(task_id)) is None:
            await self._ledger.add_record(
                task_id,
                record_type,
                _create_resource_id(name, identity),
                value
            )
        else:
            with next_record as record:
                assert record['entry_type'] == record_type, str(record)

    async def set_state(self, task_id, identity: str, name: str, value, external: bool):
        await self._with_single_record('set_state', task_id, identity, name, value)

        if identity not in self._state:
            self._state[identity] = {}
        self._state[identity][name] = State(value, external)

    async def get_state(self, task_id, identity, name):
        await self._with_single_record('get_state', task_id, identity, name, None)

        return self._state[identity][name].value

    async def delete_state(self, task_id, identity: str, name: str):
        await self._with_single_record('del_state', task_id, identity, name, None)
        del self._state[identity][name]

    async def add_action(self, task_id, identity: str, name: str, func: Callable, external: bool):
        # TODO - raise if action already exists? Treat state differently (state currently overrides)
        # Maybe we need to have add_state, set_state separately...?

        await self._with_single_record('add_action', task_id, identity, name, get_function_name(func))

        if identity not in self._actions:
            self._actions[identity] = {}
        self._actions[identity][name] = Action(func, external)

    async def remove_action(self, task_id, identity: str, name: str):
        await self._with_single_record('remove_action', task_id, identity, name, None)

        del self._actions[identity][name]
        if not self._actions[identity]:
            del self._actions[identity]

    async def invoke_action(self, caller_task_id, identity: str, name: str, *args, **kwargs):
        await self._with_single_record('invoke_action', caller_task_id, identity, name, (args, kwargs))
        await self._actions[identity][name].func(*args, **kwargs)

    async def invoke_external_action(self, caller_task_id, identity: str, name: str, *args, **kwargs):
        await self._with_single_record(
            'invoke_external_action', caller_task_id, identity, name, (identity, name, args, kwargs)
        )
        await self._actions[identity][name].func(*args, **kwargs)

    async def external_action_handler(self, external_task_id):
        # In live play, external events come from other tasks
        # During replay, we need to perform these actions again
        # Most events in the ledger are encountered through the natural flow of the code in replay.
        #  In these cases, we simply confirm that the event happening matches what was written in the ledger.
        # However, the external events are not triggered through the workflow code, so we can't rely on workflow
        #  code to invoke the events at the right times.
        # Instead, we use the ledger to identify when external events happened and replay them.
        # So, this method looks through the ledger for records created by `invoke_external_action` to replay them.
        try:
            # quest_logger.debug(f'External event handler {self._get_task_name()} starting')
            while (next_record := await self._replay_stream.get_next_record(external_task_id)) is not None:
                with next_record as record:
                    if record['entry_type'] == 'invoke_external_action':
                        identity, name, args, kwargs = record['data']
                        await self._actions[identity][name].func(*args, **kwargs)

                    else:
                        raise NotImplementedError(record['entry_type'])

            # quest_logger.debug(f'External event handler {self._get_task_name()} completed')
        except Exception:
            # quest_logger.exception('Error in _external_handler')
            raise
