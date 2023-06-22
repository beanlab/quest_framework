import logging
import uuid
from datetime import datetime
from enum import Enum
from functools import wraps
from typing import Any, Protocol, Optional, Callable, TypedDict
from .events import UniqueEvent, EventManager
from dataclasses import dataclass

ARGUMENTS = "INITIAL_ARGUMENTS"
KW_ARGUMENTS = "INITIAL_KW_ARGUMENTS"


class WorkflowSuspended(BaseException):
    pass


class WorkflowFunction(Protocol):
    def __init__(self, workflow_manager, *args, **kwargs): ...

    def __call__(self, *args, **kwargs) -> Any: ...


class Status(Enum):
    RUNNING = 1
    SUSPENDED = 2
    COMPLETED = 3
    ERRORED = 4


@dataclass
class WorkflowStatus:
    status: Status
    started: Optional[datetime]
    ended: Optional[datetime]
    steps: dict[str, 'StepEntry']
    state: dict[str, 'StateEntry']
    queues: dict[str, 'QueueEntry']

    def get_result(self):
        return self.state['result']

    def get_error(self):
        return self.state['error']


def _step(func):
    @wraps(func)
    async def new_func(self, *args, **kwargs):
        args = (self,) + args
        return await self.handle_step(func.__name__, func, *args, **kwargs)

    return new_func


def alambda(value):
    async def run():
        return value

    return run


class StepEntry(TypedDict):
    step_id: str
    name: str
    value: Any
    identity: Optional[str]


class StateEntry(TypedDict):
    state_id: str
    name: str
    value: Any
    identity: Optional[str]


class QueueEntry(TypedDict):
    queue_id: str
    name: str
    values: list
    identity: Optional[str]


def get_current_timestamp() -> datetime:
    return datetime.utcnow()


def assign_identity():
    return str(uuid.uuid4())


def filter_identity(identity, dict_to_filter: EventManager) -> dict:
    ret_dict = {}
    for key, value in dict_to_filter.items():
        if value['identity'] is None or value['identity'] == identity:
            ret_dict[key] = value
    return ret_dict


class Workflow:
    """
    Function decorator
    """

    def __init__(
            self,
            workflow_id: str,
            func: WorkflowFunction,
            step_manager: EventManager,
            state_manager: EventManager,
            queue_manager: EventManager
    ):
        self.workflow_id = workflow_id
        self._replay_events: list[UniqueEvent] = []
        self._func = func
        self._prefix = []
        self.started = get_current_timestamp()
        self.ended = None
        self.status = Status.RUNNING
        self.steps: EventManager[StepEntry] = step_manager
        self.state: EventManager[StateEntry] = state_manager
        self.queues: EventManager[QueueEntry] = queue_manager
        self.unique_ids: dict[str, UniqueEvent] = {}

    def get_status(self, identity, include_steps, include_state, include_queues):
        steps = filter_identity(identity, self.steps) if include_steps else None
        state = filter_identity(identity, self.state) if include_state else None
        queues = filter_identity(identity, self.queues) if include_queues else None
        return WorkflowStatus(
            self.status,
            self.started,
            self.ended,
            steps,
            state,
            queues
        )

    def _get_unique_id(self, event_name: str) -> str:
        prefixed_name = self._get_prefixed_name(event_name)
        if prefixed_name not in self.unique_ids.keys():
            self.unique_ids[prefixed_name] = UniqueEvent(prefixed_name)
            self._replay_events.append(self.unique_ids[prefixed_name])
        return next(self.unique_ids[prefixed_name])

    async def handle_step(self, step_name: str, func: Callable, *args, **kwargs):
        """This is called by the @step decorator"""
        step_id = self._get_unique_id(step_name)

        if step_id in self.steps:
            return self.steps[step_id]['value']
        else:
            self._prefix.append(step_name)
            payload = await func(*args, **kwargs)
            self._prefix.pop(-1)
            self.steps[step_id] = StepEntry(
                step_id=step_id,
                name=step_name,
                value=payload,
                identity=kwargs.get('identity')
            )
            return payload

    @_step
    async def create_state(self, name, initial_value, identity):
        state_id = self._get_unique_id(name)
        self.state[state_id] = StateEntry(
            state_id=state_id,
            name=name,
            value=initial_value,
            identity=identity
        )
        return state_id

    @_step
    async def remove_state(self, state_id):
        del self.state[state_id]

    async def get_state(self, state_id, identity):
        # Called by workflow manager, readonly: doesn't need to be a step
        if (state_identity := self.state[state_id]['identity']) is not None and state_identity != identity:
            raise Exception('Boo')  # TODO: real InvalidIdentity exception, pull out method?

        return self.state[state_id]['value']

    @_step
    async def set_state(self, state_id, value):
        self.state[state_id]['value'] = value

    @_step
    async def create_queue(self, name, identity):
        queue_id = self._get_unique_id(name)
        self.queues[queue_id] = QueueEntry(
            queue_id=queue_id,
            name=name,
            values=[],
            identity=identity,
        )
        return queue_id

    @_step
    async def push_queue(self, queue_id, value, identity) -> None | str:
        # Called by Workflow Manager
        if (queue_identity := self.queues[queue_id]['identity']) is not None and queue_identity != identity:
            raise Exception('Boo')  # TODO: real InvalidIdentity exception

        identity = queue_identity or assign_identity()
        self.queues[queue_id]['values'].append((identity, value))
        await self.start()
        return identity

    @_step
    async def pop_queue(self, queue_id):
        if self.queues[queue_id]['values']:
            return self.queues[queue_id]['values'].pop(0)
        else:
            raise WorkflowSuspended()

    @_step
    async def check_queue(self, queue_id) -> bool:
        return bool(self.queues[queue_id]['values'])

    @_step
    async def remove_queue(self, queue_id):
        del self.queues[queue_id]

    def _workflow_type(self) -> str:
        return self._func.__class__.__name__

    def _get_prefixed_name(self, event_name: str) -> str:
        return '.'.join(self._prefix) + '.' + event_name

    def _reset(self):
        self._prefix = []
        for ue in self._replay_events:
            ue.reset()

    async def start(self, *args, **kwargs) -> WorkflowStatus:
        self._reset()
        try:
            logging.debug('Invoking workflow')

            args = await self.handle_step(ARGUMENTS, alambda(args))
            kwargs = await self.handle_step(KW_ARGUMENTS, alambda(kwargs))
            result = await self._func(*args, **kwargs)
            self.ended = get_current_timestamp()
            logging.debug('Workflow invocation complete')
            self.status = Status.COMPLETED
            await self.create_state('result', result, None)

        except WorkflowSuspended as ws:
            self.status = Status.SUSPENDED

        except Exception as e:
            logging.debug(f'Workflow Errored: {e}')
            self.status = Status.ERRORED
            await self.create_state('error', {'name': str(e), 'details': e.args}, None)
            self.ended = get_current_timestamp()

        return self.get_status(None, True, True, True)


if __name__ == '__main__':
    pass
