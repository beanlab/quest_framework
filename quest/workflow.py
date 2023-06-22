import inspect
import logging
import uuid
from datetime import datetime
from enum import Enum
from functools import wraps
from typing import Any, Protocol, Optional, TypeVar, Callable, TypedDict
from .events import Event, UniqueEvent, EventManager
from dataclasses import dataclass

ARGUMENTS = "INITIAL_ARGUMENTS"
PREFIXED_ARGUMENTS = ".INITIAL_ARGUMENTS_0"
KW_ARGUMENTS = "INITIAL_KW_ARGUMENTS"
PREFIXED_KW_ARGUMENTS = ".INITIAL_KW_ARGUMENTS_0"
WORKFLOW_RESULT = "WORKFLOW_RESULT"


class WorkflowNotFoundException(Exception):
    pass


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
    result: Any
    state: EventManager | None
    exception: Any

    @staticmethod
    def create_started(start_time: datetime):
        return WorkflowStatus(Status.RUNNING, start_time, None, None, None, None)

    @staticmethod
    def create_successfully_completed(start_time: datetime, result: Any, state: EventManager | None):
        return WorkflowStatus(Status.COMPLETED, start_time, get_current_timestamp(), result, state, None)

    @staticmethod
    def create_suspended(start_time: datetime, state: EventManager):
        return WorkflowStatus(Status.SUSPENDED, start_time, None, None, state, None)

    @staticmethod
    def create_errored(start_time: datetime, exception: Any):
        return WorkflowStatus(Status.ERRORED, start_time, get_current_timestamp(), None, None, exception)


def find_workflow() -> 'Workflow':
    outer_frame = inspect.currentframe()
    is_workflow = False
    while not is_workflow:
        outer_frame = outer_frame.f_back
        if outer_frame is None:
            raise WorkflowNotFoundException("Workflow object not found in event stack")
        is_workflow = isinstance(outer_frame.f_locals.get('self'), Workflow)
    return outer_frame.f_locals.get('self')


class SetState:
    def __init__(self):
        self.state_id = None

    async def async_init(self, name: str, initial_value, identity):
        self.state_id = await find_workflow().create_state(name, initial_value, identity)
        return self

    async def __call__(self, value):
        # Make invoke state also act like an event
        return await find_workflow().set_state(self.state_id, value)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if not isinstance(exc_type, WorkflowSuspended):
            await find_workflow().remove_state(self.state_id)


async def state(name, initial_value=None, identity=None) -> SetState:
    return await SetState().async_init(name, initial_value, identity)


class Queue:
    def __init__(self):
        self.queue_id = None

    async def async_init(self, name: str, *args, **kwargs) -> 'Queue':
        self.queue_id = await find_workflow().create_queue(name, *args, **kwargs)
        return self

    async def check(self):
        return await find_workflow().check_queue(self.queue_id)

    async def pop(self):
        return await find_workflow().pop_queue(self.queue_id)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if isinstance(exc_type, WorkflowSuspended):
            await find_workflow().remove_queue(self.queue_id)


async def queue(name, identity=None) -> Queue:
    return await Queue().async_init(name, identity)


def step(func):
    @wraps(func)
    async def new_func(*args, **kwargs):
        return await find_workflow().handle_step(func.__name__, func, *args, **kwargs)

    return new_func


def _step(func):
    @wraps(func)
    async def new_func(self, *args, **kwargs):
        args = (self,) + args
        return await self.handle_step(func.__name__, func, *args, **kwargs)

    return new_func


class StepEntry(TypedDict):
    step_id: str
    name: str
    value: Any


class StateEntry(TypedDict):
    state_id: str
    name: str
    value: Any
    identity: str


class QueueEntry(TypedDict):
    queue_id: str
    name: str
    values: list
    identity: str


def get_current_timestamp() -> datetime:
    return datetime.utcnow()


def assign_identity():
    return str(uuid.uuid4())


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
        self.status = WorkflowStatus.create_started(self.started)
        self.state: EventManager[StateEntry] = state_manager  # dict[str, StateEntry]
        self.queues: EventManager[QueueEntry] = step_manager  # dict[str, QueueEntry]
        self.steps: EventManager[StepEntry] = queue_manager  # dict[str, EventEntry]
        self.unique_ids: dict[str, UniqueEvent] = {}

    def _get_unique_id(self, event_name: str) -> str:
        prefixed_name = self._get_prefixed_name(event_name)
        if prefixed_name not in self.unique_ids.keys():
            self.unique_ids[prefixed_name] = UniqueEvent(prefixed_name)
            self._replay_events.append(self.unique_ids[prefixed_name])
        return next(self.unique_ids[prefixed_name])

    async def handle_step(self, step_name: str, func: Callable, *args, **kwargs):
        """This is called by the @event decorator"""
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
                value=payload
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

    async def get_state(self, state_id):
        # Called by workflow manager, doesn't need to be a step
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
    async def push_queue(self, queue_id, value) -> None | str:
        # Called by Workflow Manager
        identity = self.queues[queue_id]['identity'] or assign_identity()
        self.queues[queue_id]['values'].append((identity, value))
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

    async def _async_run(self):
        self._reset()
        try:
            args = self.steps[PREFIXED_ARGUMENTS]["value"]
            kwargs = self.steps[PREFIXED_KW_ARGUMENTS]["value"]
            logging.debug('Invoking workflow')
            result = await self._func(*args, **kwargs)
            logging.debug('Workflow invocation complete')

            if WORKFLOW_RESULT not in self.steps:
                async def result_func():
                    return result

                await self.handle_step(WORKFLOW_RESULT, result_func)

            self.status = WorkflowStatus.create_successfully_completed(self.started, result, self.state)
            return self.status

        except WorkflowSuspended as ws:
            self.status = WorkflowStatus.create_suspended(self.started, self.state)
            return self.status

        except Exception as e:
            logging.debug(f'Workflow Errored: {e}')
            self.status = WorkflowStatus.create_errored(self.started, e)
            return self.status

    async def async_start(self, *args, **kwargs) -> WorkflowStatus:
        async def args_func():
            return args

        async def kwargs_func():
            return kwargs

        args = await self.handle_step(ARGUMENTS, args_func)
        kwargs = await self.handle_step(KW_ARGUMENTS, kwargs_func)
        result = await self._async_run()
        await self.create_state('return', result.result, None)
        return result

    def get_current_status(self):
        return self.status


if __name__ == '__main__':
    pass
