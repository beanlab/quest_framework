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


class InvalidIdentityError(BaseException):
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
    name: str
    value: Any
    identity: Optional[str]


class QueueEntry(TypedDict):
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


def create_id(name: str, identity: str) -> str:
    return f'{name}.{identity}' if identity is not None else name


class Workflow:
    """
    Function decorator
    """

    def __init__(
            self,
            workflow_id: str,
            func: WorkflowFunction,
            step_manager: EventManager[StepEntry],
            state_manager: EventManager[StateEntry],
            queue_manager: EventManager[QueueEntry],
            unique_ids: EventManager[UniqueEvent]
    ):
        self.workflow_id = workflow_id
        self.started = get_current_timestamp()
        self.ended = None
        self.status = Status.RUNNING

        self._func = func
        self._prefix = []

        self.steps: EventManager[StepEntry] = step_manager
        self.state: EventManager[StateEntry] = state_manager
        self.queues: EventManager[QueueEntry] = queue_manager
        self.unique_ids: EventManager[UniqueEvent] = unique_ids

    def workflow_type(self) -> str:
        """Used by serializer"""
        if (name := self._func.__class__.__name__) == 'function':
            return self._func.__name__
        else:
            return name

    async def get_status(self, identity, include_steps, include_state, include_queues):
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

    def _get_unique_id(self, event_name: str, replay=True) -> str:
        prefixed_name = self._get_prefixed_name(event_name)
        if prefixed_name not in self.unique_ids:
            self.unique_ids[prefixed_name] = UniqueEvent(prefixed_name, replay=replay)
        return next(self.unique_ids[prefixed_name])

    async def handle_step(self, step_name: str, func: Callable, *args, replay=True, **kwargs):
        """This is called by the @step decorator"""
        step_id = self._get_unique_id(step_name, replay=replay)

        if step_id in self.steps:
            return self.steps[step_id]['value']
        else:
            self._prefix.append(step_id)
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
        state_id = create_id(name, identity)
        self.state[state_id] = StateEntry(
            name=name,
            value=initial_value,
            identity=identity
        )
        return name, identity

    @_step
    async def remove_state(self, name, identity):
        del self.state[create_id(name, identity)]
        return name, identity

    @_step
    async def set_state(self, name, identity, value):
        self.state[create_id(name, identity)]['value'] = value

    @_step
    async def create_queue(self, name, identity):
        queue_id = create_id(name, identity)
        self.queues[queue_id] = QueueEntry(
            name=name,
            values=[],
            identity=identity,
        )
        return name, identity

    async def push_queue(self, name, value, identity) -> None | str:
        identity = await self.handle_step(
            'push_queue', self._push_queue, name, value, identity, replay=False)
        await self.start()
        return identity

    async def _push_queue(self, name, value, identity) -> None | str:
        # Called by Workflow Manager
        queue_id = create_id(name, identity)
        if (queue_identity := self.queues[queue_id]['identity']) is not None and queue_identity != identity:
            raise InvalidIdentityError()

        identity = queue_identity or assign_identity()
        self.queues[queue_id]['values'].append((identity, value))
        return identity

    @_step
    async def pop_queue(self, name, identity):
        queue_id = create_id(name, identity)
        if self.queues[queue_id]['values']:
            return self.queues[queue_id]['values'].pop(0)
        else:
            raise WorkflowSuspended()

    @_step
    async def check_queue(self, name, identity) -> bool:
        return bool(self.queues[create_id(name, identity)]['values'])

    @_step
    async def remove_queue(self, name, identity):
        del self.queues[create_id(name, identity)]

    def _get_prefixed_name(self, event_name: str) -> str:
        return '.'.join(self._prefix) + '.' + event_name

    def _reset(self):
        self._prefix = []
        for _, ue in self.unique_ids.items():
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

        return await self.get_status(None, True, True, True)


if __name__ == '__main__':
    pass
