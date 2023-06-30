import logging
import traceback
import uuid
from datetime import datetime
from functools import wraps
from typing import Any, Protocol, Optional, Callable, TypedDict, Literal
from .events import UniqueEvent, EventManager
from dataclasses import dataclass
import logging

ARGUMENTS = "INITIAL_ARGUMENTS"
KW_ARGUMENTS = "INITIAL_KW_ARGUMENTS"


# TODO - change to identities
# For each identity (including None)
# create the step ID, check the resource
# if the resource does not exist for any identity
# throw an error: resource name, identity list

class WorkflowSuspended(BaseException):
    pass


class InvalidIdentityError(BaseException):
    pass


class WorkflowFunction(Protocol):
    def __init__(self, *args, **kwargs): ...

    def __call__(self, *args, **kwargs) -> Any: ...


Status = Literal['RUNNING', 'SUSPENDED', 'COMPLETED', 'ERRORED']


@dataclass
class WorkflowStatus:
    status: Status
    started: Optional[str]
    ended: Optional[str]
    steps: dict[str, 'StepEntry']
    state: dict[str, 'StateEntry']
    queues: dict[str, 'QueueEntry']

    def get_result(self):
        return self.state['result']

    def get_error(self):
        return self.state['error']

    def to_json(self):
        return {
            'status': self.status,
            'started': self.started,
            'ended': self.ended,
            'steps': self.steps,
            'state': self.state,
            'queues': self.queues
        }


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


def get_current_timestamp() -> str:
    return datetime.utcnow().isoformat()


def assign_identity():
    return str(uuid.uuid4())


def filter_identities(identities: list, dict_to_filter: EventManager) -> dict:
    ret_dict = {}
    for key, value in dict_to_filter.items():
        if value['identity'] is None or value['identity'] in identities:
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
        self.status: Status = 'RUNNING'

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

    async def get_status(self, identities: list[str], include_steps, include_state, include_queues):
        steps = filter_identities(identities, self.steps) if include_steps else None
        state = filter_identities(identities, self.state) if include_state else None
        queues = filter_identities(identities, self.queues) if include_queues else None
        status = WorkflowStatus(
            self.status,
            self.started,
            self.ended,
            steps,
            state,
            queues
        )
        logging.debug(f'STATUS: {status}')
        return status

    def _get_unique_id(self, event_name: str, replay=True) -> str:
        prefixed_name = self._get_prefixed_name(event_name)
        if prefixed_name not in self.unique_ids:
            self.unique_ids[prefixed_name] = UniqueEvent(prefixed_name, replay=replay)
        return next(self.unique_ids[prefixed_name])

    async def handle_step(self, step_name: str, func: Callable, *args, replay=True, identity=None, **kwargs):
        """This is called by the @step decorator"""
        step_id = self._get_unique_id(step_name, replay=replay)

        if step_id in self.steps:
            logging.debug(f'{self.workflow_id} HANDLE_STEP CACHE: {step_id} {step_name} {args} {identity}')
            return self.steps[step_id]['value']
        else:
            logging.debug(f'{self.workflow_id} HANDLE_STEP RUN: {step_id} {step_name} {args} {identity}')
            self._prefix.append(step_id)
            payload = await func(*args, **kwargs)
            self._prefix.pop(-1)
            self.steps[step_id] = StepEntry(
                step_id=step_id,
                name=step_name,
                value=payload,
                identity=identity
            )
            return payload

    @_step
    async def create_state(self, name, initial_value, identity):
        state_id = create_id(name, identity)
        logging.debug(f'{self.workflow_id} CREATE_STATE: {state_id} {name} {initial_value} {identity}')
        self.state[state_id] = StateEntry(
            name=name,
            value=initial_value,
            identity=identity
        )
        return name, identity

    @_step
    async def remove_state(self, name, identity):
        state_id = create_id(name, identity)
        logging.debug(f'{self.workflow_id} REMOVE_STATE: {state_id} {name} {identity}')
        del self.state[state_id]
        return name, identity

    @_step
    async def set_state(self, name, identity, value):
        state_id = create_id(name, identity)
        logging.debug(f'{self.workflow_id} SET_STATE: {state_id} {name} {value} {identity}')
        self.state[state_id]['value'] = value

    @_step
    async def create_queue(self, name, identity):
        queue_id = create_id(name, identity)
        logging.debug(f'{self.workflow_id} CREATE_QUEUE: {queue_id} {name} {identity}')
        self.queues[queue_id] = QueueEntry(
            name=name,
            values=[],
            identity=identity,
        )
        return name, identity

    async def push_queue(self, name, value, identity: str | None) -> None | str:
        step_name = 'push_queue_' + name
        step_id = self._get_unique_id(step_name, False)
        identity = await self._push_queue(name, value, identity)
        logging.debug(f'{self.workflow_id} PUSH_QUEUE: {step_id} {name} {value} {identity}')
        self.steps[step_id] = StepEntry(
            step_id=step_id,
            name=step_name,
            value=value,
            identity=identity
        )
        await self.start()
        return identity

    async def _push_queue(self, name, value, identity) -> None | str:
        # Called by Workflow Manager
        queue_id = create_id(name, identity)
        if queue_id not in self.queues:
            raise InvalidIdentityError()  # No matching queue for those identities

        identity = identity or assign_identity()

        self.queues[queue_id]['values'].append((identity, value))
        return identity

    @_step
    async def pop_queues(self, queues: list[tuple[str, str | None]]):
        for index, (name, identity) in enumerate(queues):
            queue_id = create_id(name, identity)
            if self.queues[queue_id]['values']:
                logging.debug(f'{self.workflow_id} POP_QUEUES RETRIEVING: {queue_id} {name} {identity}')
                return index, identity, self.queues[queue_id]['values'].pop(0)

        logging.debug(f'{self.workflow_id} POP_QUEUES SUSPENDING: {queues}')
        raise WorkflowSuspended()

    @_step
    async def check_queue(self, name, identity) -> bool:
        queue_id = create_id(name, identity)
        logging.debug(f'{self.workflow_id} CHECK_QUEUE: {queue_id} {name} {identity}')
        return bool(self.queues[queue_id]['values'])

    @_step
    async def remove_queue(self, name, identity):
        queue_id = create_id(name, identity)
        logging.debug(f'{self.workflow_id} REMOVE_QUEUE: {queue_id} {name} {identity}')
        del self.queues[queue_id]

    def _get_prefixed_name(self, event_name: str) -> str:
        return '.'.join(self._prefix) + '.' + event_name

    def _reset(self):
        self._prefix = []
        for _, ue in self.unique_ids.items():
            ue.reset()

    async def start(self, *args, **kwargs) -> WorkflowStatus:
        logging.debug(f'{self.workflow_id} START: {args} {kwargs}')
        self._reset()
        try:
            args = await self.handle_step(ARGUMENTS, alambda(args))
            kwargs = await self.handle_step(KW_ARGUMENTS, alambda(kwargs))
            result = await self._func(*args, **kwargs)
            self.ended = get_current_timestamp()
            logging.debug(f'{self.workflow_id} COMPLETED')
            self.status = 'COMPLETED'
            # noinspection PyTypeChecker
            await self.create_state('result', result, None)

        except WorkflowSuspended as _:
            logging.debug(f'{self.workflow_id} SUSPENDED')
            self.status = 'SUSPENDED'

        except Exception as e:
            logging.warning(f'{self.workflow_id} ERRORED {e}')
            logging.debug(f'{self.workflow_id} ERRORED {e} {traceback.format_exc()}')
            self.status = 'ERRORED'
            # noinspection PyTypeChecker
            await self.create_state('error', {'name': str(e), 'details': traceback.format_exc()}, None)
            self.ended = get_current_timestamp()

        return await self.get_status([], True, True, True)


if __name__ == '__main__':
    pass
