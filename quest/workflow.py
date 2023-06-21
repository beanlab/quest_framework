import inspect
import logging
from datetime import datetime
from enum import Enum
from functools import wraps
from typing import Any, Protocol, Optional, TypeVar, Callable
from .events import Event, UniqueEvent, EventManager
from dataclasses import dataclass

ARGUMENTS = "INITIAL_ARGUMENTS"
KW_ARGUMENTS = "INITIAL_KW_ARGUMENTS"
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
    AWAITING_SIGNALS = 2
    COMPLETED = 3
    ERRORED = 4


@dataclass
class WorkflowStatus:
    status: Status
    started: Optional[datetime]
    ended: Optional[datetime]
    result: Any
    state: dict[str, 'State']
    exception: Any

    @staticmethod
    def create_started(start_time: datetime):
        return WorkflowStatus(Status.RUNNING, start_time, None, None, None, None)

    @staticmethod
    def create_successfully_completed(start_time: datetime, result: Any):
        return WorkflowStatus(Status.COMPLETED, start_time, get_current_timestamp(), result, None, None)

    @staticmethod
    def create_suspended(start_time: datetime, state: dict[str, 'State']):
        return WorkflowStatus(Status.AWAITING_SIGNALS, start_time, None, None, state, None)

    @staticmethod
    def create_errored(start_time: datetime, exception: Any):
        return WorkflowStatus(Status.ERRORED, start_time, get_current_timestamp(), None, None, exception)


class NoValue:
    ...


class IncorrectAssignIdError(Exception):
    pass


def state(name: str, **kwargs):
    new_state = State(name, **kwargs)
    find_workflow().add_to_state(name, new_state)
    return new_state


def assigned_id_decorator(func):
    def wrapper(self, *args, **kwargs):
        if 'assign_id' not in kwargs:
            raise TypeError("my_argument is a required keyword argument.")
        if kwargs['assign_id'] != self.assign_id:
            raise IncorrectAssignIdError("assign_id for state is incorrect")
        result = func(*args)
        return kwargs['assign_id'], result

    return wrapper


class State:
    def __init__(self, name: str, **kwargs):
        # State declaration
        self.value = NoValue
        self.name = name
        # Other variables
        self.replayed = False
        self.default = True
        self.writeable = False
        self.namespace = "global"
        self.assign_id = None
        # This takes care of the kw options
        self.options = {"default", "writeable", "assign_id", "namespace"}
        for kw_name, kw_value in kwargs.items():
            if kw_name in self.options:
                setattr(self, kw_name, kw_value)
        # handle kw option changes
        if self.default:
            self.value = None
        if self.assign_id is not None:
            self.__call__ = assigned_id_decorator(self.__call__)

    def __enter__(self):
        if not self.replayed:
            # find workflow and add self
            find_workflow().add_to_state(self.name, self)

    def __exit__(self, exc_type, exc_value, traceback):
        if not isinstance(exc_type, WorkflowSuspended):
            # remove from state, so it isn't "visible"
            find_workflow().remove_from_state(self.name)

    def __call__(self, *args):
        if len(args) >= 1:
            self.set_state(*args)
        return self.get_state()

    def set_state(self, value):
        self.value = value
        find_workflow().add_to_state(self.name, self.value)

    def get_state(self):
        if self.value is NoValue:
            raise WorkflowSuspended()
        return self.value


def find_workflow() -> 'Workflow':
    outer_frame = inspect.currentframe()
    is_workflow = False
    while not is_workflow:
        outer_frame = outer_frame.f_back
        if outer_frame is None:
            raise WorkflowNotFoundException("Workflow object not found in event stack")
        is_workflow = isinstance(outer_frame.f_locals.get('self'), Workflow)
    return outer_frame.f_locals.get('self')


def event(func):
    @wraps(func)
    async def new_func(*args, **kwargs):
        return await find_workflow().async_handle_event(func.__name__, func, *args, **kwargs)

    return new_func


def is_async(func):
    return inspect.iscoroutinefunction(func)


def _make_payload_event(payload) -> Event:
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "payload": payload,
        "exception": None
    }


def _make_exception_event(exception: Exception, *args, **kwargs) -> Event:
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "payload": None,
        "exception": {"name": exception.__class__.__name__, "args": args, "kwargs": kwargs}
    }


def get_current_timestamp() -> datetime:
    return datetime.utcnow()


class Workflow:
    """
    Function decorator
    """

    def __init__(self,
                 workflow_id: str,
                 func: WorkflowFunction,
                 event_manager: EventManager
                 ):
        self.workflow_id = workflow_id
        self._events: EventManager = event_manager
        self._replay_events: list[UniqueEvent] = []
        self._func = func
        self.prefix = []
        self.unique_events = {}
        self.started = get_current_timestamp()
        self.status = WorkflowStatus.create_started(self.started)
        # TODO: STATE OBJECT
        self.state: dict[str, dict] = {}

    def add_to_state(self, name: str, state_obj: State):
        self.state[name] = state_obj

    def remove_from_state(self, name: str):
        self.state.pop(name)

    async def change_state(self, name: str, value: Any):
        state_obj = self.state[name]
        if state_obj.writeable:
            state_obj.set_state(value)
            await self._async_run()

    def _workflow_type(self) -> str:
        return self._func.__class__.__name__

    def _get_unique_event_name(self, event_name: str) -> str:
        prefixed_name = self._get_prefixed_name(event_name)
        if prefixed_name not in self.unique_events:
            self.unique_events[prefixed_name] = UniqueEvent(prefixed_name)
            self._replay_events.append(self.unique_events[prefixed_name])
        return next(self.unique_events[prefixed_name])

    def _get_prefixed_name(self, event_name: str) -> str:
        return '.'.join(self.prefix) + '.' + event_name

    def _reset(self):
        self.prefix = []
        for ue in self._replay_events:
            ue.reset()

    def _record_event(self, event_name: str, payload: Any):
        if event_name in self._events:
            raise Exception(f"Duplicate event: {event_name}")
        logging.debug(f'Event recorded: {event_name}, {payload}')
        self._events[event_name] = _make_payload_event(payload)

    async def _async_run(self):
        self._reset()
        try:
            args = self._events[ARGUMENTS]["payload"]
            kwargs = self._events[KW_ARGUMENTS]["payload"]
            logging.debug('Invoking workflow')
            result = await self._func(*args, **kwargs)
            logging.debug('Workflow invocation complete')

            if WORKFLOW_RESULT not in self._events:
                self._record_event(WORKFLOW_RESULT, result)

            self.status = WorkflowStatus.create_successfully_completed(self.started, result)
            return self.status

        except WorkflowSuspended as ws:
            self.status = WorkflowStatus.create_suspended(self.started, self.state)
            return self.status

        except Exception as e:
            logging.debug(f'Workflow Errored: {e}')
            self.status = WorkflowStatus.create_errored(self.started, e)
            return self.status

    def get_current_status(self):
        return self.status

    async def async_handle_event(self, event_name, func, *args, **kwargs):
        """This is called by the @event decorator"""
        _event_name = self._get_unique_event_name(event_name)

        if _event_name in self._events:
            return self._events[_event_name]['payload']
        else:
            self.prefix.append(event_name)
            payload = await func(*args, **kwargs)
            self.prefix.pop(-1)
            self._record_event(_event_name, payload)
            return payload

    async def async_start(self, *args, **kwargs) -> WorkflowStatus:
        self._record_event(ARGUMENTS, args)
        self._record_event(KW_ARGUMENTS, kwargs)
        return await self._async_run()


if __name__ == '__main__':
    pass
