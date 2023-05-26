import inspect
import logging
from datetime import datetime
from enum import Enum
from functools import wraps
from typing import Any, Protocol
from .events import Event, UniqueEvent, EventManager

ARGUMENTS = "INITIAL_ARGUMENTS"
KW_ARGUMENTS = "INITIAL_KW_ARGUMENTS"
WORKFLOW_RESULT = "WORKFLOW_RESULT"


class WorkflowNotFoundException(Exception):
    pass


class WorkflowSuspended(BaseException):
    def __init__(self, event_name, *args, **kwargs):
        self.event_name = event_name
        self.args = args
        self.kwargs = kwargs


class WorkflowFunction(Protocol):
    def __call__(self, *args, **kwargs) -> Any: ...


class Status(Enum):
    NOT_STARTED = 1
    RUNNING = 2
    AWAITING_SIGNAL = 3
    COMPLETED = 4
    ERRORED = 5


class Signal:
    def __init__(self, name: str, *args, **kwargs):
        self.name: str = name
        self.args: tuple = args
        self.kwargs: dict = kwargs


class WorkflowStatus:
    payload: Any

    def __init__(self, status=Status.NOT_STARTED, started=None, ended=None, exception=None, result=None, signal=None):
        self.started: str = started
        self.ended: str = ended
        self.exception: Any = exception
        self.result: Any = result
        self.signal: Signal = signal
        self.status: Status = status

    def set_started(self):
        self.started = datetime.utcnow().isoformat()

    def set_ended(self):
        self.ended = datetime.utcnow().isoformat()

    def set_exception(self, exception: Any):
        self.exception = exception

    def set_result(self, result: Any):
        self.result = result

    def set_signal(self, signal: Any):
        self.signal = signal

    def set_status(self, status: Status):
        self.status = status

    def is_completed(self):
        return self.status == Status.COMPLETED


def find_workflow():
    outer_frame = inspect.currentframe()
    is_workflow = False
    while not is_workflow:
        outer_frame = outer_frame.f_back
        if outer_frame is None:
            raise WorkflowNotFoundException("Workflow object not found in event stack")
        is_workflow = isinstance(outer_frame.f_locals.get('self'), Workflow)
    return outer_frame.f_locals.get('self')


def async_event(func):
    @wraps(func)
    async def new_func(*args, **kwargs):
        return await find_workflow().run_async_event(func.__name__, func, *args, **kwargs)

    return new_func


def async_signal(func_or_name):
    if isinstance(func_or_name, str):
        def decorator(func):
            func.__event_name = func_or_name

            async def new_func(*args, **kwargs):
                return await find_workflow().execute_signal(func_or_name, args, kwargs)

            return new_func

        return decorator
    else:
        async def new_func(*args, **kwargs):
            return await find_workflow().execute_signal(func_or_name.__name__, args, kwargs)

        return new_func


def is_async(func):
    return inspect.iscoroutinefunction(func)


def _make_event(payload) -> Event:
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "payload": payload
    }


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
        self.status = WorkflowStatus()

    def _workflow_type(self) -> str:
        return self._func.__class__.__name__

    def _get_unique_event_name(self, event_name: str) -> str:
        prefixed_name = self._get_prefixed_name(event_name)
        if prefixed_name not in self.unique_events:
            self.unique_events[prefixed_name] = UniqueEvent(prefixed_name)
            self._replay_events.append(self.unique_events[prefixed_name])
        return next(self.unique_events[prefixed_name])

    def _get_unique_signal_name(self, signal_name: str) -> str:
        if signal_name not in self.unique_events:
            self.unique_events[signal_name] = UniqueEvent(signal_name)
            self._replay_events.append(self.unique_events[signal_name])
        return next(self.unique_events[signal_name])

    def _get_prefixed_name(self, event_name: str) -> str:
        return '.'.join(self.prefix) + '.' + event_name

    def _reset(self):
        for ue in self._replay_events:
            ue.reset()

    def _record_event(self, event_name: str, payload: Any):
        if event_name in self._events:
            raise Exception(f"Duplicate event: {event_name}")
        logging.debug(f'Event recorded: {event_name}, {payload}')
        self._events[event_name] = _make_event(payload)

    async def _await_signal_event(self, event_name: str, *args, **kwargs) -> Any:
        if event_name in self._events:
            payload = self._events[event_name]["payload"]
            logging.debug(f'Retrieving event {event_name}: {payload}')
            return payload
        else:
            self.prefix = []
            raise WorkflowSuspended(event_name, args, kwargs)

    async def _async_run(self):
        self._reset()
        try:
            args = self._events[ARGUMENTS]["payload"]
            kwargs = self._events[KW_ARGUMENTS]["payload"]
            logging.debug('Invoking workflow')
            self.status.set_started()
            self.status.set_status(Status.RUNNING)
            result = await self._func(*args, **kwargs)
            self.status.set_ended()
            logging.debug('Workflow invocation complete')
            self.status.set_status(Status.COMPLETED)
            self.status.set_result(result)
            self.status.set_signal(None)

            if WORKFLOW_RESULT not in self._events:
                self._record_event(WORKFLOW_RESULT, result)

            return self.status

        except WorkflowSuspended as ws:
            logging.debug(f'Workflow Suspended: awaiting event {ws.event_name}')
            name = next(self._events.counter("_await_event"))
            self._events[name] = _make_event(ws.event_name)
            self.status.set_status(Status.AWAITING_SIGNAL)
            self.status.set_signal(Signal(ws.event_name, ws.args, ws.kwargs))
            return self.status
        except Exception as e:
            logging.debug(f'Workflow Errored: {str(e)}')
            self.status.set_exception(e)
            self.status.set_status(Status.ERRORED)
            self.status.set_ended()
            return self.status

    def get_current_status(self):
        return self.status

    async def run_async_event(self, event_name, func, *args, **kwargs):
        _event_name = self._get_unique_event_name(event_name)

        if _event_name in self._events:
            return self._events[_event_name]['payload']
        else:
            self.prefix.append(event_name)
            payload = await func(*args, **kwargs)
            self.prefix.pop(-1)
            self._record_event(_event_name, payload)
            return payload

    async def execute_signal(self, event_name: str, *args, **kwargs):
        logging.debug(f'Registering signal event: {event_name}')
        return await self._await_signal_event(self._get_unique_signal_name(event_name), args, kwargs)

    async def async_start(self, *args, **kwargs) -> WorkflowStatus:
        self._record_event(ARGUMENTS, args)
        self._record_event(KW_ARGUMENTS, kwargs)
        return await self._async_run()

    async def async_send_event(self, event_name: str, payload: Any) -> WorkflowStatus:
        self._record_event(
            next(self._events.counter(event_name)),
            payload
        )
        return await self._async_run()


if __name__ == '__main__':
    pass
