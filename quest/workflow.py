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


class SignalException(Exception):
    def __init__(self, name: str, *args, **kwargs):
        self.name = name
        self.args = args
        self.kwargs = kwargs


class WorkflowSuspended(BaseException):
    def __init__(self, event_name, *args, **kwargs):
        self.event_name = event_name
        self.args = args
        self.kwargs = kwargs


class WorkflowFunction(Protocol):
    def __init__(self, workflow_manager, *args, **kwargs): ...

    def __call__(self, *args, **kwargs) -> Any: ...


class Status(Enum):
    RUNNING = 1
    AWAITING_SIGNALS = 2
    COMPLETED = 3
    ERRORED = 4


class Signal:
    def __init__(self, name: str, *args, **kwargs):
        self.name: str = name
        self.args: tuple = args
        self.kwargs: dict = kwargs


RT = TypeVar('RT')
CRT = TypeVar('CRT')


class Promise:
    def __init__(self, signal_name: str, unique_signal_name: str, callback: Optional[Callable[[RT], CRT]], *args,
                 **kwargs):
        self.signal_name: str = signal_name
        self.unique_signal_name: str = unique_signal_name
        self.args: tuple[Any] = args
        self.kwargs: dict[str, Any] = kwargs
        self.callback: Optional[Callable[[RT], CRT]] = callback

    async def join(self) -> CRT:
        return await find_workflow().async_handle_signal(self.unique_signal_name, self.signal_name, *self.args,
                                                         **self.kwargs)


@dataclass
class WorkflowStatus:
    status: Status
    started: Optional[datetime]
    ended: Optional[datetime]
    result: Any
    signals: Optional[list[Signal]]
    exception: Any

    @staticmethod
    def create_started(start_time: datetime):
        return WorkflowStatus(Status.RUNNING, start_time, None, None, None, None)

    @staticmethod
    def create_successfully_completed(start_time: datetime, result: Any):
        return WorkflowStatus(Status.COMPLETED, start_time, get_current_timestamp(), result, None, None)

    @staticmethod
    def create_signaled(start_time: datetime, signals: list[Signal]):
        return WorkflowStatus(Status.AWAITING_SIGNALS, start_time, None, None, signals, None)

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


def event(func):
    @wraps(func)
    async def new_func(*args, **kwargs):
        return await find_workflow().async_handle_event(func.__name__, func, *args, **kwargs)

    return new_func


def signal(func_or_name) -> Callable:
    if isinstance(func_or_name, str):
        def decorator(func):
            func.__event_name = func_or_name
            if inspect.getfullargspec(func).args[0] == 'self':
                async def new_func(self, *args, **kwargs):
                    return await (await find_workflow().async_start_signal(func_or_name, *args, **kwargs)).join()
            else:
                async def new_func(*args, **kwargs):
                    return await (await find_workflow().async_start_signal(func_or_name, *args, **kwargs)).join()

            return new_func

        return decorator
    else:
        if inspect.getfullargspec(func_or_name).args[0] == 'self':
            async def new_func(self, *args, **kwargs):
                return await (await find_workflow().async_start_signal(func_or_name.__name__, *args, **kwargs)).join()
        else:
            async def new_func(*args, **kwargs):
                return await (await find_workflow().async_start_signal(func_or_name.__name__, *args, **kwargs)).join()

        return new_func


def promised_signal(func_or_name) -> Callable:
    if isinstance(func_or_name, str):
        def decorator(func):
            func.__event_name = func_or_name
            if inspect.getfullargspec(func).args[0] == 'self':
                async def new_func(self, *args, **kwargs):
                    return await find_workflow().async_start_signal(func_or_name, *args, **kwargs)
            else:
                async def new_func(*args, **kwargs):
                    return await find_workflow().async_start_signal(func_or_name, *args, **kwargs)

            return new_func

        return decorator
    else:
        if inspect.getfullargspec(func_or_name).args[0] == 'self':
            async def new_func(self, *args, **kwargs):
                return await find_workflow().async_start_signal(func_or_name.__name__, *args, **kwargs)
        else:
            async def new_func(*args, **kwargs):
                return await find_workflow().async_start_signal(func_or_name.__name__, *args, **kwargs)

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
        self.promised_signals = []
        self.unique_promised_signal_names = set()
        self.status = WorkflowStatus.create_started(self.started)

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
        self._events[event_name] = _make_payload_event(payload)

    def _record_exception_event(self, event_name: str, exception: Exception, *args, **kwargs):
        if event_name in self._events:
            raise Exception(f"Duplicate event: {event_name}")
        logging.debug(f'Exception event recorded: {event_name}, {exception}')
        self._events[event_name] = _make_exception_event(exception, *args, **kwargs)

    async def _await_signal_event(self, unique_event_name: str, event_name: str, *args, **kwargs) -> Any:
        if unique_event_name in self._events:
            exception = self._events[unique_event_name]["exception"]
            if exception is not None:
                logging.debug(f'Retrieving exception event {unique_event_name}: {exception}')
                raise SignalException(exception["name"], *exception["args"], **exception["kwargs"])
            else:
                payload = self._events[unique_event_name]["payload"]
                logging.debug(f'Retrieving event {unique_event_name}: {payload}')
                return payload
        else:
            self.prefix = []
            raise WorkflowSuspended(event_name, *args, **kwargs)

    def _remove_from_promised_signals(self, signal_name: str):
        for sig in self.promised_signals:
            if sig.name == signal_name:
                self.promised_signals.remove(sig)
                break

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
            logging.debug(f'Workflow Suspended: awaiting event {ws.event_name}')
            name = next(self._events.counter("_await_event"))
            self._events[name] = _make_payload_event(ws.event_name)
            self.status = WorkflowStatus.create_signaled(self.started, self.promised_signals)
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

    async def async_start_signal(self, signal_name: str, *args, **kwargs) -> Promise:
        unique_signal_name = self._get_unique_signal_name(signal_name)
        if unique_signal_name not in self.unique_promised_signal_names and unique_signal_name not in self._events:
            self.unique_promised_signal_names.add(unique_signal_name)
            self.promised_signals.append(Signal(signal_name, *args, **kwargs))
        return Promise(signal_name, unique_signal_name, None, *args, **kwargs)

    async def async_handle_signal(self, unique_signal_name: str, signal_name: str, *args, **kwargs):
        """This is called by the @signal decorator"""

        logging.debug(f'Registering signal event: {unique_signal_name}')
        return await self._await_signal_event(unique_signal_name, signal_name, *args, **kwargs)

    async def async_start(self, *args, **kwargs) -> WorkflowStatus:
        self._record_event(ARGUMENTS, args)
        self._record_event(KW_ARGUMENTS, kwargs)
        return await self._async_run()

    async def async_send_signal(self, event_name: str, payload: Any) -> WorkflowStatus:
        self._record_event(
            next(self._events.counter(event_name)),
            payload
        )
        self._remove_from_promised_signals(event_name)
        return await self._async_run()

    async def async_send_signal_exception(self, event_name: str, exception: Exception, *args, **kwargs) -> WorkflowStatus:
        self._record_exception_event(
            next(self._events.counter(event_name)),
            exception,
            *args,
            **kwargs
        )
        self._remove_from_promised_signals(event_name)
        return await self._async_run()


if __name__ == '__main__':
    pass
