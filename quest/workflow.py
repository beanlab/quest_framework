import inspect
import logging
from datetime import datetime
from functools import wraps
from typing import Any, Protocol
from .events import Event, UniqueEvent, EventManager

ARGUMENTS = "INITIAL_ARGUMENTS"
KW_ARGUMENTS = "INITIAL_KW_ARGUMENTS"
WORKFLOW_RESULT = "WORKFLOW_RESULT"


class WorkflowNotFoundException(Exception):
    pass


class WorkflowSuspended(BaseException):
    def __init__(self, event_name):
        self.event_name = event_name


class WorkflowFunction(Protocol):
    def __call__(self, *args, **kwargs) -> Any: ...


class WorkflowResult:
    payload: Any

    def __init__(self, payload):
        self.payload = payload


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
                return await find_workflow().run_signal_event(func_or_name)

            return new_func

        return decorator
    else:
        async def new_func(*args, **kwargs):
            return await find_workflow().run_signal_event(func_or_name.__name__)

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

    async def _await_signal_event(self, event_name: str) -> Any:
        if event_name in self._events:
            payload = self._events[event_name]["payload"]
            logging.debug(f'Retrieving event {event_name}: {payload}')
            return payload
        else:
            self.prefix = []
            raise WorkflowSuspended(event_name)

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

            return WorkflowResult(result)

        except WorkflowSuspended as ws:
            logging.debug(f'Workflow Suspended: awaiting event {ws.event_name}')
            name = next(self._events.counter("_await_event"))
            self._events[name] = _make_event(ws.event_name)

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

    async def run_signal_event(self, event_name: str):
        logging.debug(f'Registering signal event: {event_name}')
        return await self._await_signal_event(self._get_unique_signal_name(event_name))

    async def async_start(self, *args, **kwargs) -> WorkflowResult:
        self._record_event(ARGUMENTS, args)
        self._record_event(KW_ARGUMENTS, kwargs)
        return await self._async_run()

    async def async_send_event(self, event_name: str, payload: Any) -> WorkflowResult:
        self._record_event(
            next(self._events.counter(event_name)),
            payload
        )
        return await self._async_run()


if __name__ == '__main__':
    pass
