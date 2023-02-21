import json
import logging
from datetime import datetime
from typing import Any, Protocol

from .events import Event, UniqueEvent, EventManager, InMemoryEventManager

ARGUMENTS = "INITIAL_ARGUMENTS"
KW_ARGUMENTS = "INITIAL_KW_ARGUMENTS"


def event(func):
    func.__is_workflow_event = True
    func.__event_name = func.__name__
    return func


def external_event(func_or_name):
    if isinstance(func_or_name, str):
        def decorator(func):
            func.__is_external_event = True
            func.__event_name = func_or_name
            return func

        return decorator
    else:
        func_or_name.__is_external_event = True
        func_or_name.__event_name = func_or_name.__name__
        return func_or_name


class WorkflowFunction(Protocol):
    def __call__(self, *args, **kwargs) -> Any: ...


class WorkflowSuspended(BaseException):
    def __init__(self, event_name):
        self.event_name = event_name


def _make_event(payload) -> Event:
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "payload": payload

    }


class WorkflowResult:
    payload: Any

    def __init__(self, payload):
        self.payload = payload


class Workflow:
    """
    Function decorator
    """

    def __init__(self, func: WorkflowFunction, event_manager: EventManager = None):
        self._events: EventManager = event_manager if event_manager is not None else InMemoryEventManager()
        self._replay_events: list[UniqueEvent] = []
        self._func = self._decorate(func)

    def _decorate(self, func: WorkflowFunction):
        for prop_name in dir(func):
            prop = getattr(func, prop_name)
            if callable(prop) and hasattr(prop, '__is_workflow_event'):
                name = getattr(prop, '__event_name')
                setattr(func, prop_name, self._as_event(name, prop))

            elif callable(prop) and hasattr(prop, '__is_external_event'):
                name = getattr(prop, '__event_name')
                setattr(func, prop_name, self._as_external_event(name))

        return func

    def _as_event(self, event_name, func):
        """
        Decorator.
        Turns the function into an event.
        Events only happen once, and are replayed when the function is replayed.
        """
        logging.debug(f'Registering workflow event: {event_name}')

        _unique_name = UniqueEvent(event_name)
        self._replay_events.append(_unique_name)

        def new_func(*args, **kwargs):
            _event_name = next(_unique_name)

            if _event_name in self._events:
                return self._events[_event_name]
            else:
                payload = func(*args, **kwargs)
                self._record_event(_event_name, payload)
                return payload

        return new_func

    def _as_external_event(self, event_name: str):
        """
        Decorator.
        Indicates that `func` represents an external event.
        The event name will be that of the function.
        `func` should have a pass body.
        """
        logging.debug(f'Registering external event: {event_name}')

        _unique_name = UniqueEvent(event_name)
        self._replay_events.append(_unique_name)

        def new_func(*args, **kwargs):
            event_name = next(_unique_name)
            return self._await_event(event_name)

        return new_func

    def _reset(self):
        for ue in self._replay_events:
            ue.reset()

    def _record_event(self, event_name: str, payload: Any):
        if event_name in self._events:
            raise Exception(f"Duplicate event: {event_name}")
        logging.debug(f'Event recorded: {event_name}, {payload}')
        self._events[event_name] = _make_event(payload)

    def _await_event(self, event_name: str) -> Any:
        if event_name in self._events:
            payload = self._events[event_name]["payload"]
            logging.debug(f'Retrieving event {event_name}: {payload}')
            return payload
        else:
            raise WorkflowSuspended(event_name)

    def _run(self):
        self._reset()
        try:
            args = self._events[ARGUMENTS]["payload"]
            kwargs = self._events[KW_ARGUMENTS]["payload"]

            logging.debug('Invoking workflow')
            result = self._func(*args, **kwargs)
            logging.debug('Workflow invocation complete')
            return WorkflowResult(result)

        except WorkflowSuspended as ws:
            logging.debug(f'Workflow Suspended: awaiting event {ws.event_name}')
            name = next(self._events.counter("_await_event"))
            self._events[name] = _make_event(ws.event_name)

    def __call__(self, *args, **kwargs) -> WorkflowResult:
        self._record_event(ARGUMENTS, args)
        self._record_event(KW_ARGUMENTS, kwargs)
        return self._run()

    def send_event(self, event_name: str, payload: Any) -> WorkflowResult:
        self._record_event(
            next(self._events.counter(event_name)),
            payload
        )
        return self._run()


class WorkflowManager:
    def signal_workflow(self, workflow_id: str, event_name: str, payload: Any):
        """Sends the event to the indicated workflow"""

    def new_workflow(self, workflow_id: str, func: WorkflowFunction, *args, **kwargs):
        """Registers the function as a new workflow"""


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    INPUT_EVENT_NAME = 'input'


    class RegisterUserFlow:

        @event
        def display(self, text: str):
            print(text)

        @external_event(INPUT_EVENT_NAME)
        def get_input(self): ...

        def get_name(self):
            self.display('Name: ')
            return self.get_input()

        def get_student_id(self):
            self.display('Student ID: ')
            return self.get_input()

        def __call__(self, welcome_message):
            self.display(welcome_message)
            name = self.get_name()
            sid = self.get_student_id()
            self.display(f'Name: {name}, ID: {sid}')


    register_user = Workflow(RegisterUserFlow())
    register_user('Howdy')
    print('---')
    result = register_user.send_event(INPUT_EVENT_NAME, 'Foo')
    assert result is None
    print('---')
    result = register_user.send_event(INPUT_EVENT_NAME, '123')
    print('---')
    assert result is not None
    print(json.dumps(register_user._events._state, indent=2))
