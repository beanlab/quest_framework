import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol, Callable, TypeVar

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

    def __init__(self, workflow_id: str, func: WorkflowFunction, event_manager: EventManager = None):
        self.workflow_id = workflow_id
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


EM = TypeVar('EM', bound=EventManager)


class WorkflowMetadataSerializer(Protocol):
    """
    Stores and retrieves workflow metadata.
    There should be a unique WorkflowMetadataSerializer for each WorkflowManager
    """

    def save(self, workflow_metadata: dict): ...

    def load(self) -> dict: ...


class EventSerializer(Protocol[EM]):
    def save_events(self, key: str, event_manager: EM): ...

    def load_events(self, key: str) -> EM: ...


class WorkflowSerializer(Protocol):
    """
    The WorkflowSerializer handles the logic needed to save and load complex dependencies
        in the workflow objects.

    For example, if a workflow has stateful dependencies, such as an API client,
        then the WorkflowSerializer is responsible for saving the information necessary
        to recreate the client from data, as well as recreating the workflow from that data.

    The WorkflowManager will save/load all the special workflow data (status, step, etc.)
    """

    def serialize_workflow(self, workflow_id: str, workflow: WorkflowFunction):
        """
        Serializes the data necessary to rehydrate the workflow object.

        :param workflow_id: The ID of the workflow
        :param workflow: The workflow object to be saved.
        """

    def deserialize_workflow(self, workflow_id: str) -> WorkflowFunction:
        """
        Recreate the workflow object that was associated with the workflow ID.

        :param workflow_id: Unique string identifying the workflow to be recreated
        :return: The WorkflowFunction associated with the workflow ID
        """


class WorkflowManager:
    RESUME_WORKFLOW = '__resume_workflow__'

    def __init__(
            self,
            create_event_manager: Callable[[], EM],
            metadata_serializer: WorkflowMetadataSerializer,
            event_serializer: EventSerializer[EM],
            workflow_serializers: dict[str, WorkflowSerializer],
    ):
        self.create_event_manager = create_event_manager
        self.metadata_serializer = metadata_serializer
        self.event_serializer = event_serializer
        self.workflow_serializers = workflow_serializers
        self.workflows: dict[str, Workflow] = {}
        self.event_managers: dict[str, EventManager] = {}

    def signal_workflow(self, workflow_id: str, event_name: str, payload: Any) -> WorkflowResult:
        """Sends the event to the indicated workflow"""
        workflow = self.workflows[workflow_id]
        return workflow.send_event(event_name, payload)

    def new_workflow(self, workflow_id: str, func: WorkflowFunction) -> Workflow:
        """Registers the function as a new workflow"""
        event_manager = self.create_event_manager()
        workflow = Workflow(
            workflow_id,
            func,
            event_manager=event_manager
        )
        self.event_managers[workflow_id] = event_manager
        self.workflows[workflow_id] = workflow
        return workflow

    def __enter__(self):
        workflow_types = self.metadata_serializer.load()
        self._load_workflows(workflow_types)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._save_workflows()

    def _save_workflows(self) -> dict[str, str]:
        """
        Serialize the workflow event managers

        Returns a dict of the workflow IDs to workflow types
        """
        for wid, event_manager in self.event_managers.items():
            self.event_serializer.save_events(wid, event_manager)

        for wid, workflow in self.workflows.items():
            self.workflow_serializers[workflow._func.__class__.__name__].serialize_workflow(wid, workflow)

        return {wid: str(type(workflow)) for wid, workflow in self.workflows.items()}

    def _load_workflows(self, workflow_types: dict[str, str]):
        for wid, wtype in workflow_types.items():
            event_manager = self.event_serializer.load_events(wid)
            workflow_func = self.workflow_serializers[wtype].deserialize_workflow(wid)
            self.workflows[wid] = (workflow := Workflow(wid, workflow_func, event_manager))
            self.event_managers[wid] = event_manager
            workflow.send_event(WorkflowManager.RESUME_WORKFLOW, None)


class JsonEventSerializer(EventSerializer[InMemoryEventManager]):
    def __init__(self, folder: Path):
        self.folder = folder
        if not self.folder.exists():
            self.folder.mkdir(parents=True)

    def save_events(self, key: str, event_manager: InMemoryEventManager):
        file = key + '.json'
        with open(self.folder / file, 'w') as file:
            json.dump(event_manager._state, file)

    def load_events(self, key: str) -> InMemoryEventManager:
        file = key + '.json'
        with open(self.folder / file) as file:
            state = json.load(file)
            return InMemoryEventManager(state)


class JsonMetadataSerializer(WorkflowMetadataSerializer):
    def __init__(self, folder: Path):
        self.folder = folder

    def save(self, workflow_metadata: dict):
        with open(self.folder / "workflow_metadata.json", 'w') as file:
            json.dump(workflow_metadata, file)

    def load(self) -> dict:
        meta_file = self.folder / "workflow_metadata.json"
        if not meta_file.exists():
            return {}
        with open(meta_file) as file:
            return json.load(file)


if __name__ == '__main__':
    pass
