import json
import logging
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any, Protocol, Callable, TypeVar
import inspect

from .events import Event, UniqueEvent, EventManager, InMemoryEventManager

ARGUMENTS = "INITIAL_ARGUMENTS"
KW_ARGUMENTS = "INITIAL_KW_ARGUMENTS"
WORKFLOW_RESULT = "WORKFLOW_RESULT"


class WorkflowNotFoundException(Exception):
    pass


def find_frame():
    outer_frame = inspect.currentframe()
    is_workflow = False
    while not is_workflow:
        outer_frame = outer_frame.f_back
        if outer_frame is None:
            raise WorkflowNotFoundException("Workflow object not found in event stack")
        try:
            is_workflow = isinstance(outer_frame.f_locals['self'], Workflow)
        except KeyError as e:
            logging.debug(str(e) + ": outer_frame not called from a class")
    return outer_frame


def event(func):
    @wraps(func)
    def new_func(*args, **kwargs):
        return find_frame().f_locals['self'].run_event(func.__name__, func, *args, **kwargs)

    return new_func


def signal_event(func_or_name):
    if isinstance(func_or_name, str):
        def decorator(func):
            func.__event_name = func_or_name

            def new_func(*args, **kwargs):
                return find_frame().f_locals['self'].run_signal_event(func_or_name)

            return new_func

        return decorator
    else:
        def new_func(*args, **kwargs):
            return find_frame().f_locals['self'].run_signal_event(func_or_name.__name__)

        return new_func


class DuplicateWorkflowIDException(Exception):
    def __init__(self, workflow_id: str):
        super().__init__(f'Workflow id {workflow_id} already in use')


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

    def run_event(self, event_name, func, *args, **kwargs):
        prefixed_name = '.'.join(self.prefix) + '.' + event_name
        if prefixed_name not in self.unique_events:
            self.unique_events[prefixed_name] = UniqueEvent(prefixed_name)
            self._replay_events.append(self.unique_events[prefixed_name])
        _event_name = next(self.unique_events[prefixed_name])

        if _event_name in self._events:
            return self._events[_event_name]['payload']
        else:
            self.prefix.append(event_name)
            payload = func(*args, **kwargs)
            self.prefix.pop(-1)
            self._record_event(_event_name, payload)
            return payload

    def run_signal_event(self, event_name: str):
        logging.debug(f'Registering external event: {event_name}')
        prefixed_name = '.'.join(self.prefix) + '.' + event_name
        if prefixed_name not in self.unique_events:
            self.unique_events[prefixed_name] = UniqueEvent(event_name)
            self._replay_events.append(self.unique_events[prefixed_name])
        _event_name = next(self.unique_events[prefixed_name])
        return self._await_event(_event_name)

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

            if WORKFLOW_RESULT not in self._events:
                self._record_event(WORKFLOW_RESULT, result)

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
    def new_event_manager(self, workflow_id: str) -> EM: ...

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


class StatelessWorkflowSerializer(WorkflowSerializer):
    def __init__(self, create_workflow: Callable[[], WorkflowFunction]):
        self.create_workflow = create_workflow

    def serialize_workflow(self, workflow_id: str, workflow: WorkflowFunction):
        """Nothing needed"""

    def deserialize_workflow(self, workflow_id: str) -> WorkflowFunction:
        return self.create_workflow()


class WorkflowManager:
    RESUME_WORKFLOW = '__resume_workflow__'

    def __init__(
            self,
            metadata_serializer: WorkflowMetadataSerializer,
            event_serializer: EventSerializer[EM],
            workflow_serializers: dict[str, WorkflowSerializer],
    ):
        self.metadata_serializer = metadata_serializer
        self.event_serializer = event_serializer
        self.workflow_serializers = workflow_serializers

        self.workflows: dict[str, Workflow] = {}
        self.event_managers: dict[str, EventManager] = {}

    def signal_workflow(self, workflow_id: str, event_name: str, payload: Any) -> WorkflowResult:
        """Sends the event to the indicated workflow"""
        workflow = self.workflows[workflow_id]
        return workflow.send_event(event_name, payload)

    def start_workflow(self, workflow_id: str, func: WorkflowFunction, *args, **kwargs) -> WorkflowResult:
        """Registers the function as a new workflow"""
        if workflow_id in self.workflows:
            logging.error(f'Workflow ID {workflow_id} already in use')
            raise DuplicateWorkflowIDException(workflow_id)

        event_manager = self.event_serializer.new_event_manager(workflow_id)
        workflow = Workflow(
            workflow_id,
            func,
            event_manager=event_manager
        )
        self.event_managers[workflow_id] = event_manager
        self.workflows[workflow_id] = workflow
        return workflow(*args, **kwargs)

    def has_workflow(self, wid: str):
        return wid in self.workflows

    def __enter__(self):
        workflow_types = self.metadata_serializer.load()
        self._load_workflows(workflow_types)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._save_workflows()
        self.metadata_serializer.save(self._workflow_types())

    def _workflow_types(self) -> dict[str, str]:
        return {wid: workflow._workflow_type() for wid, workflow in self.workflows.items()}

    def _save_workflows(self):
        """
        Serialize the workflow event managers

        Returns a dict of the workflow IDs to workflow types
        """
        for wid, event_manager in self.event_managers.items():
            self.event_serializer.save_events(wid, event_manager)

        for wid, workflow in self.workflows.items():
            self.workflow_serializers[workflow._workflow_type()].serialize_workflow(wid, workflow)

    def _load_workflows(self, workflow_types: dict[str, str]):
        for wid, wtype in workflow_types.items():
            event_manager = self.event_serializer.load_events(wid)
            workflow_func = self.workflow_serializers[wtype].deserialize_workflow(wid)
            self.workflows[wid] = (workflow := Workflow(wid, workflow_func, event_manager))
            self.event_managers[wid] = event_manager
            workflow._run()


class JsonEventSerializer(EventSerializer[InMemoryEventManager]):
    def __init__(self, folder: Path):
        self.folder = folder
        if not self.folder.exists():
            self.folder.mkdir(parents=True)

    def new_event_manager(self, workflow_id: str):
        return InMemoryEventManager(workflow_id)

    def save_events(self, key: str, event_manager: InMemoryEventManager):
        file = key + '.json'
        with open(self.folder / file, 'w') as file:
            json.dump({
                'workflow_id': event_manager._workflow_id,
                'events': event_manager._state,
                'counters': {k: ue.to_json() for k, ue in event_manager._counters.items()}
            }, file)

    def load_events(self, key: str) -> InMemoryEventManager:
        file = key + '.json'
        with open(self.folder / file) as file:
            state = json.load(file)
            counters = {k: UniqueEvent(**ue) for k, ue in state['counters'].items()}
            return InMemoryEventManager(state['workflow_id'], state['events'], counters)


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
