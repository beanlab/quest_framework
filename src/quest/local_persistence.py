import json
from pathlib import Path
from typing import Callable, Any, TypeVar

from src.quest.historian import History
from src.quest.lifecycle import HistoryFactory
from .events import InMemoryEventManager, UniqueEvent
from .workflow_manager import EventSerializer, WorkflowMetadataSerializer, \
    WorkflowManager

WT = TypeVar('WT')


class JsonHistoryFactory(HistoryFactory):
    def __init__(self, root_folder: Path):
        self._root_folder = root_folder

    def create_history(self, workflow_id) -> History:
        return

    def load_history(self, workflow_id) -> History: ...

    def save_history(self, workflow_id, history: History): ...


class JsonEventSerializer(EventSerializer[InMemoryEventManager]):
    def __init__(self, folder: Path, from_dict: Callable[[dict], Any] = lambda d: d):
        self.folder = folder
        self.from_dict = from_dict
        if not self.folder.exists():
            self.folder.mkdir(parents=True)

    def new_event_manager(self, workflow_id: str):
        return InMemoryEventManager(workflow_id)

    def save_events(self, key: str, event_manager: InMemoryEventManager):
        file = key + '.json'
        with open(self.folder / file, 'w') as file:
            json.dump({
                'workflow_id': event_manager.workflow_id,
                'events': {
                    key: (value.to_json() if hasattr(value, 'to_json') else value)
                    for key, value in event_manager.items()
                }
            }, file)

    def load_events(self, key: str) -> InMemoryEventManager:
        file = key + '.json'
        with open(self.folder / file) as file:
            state = json.load(file)
            state['events'] = {k: self.from_dict(v) for k, v in state['events'].items()}
            return InMemoryEventManager(state['workflow_id'], state['events'])


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


def get_local_workflow_manager(save_folder: Path, workflow_function):
    return WorkflowManager(
        JsonMetadataSerializer(save_folder),
        JsonEventSerializer(save_folder / 'workflow_steps'),
        JsonEventSerializer(save_folder / 'workflow_state'),
        JsonEventSerializer(save_folder / 'workflow_queues'),
        JsonEventSerializer(save_folder / 'workflow_unique_ids', lambda d: UniqueEvent(**d)),
        {workflow_function.__name__: StatelessWorkflowSerializer(lambda: workflow_function)}
    )


class NoOpMetadataSerializer(WorkflowMetadataSerializer):
    def save(self, workflow_metadata: dict):
        pass

    def load(self) -> dict:
        return {}


class NoOpEventSerializer(EventSerializer):
    def new_event_manager(self, workflow_id: str):
        return InMemoryEventManager(workflow_id)

    def save_events(self, key: str, event_manager):
        pass

    def load_events(self, key: str):
        raise NotImplementedError()


def get_inmemory_workflow_manager(workflow_function) -> WorkflowManager:
    return WorkflowManager(
        NoOpMetadataSerializer(),
        NoOpEventSerializer(),
        NoOpEventSerializer(),
        NoOpEventSerializer(),
        NoOpEventSerializer(),
        {workflow_function.__name__: StatelessWorkflowSerializer(lambda: workflow_function)}
    )


if __name__ == '__main__':
    pass
