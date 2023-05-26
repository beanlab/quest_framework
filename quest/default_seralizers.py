import json
from pathlib import Path
from typing import Callable
from .workflow_manager import WorkflowSerializer, EventSerializer, WorkflowFunction, WorkflowMetadataSerializer
from .events import UniqueEvent, InMemoryEventManager


class StatelessWorkflowSerializer(WorkflowSerializer):
    def __init__(self, create_workflow: Callable[[], WorkflowFunction]):
        self.create_workflow = create_workflow

    def serialize_workflow(self, workflow_id: str, workflow: WorkflowFunction):
        """Nothing needed"""

    def deserialize_workflow(self, workflow_id: str) -> WorkflowFunction:
        return self.create_workflow()


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
