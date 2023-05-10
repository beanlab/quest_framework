import json
import logging
import shutil
from pathlib import Path

from quest import event, external_event
from quest.events import InMemoryEventManager
from quest.workflow import WorkflowManager, JsonEventSerializer, WorkflowSerializer, JsonMetadataSerializer, \
    WorkflowFunction, StatelessWorkflowSerializer

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


if __name__ == '__main__':
    saved_state = Path('saved-state')

    # Remove data
    shutil.rmtree(saved_state, ignore_errors=True)
    saved_state.mkdir(exist_ok=True, parents=True)

    workflow_manager = WorkflowManager(
        InMemoryEventManager,
        JsonMetadataSerializer(saved_state),
        JsonEventSerializer(saved_state),
        {'RegisterUserFlow': StatelessWorkflowSerializer(RegisterUserFlow)}
    )

    with workflow_manager:
        result = workflow_manager.start_workflow("123", RegisterUserFlow(), 'Howdy')
        assert result is None

        print('---')
        result = workflow_manager.signal_workflow("123", INPUT_EVENT_NAME, "Foo")
        assert result is None

    with workflow_manager:
        print('---')
        result = workflow_manager.signal_workflow("123", INPUT_EVENT_NAME, '123')
        print('---')
        assert result is not None

        print(json.dumps(workflow_manager.workflows["123"]._events._state, indent=2))
