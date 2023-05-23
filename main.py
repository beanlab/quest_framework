import json
import logging
import shutil
import uuid
from pathlib import Path

from quest import event
from quest.workflow import WorkflowManager, JsonEventSerializer, JsonMetadataSerializer, \
    StatelessWorkflowSerializer, signal_event

logging.basicConfig(level=logging.DEBUG)
INPUT_EVENT_NAME = 'input'


class RegisterUserFlow:

    @event
    def display(self, text: str):
        print(text)

    @signal_event(INPUT_EVENT_NAME)
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
    # shutil.rmtree(saved_state, ignore_errors=True)

    workflow_manager = WorkflowManager(
        JsonMetadataSerializer(saved_state),
        JsonEventSerializer(saved_state / 'workflow_state'),
        {'RegisterUserFlow': StatelessWorkflowSerializer(RegisterUserFlow)}
    )

    workflow_id = str(uuid.uuid4())

    with workflow_manager:
        result = workflow_manager.start_workflow(workflow_id, RegisterUserFlow(), 'Howdy')
        assert result is None

        print('---')
        result = workflow_manager.signal_workflow(workflow_id, INPUT_EVENT_NAME, "Foo")
        assert result is None

    with workflow_manager:
        print('---')
        result = workflow_manager.signal_workflow(workflow_id, INPUT_EVENT_NAME, '123')
        print('---')
        assert result is not None

        print(json.dumps(workflow_manager.workflows[workflow_id]._events._state, indent=2))
