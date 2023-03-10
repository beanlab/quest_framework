import json
import logging
from pathlib import Path

from quest import event, external_event
from quest.events import InMemoryEventManager
from quest.workflow import WorkflowManager, JsonEventSerializer, RegisterUserFlowSerializer

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
    workflow_manager = WorkflowManager(
        InMemoryEventManager,
        JsonEventSerializer(Path('saved-state')),
        {str(type(RegisterUserFlow)): RegisterUserFlowSerializer()}
    )

    register_user = workflow_manager.new_workflow("123", RegisterUserFlow())

    register_user('Howdy')
    print('---')
    result = register_user.send_event(INPUT_EVENT_NAME, 'Foo')
    assert result is None
    print('---')
    result = register_user.send_event(INPUT_EVENT_NAME, '123')
    print('---')
    assert result is not None

    # TODO - fix serialization
    # When saving a workflow by type, get the type of the function, not Workflow
    # workflow_manager.save_workflows()

    print(json.dumps(register_user._events._state, indent=2))
