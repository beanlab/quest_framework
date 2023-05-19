import shutil
import uuid
import pytest
from quest import *
from quest.workflow import *


@event
def do_something():
    print("did something")


class TestFlow:

    @event
    def store_text(self, text: str) -> str:
        return text

    def __call__(self, text):
        do_something()
        self.store_text(text)


def create_workflow_manager(flow_function, flow_function_name) -> WorkflowManager:
    saved_state = Path('saved-state')

    # Remove data from previous tests
    shutil.rmtree(saved_state, ignore_errors=True)

    workflow_manager = WorkflowManager(
        JsonMetadataSerializer(saved_state),
        JsonEventSerializer(saved_state / 'workflow_state'),
        {flow_function_name: StatelessWorkflowSerializer(flow_function)}
    )
    return workflow_manager


def get_workflow_id() -> str:
    return str(uuid.uuid4())


def test_event_stored_correctly():
    workflow_manager = create_workflow_manager(TestFlow, 'TestFlow')
    workflow_id = get_workflow_id()
    with workflow_manager:
        # start initial workflow
        result = workflow_manager.start_workflow(workflow_id, TestFlow(), 'Howdy')
        # make sure workflow __call__ returns None
        assert result is not None
        assert result.payload is None
        # make sure there is only one extra event (1: init args, 2: init kwargs, 3: Workflow result)
        assert len(workflow_manager.workflows.get(workflow_id)._events) == 4
        # assert that the one event's payload is 'Howdy'
        assert workflow_manager.workflows.get(workflow_id)._events['.store_text_0'] is not None
        assert workflow_manager.workflows.get(workflow_id)._events['.store_text_0']['payload'] == 'Howdy'
