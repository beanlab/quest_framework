import shutil
import uuid
import pytest
from quest import *
from quest.workflow import *

STOP_EVENT_NAME = 'stop'


class TestFlow:

    @event
    def store_text(self, text: str) -> str:
        return text

    def __call__(self, text):
        self.store_text(text)


class CountTestFlow:

    @event
    def count(self, counter: int) -> int:
        return counter + 1

    @external_event(STOP_EVENT_NAME)
    def stop(self): ...

    def __call__(self, counter):
        counter = self.count(counter)
        self.stop()
        return counter


class NestedEventFlow:

    @event
    def count_1(self, counter: int) -> int:
        return counter + 1

    @event
    def count_2(self, counter: int) -> int:
        counter = self.count_1(counter)
        return counter + 1

    @external_event(STOP_EVENT_NAME)
    def stop(self): ...

    def __call__(self):
        count_2 = self.count_2(0)
        count_1 = self.count_1(count_2)
        self.stop()
        return {"count_1": count_1, "count_2": count_2}


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
        assert workflow_manager.workflows.get(workflow_id)._events['store_text_0'] is not None
        assert workflow_manager.workflows.get(workflow_id)._events['store_text_0']['payload'] == 'Howdy'


def test_event_occurs_once():
    workflow_manager = create_workflow_manager(CountTestFlow, 'CountTestFlow')
    workflow_id = get_workflow_id()
    with workflow_manager:
        # start initial workflow
        result = workflow_manager.start_workflow(workflow_id, CountTestFlow(), 0)
        # make sure result is None, as it should stop, but should have recorded an event
        assert result is None
        assert workflow_manager.workflows.get(workflow_id)._events['count_0'] is not None
        assert workflow_manager.workflows.get(workflow_id)._events['count_0']['payload'] == 1
        # make sure the workflow is still running
        with pytest.raises(KeyError):
            assert workflow_manager.workflows.get(workflow_id)._events['WORKFLOW_RESULT']
        # restart the workflow with payload result from before, but pass in 1, because that should not change the result
        result = workflow_manager.signal_workflow(workflow_id, STOP_EVENT_NAME, 1)
        assert result is not None
        assert result.payload == 1
        # check that there is still only one event
        with pytest.raises(KeyError):
            assert workflow_manager.workflows.get(workflow_id)._events['count_1'] is None


def test_nested_event():
    workflow_manager = create_workflow_manager(NestedEventFlow, 'NestedEventFlow')
    workflow_id = get_workflow_id()
    with workflow_manager:
        result = workflow_manager.start_workflow(workflow_id, NestedEventFlow())
        # make sure result is None, as it should stop, but should have recorded both count_1 and count_2 events with correct values
        assert result is None
        assert workflow_manager.workflows.get(workflow_id)._events['count_1_0'] is not None
        assert workflow_manager.workflows.get(workflow_id)._events['count_1_0']['payload'] == 1
        assert workflow_manager.workflows.get(workflow_id)._events['count_2_0'] is not None
        assert workflow_manager.workflows.get(workflow_id)._events['count_2_0']['payload'] == 2
        # make sure the flow has not finished
        with pytest.raises(KeyError):
            assert workflow_manager.workflows.get(workflow_id)._events['WORKFLOW_RESULT']
        # restart the flow and make sure the count return values are correct
        result = workflow_manager.signal_workflow(workflow_id, STOP_EVENT_NAME, 1)
        assert result.payload['count_1'] == 3
        assert result.payload['count_2'] == 2
