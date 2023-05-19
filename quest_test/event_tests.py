import shutil
import uuid
import pytest
from quest import *
from quest.workflow import *

STOP_EVENT_NAME = 'stop'


def create_workflow_manager(flow_function, flow_function_name, path) -> WorkflowManager:
    saved_state = path / "saved-state"

    workflow_manager = WorkflowManager(
        JsonMetadataSerializer(saved_state),
        JsonEventSerializer(saved_state / 'workflow_state'),
        {flow_function_name: StatelessWorkflowSerializer(flow_function)}
    )
    return workflow_manager


def get_workflow_id() -> str:
    return str(uuid.uuid4())


class CalledOnceFlow:

    def __init__(self):
        self.call_count = 0

    @event
    def count(self):
        self.call_count += 1

    def __call__(self):
        self.count()


def test_event_called_once(tmp_path):
    EVENT_NAME = 'count'
    workflow_manager = create_workflow_manager(CalledOnceFlow, 'CalledOnceFlow', tmp_path)
    workflow_id = get_workflow_id()
    with workflow_manager:
        workflow_func = CalledOnceFlow()
        # start initial workflow
        result = workflow_manager.start_workflow(workflow_id, workflow_func)
        # make sure workflow __call__ returns None
        assert result is not None
        assert result.payload is None
        # signal workflow more times, each time count should be one
        assert workflow_func.call_count == 1
        for i in range(5):
            workflow_manager.signal_workflow(workflow_id, EVENT_NAME, None)
            assert workflow_func.call_count == 1


class CalledOnceExternalFlow:

    def __init__(self):
        self.call_count = 0

    @event
    def count(self):
        self.call_count += 1

    @external_event(STOP_EVENT_NAME)
    def stop(self): ...

    def __call__(self):
        self.count()
        self.stop()
        return self.call_count


def test_event_occurs_once_with_external_event(tmp_path):
    EVENT_NAME = 'stop'
    workflow_manager = create_workflow_manager(CalledOnceExternalFlow, 'CalledOnceExternalFlow', tmp_path)
    workflow_id = get_workflow_id()
    with workflow_manager:
        workflow_func = CalledOnceExternalFlow()
        # start initial workflow
        result = workflow_manager.start_workflow(workflow_id, workflow_func)
        # make sure workflow returns None because of stop call
        assert result is None
        # signal workflow more times, each time count should be one
        assert workflow_func.call_count == 1
        result = workflow_manager.signal_workflow(workflow_id, EVENT_NAME, None)
        assert workflow_func.call_count == 1
        # result of signal should not be none, but payload should be as it should finish
        assert result is not None


def test_event_occurs_once_when_killed(tmp_path):
    EVENT_NAME = 'stop'
    workflow_manager = create_workflow_manager(CalledOnceExternalFlow, 'CalledOnceExternalFlow', tmp_path)
    workflow_id = get_workflow_id()
    workflow_func = CalledOnceExternalFlow()
    with workflow_manager:
        # start initial workflow
        result = workflow_manager.start_workflow(workflow_id, workflow_func)
        # make sure workflow returns None because of stop call
        assert result is None
    # "kill" the function by exiting context, create new workflow_manger and restart
    workflow_manager = create_workflow_manager(CalledOnceExternalFlow, 'CalledOnceExternalFlow', tmp_path)
    with workflow_manager:
        # try to call same workflow, should get a duplicate workflow error
        with pytest.raises(DuplicateWorkflowIDException):
            workflow_manager.start_workflow(workflow_id, workflow_func)
        # signal workflow to resume, call count return should be 0, because count() should not be called
        result = workflow_manager.signal_workflow(workflow_id, EVENT_NAME, None)
        assert result is not None
        assert result.payload == 0


def test_nested_event():
    workflow_manager = create_workflow_manager(NestedEventFlow, 'NestedEventFlow')
    workflow_id = get_workflow_id()
    with workflow_manager:
        result = workflow_manager.start_workflow(workflow_id, NestedEventFlow())
        # make sure result is None, as it should stop, but should have recorded both count_1 and count_2 events with correct values
        assert result is None
        # make sure the flow has not finished
        with pytest.raises(KeyError):
            assert workflow_manager.workflows.get(workflow_id)._events['WORKFLOW_RESULT']
        # restart the flow and make sure the count return values are correct
        result = workflow_manager.signal_workflow(workflow_id, STOP_EVENT_NAME, 1)
        assert result.payload['count_1'] == 3
        assert result.payload['count_2'] == 2
