import pytest

from quest import queue
from quest.workflow import *
from quest.json_seralizers import *

ID = "ID"


def create_workflow_manager(flow_function, flow_function_name, path) -> WorkflowManager:
    saved_state = path / "saved-state"

    workflow_manager = WorkflowManager(
        JsonMetadataSerializer(saved_state),
        JsonEventSerializer(saved_state / 'step_state'),
        JsonEventSerializer(saved_state / 'state_state'),
        JsonEventSerializer(saved_state / 'queue_state'),
        {flow_function_name: StatelessWorkflowSerializer(flow_function)}
    )
    return workflow_manager


def get_workflow_id() -> str:
    return str(uuid.uuid4())


class QueueFlow:

    def __init__(self, workflow_manager):
        ...

    async def __call__(self):
        self.event_counter = 0
        async with await queue('queue1') as queue1:
            # pop should suspend until we have a value
            await queue1.pop()
        # now we shouldn't be able to push to queue1
        queue2 = await queue('queue2', ID)
        identity, value = await queue2.pop()
        return {'identity': identity, 'value': value}


def find_queue_id(result: WorkflowStatus, value: str) -> str | None:
    for queue_entry in result.queues.values():
        if queue_entry['name'] == value:
            return queue_entry['queue_id']
    return None


def find_state_value(result: WorkflowStatus, value: str) -> dict | None:
    for state_entry in result.state.values():
        if state_entry['name'] == value:
            return state_entry['value']
    return None


@pytest.mark.asyncio
async def test_state(tmp_path):
    """
    This test shows that state works as intended
    """
    workflow_manager = create_workflow_manager(QueueFlow, "StateFlow", tmp_path)
    workflow_id = get_workflow_id()
    async with workflow_manager:
        result = await workflow_manager.start_workflow(workflow_id, "StateFlow")
        # the workflow should be suspended
        assert result is not None
        assert result.status == Status.SUSPENDED
        # queue1 should be waiting, push to value to queue1
        assert find_queue_id(result, 'queue1') is not None
        await workflow_manager.push_queue(workflow_id, find_queue_id(result, 'queue1'), "push_to_queue")
        # get status. Should have another waiting queue only for identity "ID", and shouldn't have queue1 because of context
        status = workflow_manager.get_status(workflow_id, None)
        assert find_queue_id(status, 'queue2') is None
        assert find_queue_id(status, 'queue1') is None
        status = workflow_manager.get_status(workflow_id, ID)
        assert find_queue_id(status, 'queue2') is not None
        # push to queue2, workflow should now be complete
        await workflow_manager.push_queue(workflow_id, find_queue_id(status, 'queue2'), "push_to_queue", ID)
        status = workflow_manager.get_status(workflow_id, ID)
        assert status.status == Status.COMPLETED
        assert find_queue_id(status, 'queue2') is not None
        assert find_state_value(result, 'result')['identity'] == ID
        assert find_state_value(result, 'result')['value'] == 'push_to_queue'

    # going out of context deserializes the workflow
    async with workflow_manager:
        # get status of rehydrated workflow
        result = workflow_manager.get_status(workflow_id)
        # the workflow should be done, and we should have a result
        assert result is not None
        assert result.status == Status.COMPLETED
        assert find_state_value(result, 'result')['identity'] == ID
        assert find_state_value(result, 'result')['value'] == 'push_to_queue'
