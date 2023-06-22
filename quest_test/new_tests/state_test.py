import pytest
from quest.workflow import *
from quest.json_seralizers import *

VISIBLE = 'VISIBLE'
NOT_VISIBLE = 'NOT_VISIBLE'
VISIBLE_BY_ID = 'VISIBLE_BY_ID'
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


class StateFlow:

    def __init__(self, workflow_manager):
        ...

    @step
    async def event_step(self):
        self.event_counter += 1
        return self.event_counter

    async def __call__(self):
        self.event_counter = 0
        async with await state(NOT_VISIBLE, NOT_VISIBLE) as not_visible:
            print("Testing that the state NOT_VISIBLE is not able to be read out of context")
        await state(VISIBLE, VISIBLE)
        await state(VISIBLE_BY_ID, VISIBLE_BY_ID, ID)


def find_in_workflow_result(result: WorkflowStatus, value: str) -> dict | None:
    for state_entry in result.state.values():
        if state_entry['name'] == value:
            return state_entry['value']
    return None


@pytest.mark.asyncio
async def test_state(tmp_path):
    """
    This test shows that state works as intended
    """
    workflow_manager = create_workflow_manager(StateFlow, "StateFlow", tmp_path)
    workflow_id = get_workflow_id()
    async with workflow_manager:
        result = await workflow_manager.start_workflow(workflow_id, "StateFlow")
        # the workflow should be done, and we should have a result
        assert result is not None
        assert result.status == Status.COMPLETED
        # should only have visible state
        assert find_in_workflow_result(result, VISIBLE) == VISIBLE
        assert find_in_workflow_result(result, NOT_VISIBLE) is None
        assert find_in_workflow_result(result, VISIBLE_BY_ID) is None
        # get status with identification
        status = workflow_manager.get_status(workflow_id, ID)
        assert find_in_workflow_result(status, VISIBLE_BY_ID) == VISIBLE_BY_ID
        
    # going out of context deserializes the workflow
    async with workflow_manager:
        # get status of rehydrated workflow
        result = workflow_manager.get_status(workflow_id)
        # the workflow should be done, and we should have a result
        assert result is not None
        assert result.status == Status.COMPLETED
        # should only have visible state
        assert find_in_workflow_result(result, VISIBLE) == VISIBLE
        assert find_in_workflow_result(result, NOT_VISIBLE) is None
        assert find_in_workflow_result(result, VISIBLE_BY_ID) is None