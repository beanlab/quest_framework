import pytest

from quest import step
from quest.workflow import *
from quest.json_seralizers import *

STOP_EVENT_NAME = 'stop'
OTHER_EVENT_NAME = 'other_event'


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


class NestedStepFlow:

    def __init__(self, workflow_manager):
        ...

    @step
    async def outer_step(self):
        return await self.event_step()

    @step
    async def event_step(self):
        self.event_counter += 1
        return self.event_counter

    async def __call__(self):
        self.event_counter = 0
        outer_event_count = await self.outer_step()
        event_count = await self.event_step()  # this is proof that events in different scope will cache correctly
        return {"event_count": event_count, "self_event_counter": self.event_counter,
                "outer_event_count": outer_event_count}


def find_workflow_result(result: WorkflowStatus) -> dict:
    for state_entry in result.state.values():
        if state_entry['name'] == 'result':
            return state_entry['value']


@pytest.mark.asyncio
async def test_nested_event(tmp_path):
    """
    This test shows that when an event is called within an event, the events will cache correctly and return the correct value
    as expected.
    """
    workflow_manager = create_workflow_manager(NestedStepFlow, "NestedStepFlow", tmp_path)
    workflow_id = get_workflow_id()
    async with workflow_manager:
        result = await workflow_manager.start_workflow(workflow_id, "NestedStepFlow")
        # the workflow should be done, and we should have a result
        assert result is not None
        assert Status.COMPLETED == result.status
        # find result of the workflow
        wf_result = find_workflow_result(result)
        # outer_event calls event_count once, should return 1
        assert 1 == wf_result["outer_event_count"]
        # event_count called two times, should return 2
        assert 2 == wf_result["event_count"]

    # going out of context deserializes the workflow
    async with workflow_manager:
        # get status of rehydrated workflow
        result = workflow_manager.get_status(workflow_id)
        # find wf result
        wf_result = find_workflow_result(result)
        # these three values should be the same as before we deserialized and reserialized
        assert 1 == wf_result["outer_event_count"]
        assert 2 == wf_result["event_count"]
        assert Status.COMPLETED == result.status
