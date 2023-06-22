import uuid
import pytest
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


@pytest.mark.asyncio
async def test_nested_event(tmp_path):
    """
    This test shows that when an event is called within an event, the events will cache correctly and return the correct value
    as expected.
    """
    workflow_manager = create_workflow_manager(NestedStepFlow, "NestedStepFlow", tmp_path)
    workflow_id = get_workflow_id()
    async with workflow_manager:
        result = await workflow_manager.start_async_workflow(workflow_id, "NestedStepFlow")
        # the workflow should be done, and we should have a result
        assert result is not None
        assert Status.COMPLETED == result.status
        # outer_event calls event_count once, should return 1
        assert 1 == result.result["outer_event_count"]
        # event_count called two times, should return 2
        assert 2 == result.result["event_count"]

    # going out of context deserializes the workflow
    # going back into context should serialize the workflow and run it once
    async with workflow_manager:
        # call a signal to rerun workflow, every event and signal should be cached and return a payload
        result = workflow_manager.get_workflow_status(workflow_id, None)
        # these three values should be the same as before we deserialized and reserialized
        assert 1 == result.result["outer_event_count"]
        assert 2 == result.result["event_count"]
        assert 0 == result.result['self_event_counter']
        assert Status.COMPLETED == result.status
