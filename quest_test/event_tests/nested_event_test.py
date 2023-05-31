import uuid
import pytest
import asyncio
from quest.workflow_manager import *
from quest.workflow import *
from quest.json_seralizers import *

STOP_EVENT_NAME = 'stop'
OTHER_EVENT_NAME = 'other_event'


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


class NestedEventFlow:

    @event
    async def outer_event(self):
        return await self.event_count()

    @event
    async def event_count(self):
        await asyncio.sleep(2)  # this is here as proof that the system still works when you await
        self.event_counter += 1
        return self.event_counter

    @signal(STOP_EVENT_NAME)
    async def stop(self): ...

    async def __call__(self):
        self.event_counter = 0
        outer_event_count = await self.outer_event()
        event_count = await self.event_count()  # this is proof that events in different scope will cache correctly
        await self.stop()
        return {"event_count": event_count, "self_event_counter": self.event_counter,
                "outer_event_count": outer_event_count}


@pytest.mark.asyncio
async def test_nested_event(tmp_path):
    """
    This test shows that when an event is called within an event, the events will cache correctly and return the correct value
    as expected.
    """
    workflow_manager = create_workflow_manager(NestedEventFlow, "NestedEventFlow", tmp_path)
    workflow_id = get_workflow_id()
    workflow_func = NestedEventFlow()
    async with workflow_manager:
        result = await workflow_manager.start_async_workflow(workflow_id, workflow_func)
        assert result is not None  # should be stopped by stop signal call, be status awaiting signal
        assert Status.AWAITING_SIGNAL == result.status
        result = await workflow_manager.signal_async_workflow(workflow_id, STOP_EVENT_NAME, None)  # signal the workflow to return stop signal
        assert result is not None  # now the workflow should be done, and we should have a result
        assert 1 == result.result["outer_event_count"]  # outer_event calls event_count once, should return 1
        assert 2 == result.result["event_count"]  # event_count called two times, should return 2
        assert 0 == result.result['self_event_counter']  # because it was replayed, self_event_count should be zero as all event_count calls should have been cached on the stop signals
        assert Status.COMPLETED == result.status

    # going out of context deserializes the workflow
    # going back into context should serialize the workflow and run it once
    async with workflow_manager:
        result = await workflow_manager.signal_async_workflow(workflow_id, OTHER_EVENT_NAME, None)  # call a signal to rerun workflow, every event and signal should be cached and return a payload
        assert 1 == result.result["outer_event_count"]  # these three values should be the same as before we deserialized and reserialized
        assert 2 == result.result["event_count"]
        assert 0 == result.result['self_event_counter']
        assert Status.COMPLETED == result.status

