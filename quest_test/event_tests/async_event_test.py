import uuid
import pytest
import asyncio
from quest.workflow import *
from quest.workflow_manager import *
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


class AsyncEventFlow:

    def __init__(self, workflow_manager):
        ...

    @step
    async def event_count(self):
        await asyncio.sleep(2)  # this is here as proof that the system still works when you await
        self.event_counter += 1
        return self.event_counter

    @signal(STOP_EVENT_NAME)
    async def stop(self): ...

    async def __call__(self):
        self.event_counter = 0
        event_count = await self.event_count()
        await self.stop()
        return {"event_count": event_count, "self_event_counter": self.event_counter}


@pytest.mark.asyncio
async def test_async_event(tmp_path):
    """
    This is a simple test showing the expected behavior of an event and a signal
    """
    workflow_manager = create_workflow_manager(AsyncEventFlow, "AsyncEventFlow", tmp_path)
    workflow_id = get_workflow_id()
    async with workflow_manager:
        result = await workflow_manager.start_workflow(workflow_id, "AsyncEventFlow")  # start workflow
        assert result is not None  # workflow calls stop signal, should return awaiting result. There should be one signal waiting
        assert result.status == Status.SUSPENDED
        assert len(result.signals) == 1
        result = await workflow_manager.signal_async_workflow(workflow_id, result.signals[0].unique_signal_name,
                                                              None)  # return stop signal to workflow
        assert result is not None  # workflow should now be complete and return the correct result
        assert result.status == Status.COMPLETED
        assert 1 == result.result["event_count"]  # event should only be called once
        assert 0 == result.result[
            'self_event_counter']  # event should be cached, and self_event_counter should not increment

    # going out of context deserializes the workflow
    # going back into context should serialize the workflow and run it once
    async with workflow_manager:
        result = workflow_manager.get_current_workflow_status(workflow_id)
        assert 1 == result.result[
            "event_count"]  # result should be the same as the last signal call, as it should all have been cached even through serialization
        assert 0 == result.result['self_event_counter']
        assert result.status == Status.COMPLETED
