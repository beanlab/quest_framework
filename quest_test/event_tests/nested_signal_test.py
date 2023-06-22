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


class NestedSignalFlow:

    def __init__(self, workflow_manager):
        ...

    @step
    async def outer_event(self):
        self.event_counter = await self.event_count()  # this proves that outer_event will be replayed when the stop signal is sent the first time
        await self.stop()  # this proves that you can nest signals in an event, and it will work as expected
        count = await self.event_count()  # this proves that execution will continue correctly after stop signal is returned
        return count

    @step
    async def event_count(self):
        self.event_counter += 1
        return self.event_counter

    @signal(STOP_EVENT_NAME)
    async def stop(self): ...

    async def __call__(self):
        self.event_counter = 0
        outer_event_count = await self.outer_event()
        event_count = await self.event_count()  # proves that events in different scope works as expected
        await self.stop()
        return {"event_count": event_count, "self_event_counter": self.event_counter,
                "outer_event_count": outer_event_count}


@pytest.mark.asyncio
async def test_nested_signal(tmp_path):
    """
    This test shows that when a signal is called in an event, the behavior works as expected: the code will resume
    inside outer event when the stop signal is returned to the workflow
    """
    workflow_manager = create_workflow_manager(NestedSignalFlow, "NestedSignalFlow", tmp_path)
    workflow_id = get_workflow_id()
    workflow_func = "NestedSignalFlow"
    async with workflow_manager:
        result = await workflow_manager.start_workflow(workflow_id, workflow_func)  # start the workflow
        assert result is not None  # code should be stopped with a signal, return with status awaiting signal
        assert Status.SUSPENDED == result.status
        assert 1 == len(result.signals)
        result = await workflow_manager.signal_async_workflow(workflow_id, result.signals[0].unique_signal_name, None)  # signal the workflow for the first stop
        assert result is not None  # code should be stopped with a signal, return with status awaiting signal
        assert Status.SUSPENDED == result.status
        assert 1 == len(result.signals)
        result = await workflow_manager.signal_async_workflow(workflow_id, result.signals[0].unique_signal_name, None)  # signal the workflow for the second stop
        assert result is not None  # now the workflow should be done, and we should have a result
        assert 2 == result.result["outer_event_count"]  # outer_event calls event_count twice, should return 2
        assert 3 == result.result["event_count"]  # event_count called three times, should return 3
        assert 0 == result.result['self_event_counter']  # because it was replayed, self_event_count should be zero as all event_count calls should have been cached with the stop calls
        assert Status.COMPLETED == result.status

    # going out of context deserializes the workflow
    # going back into context should serialize the workflow and run it once
    async with workflow_manager:
        result = workflow_manager.get_current_workflow_status(workflow_id)  # call a signal to rerun workflow, every event and signal should be cached and return a payload
        assert 2 == result.result["outer_event_count"]  # result should be the same as before we deserialized and reserialized
        assert 3 == result.result["event_count"]
        assert 0 == result.result['self_event_counter']
        assert Status.COMPLETED == result.status
