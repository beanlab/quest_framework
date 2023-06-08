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


class PromisedSignalEventFlow:

    def __init__(self, workflow_manager):
        ...

    @event
    async def event_count(self):
        self.event_counter += 1
        return self.event_counter

    @promised_signal(STOP_EVENT_NAME)
    async def stop(self): ...

    async def __call__(self):
        self.event_counter = 0
        event_count = await self.event_count()
        promises = [await self.stop() for i in range(0, 2)]
        [await promise.join() for promise in promises]
        return {"event_count": event_count, "self_event_counter": self.event_counter}


@pytest.mark.asyncio
async def test_promised_signal(tmp_path):
    """
    This is a simple test showing the expected behavior of an event and a signal
    """
    workflow_manager = create_workflow_manager(PromisedSignalEventFlow, "PromisedSignalEventFlow", tmp_path)
    workflow_id = get_workflow_id()
    async with workflow_manager:
        result = await workflow_manager.start_async_workflow(workflow_id, "PromisedSignalEventFlow")  # start workflow
        assert result is not None  # workflow promises stop signal twice, should return awaiting result with two signals
        assert result.status == Status.AWAITING_SIGNALS
        assert 2 == len(result.signals)
        result = await workflow_manager.signal_async_workflow(workflow_id, STOP_EVENT_NAME,
                                                              None)  # return stop signal to workflow once
        assert result is not None  # should still have an awaiting signal result with one signal left
        assert result.status == Status.AWAITING_SIGNALS
        assert 1 == len(result.signals)

    # now we will force a rehydrate by going out of context. There should still be one waiting signal that we will send
    async with workflow_manager:
        status = workflow_manager.get_current_workflow_status(workflow_id)
        assert status is not None  # should still have an awaiting signal result with one signal left
        assert result.status == Status.AWAITING_SIGNALS
        assert 1 == len(result.signals)
        result = await workflow_manager.signal_async_workflow(workflow_id, STOP_EVENT_NAME,
                                                              None)  # return stop signal to workflow a second time
        assert result is not None  # workflow should now be complete and return the correct result
        assert 1 == result.result["event_count"]  # event should only be called once
        assert 0 == result.result[
            'self_event_counter']  # event should be cached, and self_event_counter should not increment
        assert result.status == Status.COMPLETED

    # going out of context deserializes the workflow
    # going back into context should serialize the workflow and run it once
    async with workflow_manager:
        result = await workflow_manager.signal_async_workflow(workflow_id, OTHER_EVENT_NAME,
                                                              None)  # signal the workflow to rerun it
        assert 1 == result.result[
            "event_count"]  # result should be the same as the last signal call, as it should all have been cached even through serialization
        assert 0 == result.result['self_event_counter']
        assert result.status == Status.COMPLETED
