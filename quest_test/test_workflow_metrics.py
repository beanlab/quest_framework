import asyncio
import pytest

from quest.manager import WorkflowManager
from quest.persistence import InMemoryBlobStorage
from quest.serializer import NoopSerializer


@pytest.mark.asyncio
async def test_workflow_metrics_simple():
    storage = InMemoryBlobStorage()
    histories = {}

    def create_history(wid: str):
        if wid not in histories:
            histories[wid] = []
        return histories[wid]

    # Event to control the execution flow
    pause_event = asyncio.Event()

    async def sample_workflow():
        await pause_event.wait()
        return "Workflow Completed"

    # Events to track workflow completion - used due to callback
    done_event1 = asyncio.Event()
    done_event2 = asyncio.Event()

    async with WorkflowManager('test-manager', storage, create_history, lambda w_type: sample_workflow,
                               serializer=NoopSerializer()) as manager:
        manager.start_workflow_background('sample_workflow_type', 'wid1')
        manager.start_workflow_background('sample_workflow_type', 'wid2')

        # Callbacks when workflows are done
        manager.get_workflow('wid1').add_done_callback(lambda _: done_event1.set())
        manager.get_workflow('wid2').add_done_callback(lambda _: done_event2.set())

        metrics = manager.get_workflow_metrics()
        assert len(metrics) == 2

        workflow_ids = [metric['workflow_id'] for metric in metrics]
        assert 'wid1' in workflow_ids
        assert 'wid2' in workflow_ids

        # Allow workflows to proceed
        pause_event.set()

        # Wait for both workflows to be done
        await done_event1.wait()
        await done_event2.wait()

    metrics = manager.get_workflow_metrics()
    assert len(metrics) == 0
