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

    async with WorkflowManager('test-manager', storage, create_history, lambda w_type: sample_workflow,
                               serializer=NoopSerializer()) as manager:
        manager.start_workflow('sample_workflow_type', 'wid1')
        manager.start_workflow('sample_workflow_type', 'wid2')

        # Check the metrics immediately after starting the workflows
        metrics = manager.get_workflow_metrics()
        assert len(metrics) == 2

        workflow_ids = [metric['workflow_id'] for metric in metrics]
        assert 'wid1' in workflow_ids
        assert 'wid2' in workflow_ids

        # Allow workflows to proceed
        pause_event.set()

        # Workflow process done
        await manager.get_workflow('wid1')
        await manager.get_workflow('wid2')

        # Workflows in foreground not automatically removed
        manager._remove_workflow('wid1')
        manager._remove_workflow('wid2')

        metrics = manager.get_workflow_metrics()
        assert len(metrics) == 0
