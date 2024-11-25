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

    async def sample_workflow():
        await asyncio.sleep(0.1)
        return "Workflow Completed"

    async with WorkflowManager('test-manager', storage, create_history, lambda w_type: sample_workflow,
                               serializer=NoopSerializer()) as manager:
        # Start two workflows
        manager.start_workflow_background('sample_workflow_type', 'wid1')
        manager.start_workflow_background('sample_workflow_type', 'wid2')
        await asyncio.sleep(0.05)

        metrics = manager.get_workflow_metrics()
        assert len(metrics) == 2

        workflow_ids = [metric['workflow_id'] for metric in metrics]
        assert 'wid1' in workflow_ids
        assert 'wid2' in workflow_ids

        # Wait for workflows to complete
        await asyncio.sleep(0.3)

        # Metrics empty since all workflows have completed
        metrics = manager.get_workflow_metrics()
        assert len(metrics) == 0
