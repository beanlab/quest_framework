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

    # Event to control the completion of workflows
    pause_event = asyncio.Event()

    async def sample_workflow():
        # Wait until pause_event is set before completing
        await pause_event.wait()
        return "Workflow Completed"

    async with WorkflowManager('test-manager', storage, create_history, lambda w_type: sample_workflow,
                               serializer=NoopSerializer()) as manager:
        manager.start_workflow('sample_workflow_type', 'wid1')
        manager.start_workflow_background('sample_workflow_type', 'wid2')

        # 2 active workflows in metrics
        metrics = manager.get_workflow_metrics()
        assert len(metrics) == 2, "Expected two active workflows"

        pause_event.set()

        await manager.get_workflow('wid1')

        # Event loop to run callbacks for the background workflow
        await asyncio.sleep(0)

        # Results for foreground workflow
        result_wid1 = manager.get_workflow_result('wid1', delete=False)
        assert result_wid1 == "Workflow Completed", "Expected 'wid1' to have its result stored."

        # Background workflow - no result should be stored, return None
        result_wid2 = manager.get_workflow_result('wid2', delete=False)
        assert result_wid2 is None, "Expected 'wid2' to have no result stored for background workflow."

        assert not manager.has_workflow('wid1'), "Expected 'wid1' to be inactive after completion."
        assert not manager.has_workflow('wid2'), "Expected 'wid2' to be inactive after completion."

        metrics = manager.get_workflow_metrics()
        assert len(metrics) == 0, "Expected no active workflows after completion."

        # Optionally, test that results for the foreground workflow can be deleted
        manager.get_workflow_result('wid1', delete=True)
        assert manager.get_workflow_result('wid1',
                                           delete=False) is None, "Expected 'wid1' result to be removed after deletion."
