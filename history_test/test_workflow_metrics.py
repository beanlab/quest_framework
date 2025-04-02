import asyncio

import pytest

from history.historian import WorkflowNotFound
from .utils import timeout, create_in_memory_historian


@pytest.mark.asyncio
@timeout(6)
async def test_workflow_metrics():
    gate1 = asyncio.Event()

    async def workflow1():
        await gate1.wait()
        return "done"

    gate2 = asyncio.Event()

    async def workflow2():
        await gate2.wait()
        return "done"

    async with create_in_memory_historian({'w1': workflow1, 'w2': workflow2}) as manager:
        manager.start_soon('w1', 'wid1', delete_on_finish=False)
        manager.start_soon('w2', 'wid2')

        # Scheduled workflows are counted as "running"
        assert len(manager.get_metrics()) == 2

        # Yield to let the workflows start
        await asyncio.sleep(0.1)

        # Let wf1 finish (but it doesn't self-delete)
        gate1.set()
        await asyncio.sleep(0.1)

        # Finished workflows with a stored result are still "running"
        assert len(manager.get_metrics()) == 2

        # Allow wid1 to return result and clean up
        await manager.get_result('wid1', delete=True)

        assert len(manager.get_metrics()) == 1

        gate2.set()
        await asyncio.sleep(0.1)

        assert len(manager.get_metrics()) == 0


@pytest.mark.asyncio
@timeout(6)
async def test_workflow_deletion():
    async def sample_workflow():
        return "sample workflow result"

    workflows = {
        "sample_workflow": sample_workflow
    }
    manager = create_in_memory_historian(workflows=workflows)

    async with manager:
        # Start workflow but don't delete its result immediately
        manager.start_soon('sample_workflow', 'wid2', delete_on_finish=False)
        future_wid2 = await manager.get_result('wid2')
        await asyncio.sleep(0.1)
        assert future_wid2 is not None

        await manager.delete_workflow('wid2')

        with pytest.raises(WorkflowNotFound):
            await manager.get_result('wid2')

        # Trying to delete a non-existing workflow raises WorkflowNotFound as well
        with pytest.raises(WorkflowNotFound):
            await manager.delete_workflow('not_existing')


@pytest.mark.asyncio
@timeout(6)
async def test_workflow_cancellation():
    gate = asyncio.Event()

    async def long_running_workflow():
        await gate.wait()
        return "should not reach"

    manager = create_in_memory_historian(workflows={"long_workflow": long_running_workflow})

    async with manager:
        manager.start_soon('long_workflow', 'wid1')
        await asyncio.sleep(0.1)

        # Cancel the running workflow
        assert manager.has('wid1')
        await manager.delete_workflow('wid1')
        await asyncio.sleep(0.1)

        assert not manager.has('wid1')
        with pytest.raises(WorkflowNotFound):
            await manager.get_result('wid1')
