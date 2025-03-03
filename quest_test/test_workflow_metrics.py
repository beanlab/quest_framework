import asyncio
import pytest

from quest.manager import WorkflowNotFound
from .utils import timeout, create_in_memory_workflow_manager


async def sample_workflow():
    await asyncio.sleep(0.01)
    return "sample workflow result"


async def error_workflow():
    raise Exception("Intended failure")


async def paused_workflow(pause_event: asyncio.Event, done_event: asyncio.Event):
    await pause_event.wait()
    await asyncio.sleep(0.01)
    done_event.set()
    return "paused workflow result"


@pytest.mark.asyncio
@timeout(6)
async def test_three_workflows_and_check_result():
    workflows = {
        "sample_workflow": sample_workflow
    }

    manager = create_in_memory_workflow_manager(workflows=workflows)

    async with manager:
        manager.start_workflow('sample_workflow', 'wid1', delete_on_finish=False)
        manager.start_workflow('sample_workflow', 'wid2', delete_on_finish=False)
        manager.start_workflow('sample_workflow', 'wid3')

        metrics = manager.get_workflow_metrics()
        assert len(metrics) == 3

        # Retrieve result for wid1 and check if it's removed
        result_wid1 = await manager.get_workflow_result('wid1')
        assert result_wid1 is not None
        assert not manager.has_workflow('wid1')

        await asyncio.sleep(0.1)

        # Retrieve result for wid2 and check if it's removed
        result_wid2 = await manager.get_workflow_result('wid2')
        assert result_wid2 is not None
        assert not manager.has_workflow('wid2')

        await asyncio.sleep(0.1)

        # Check wid3 has been removed
        assert not manager.has_workflow('wid3')

        if manager.has_workflow('wid3'):
            await manager.delete_workflow('wid3')

        with pytest.raises(WorkflowNotFound):
            await manager.get_workflow_result('wid3')

        metrics = manager.get_workflow_metrics()
        assert len(metrics) == 0


@pytest.mark.asyncio
@timeout(6)
async def test_exception_handling():
    workflows = {
        "error_workflow": error_workflow
    }
    manager = create_in_memory_workflow_manager(workflows=workflows)

    async with manager:
        manager.start_workflow('error_workflow', 'wid1')

        # Retrieving the result raises an exception
        with pytest.raises(Exception):
            await manager.get_workflow_result("wid1")

        # Check the workflow is removed after failure
        assert not manager.has_workflow("wid1")


@pytest.mark.asyncio
@timeout(6)
async def test_workflow_deletion():
    workflows = {
        "sample_workflow": sample_workflow
    }
    manager = create_in_memory_workflow_manager(workflows=workflows)

    async with manager:
        manager.start_workflow('sample_workflow', 'wid1')
        assert manager.has_workflow('wid1')

        # Delete the workflow and check the removal
        await manager.delete_workflow('wid1')
        assert not manager.has_workflow('wid1')

        with pytest.raises(WorkflowNotFound):
            await manager.get_workflow_result('wid1')

        # Start another workflow but don't delete its result immediately
        manager.start_workflow('sample_workflow', 'wid2', delete_on_finish=False)
        done_result = await manager.get_workflow_result('wid2')
        assert done_result is not None

        if manager.has_workflow('wid2'):
            await manager.delete_workflow('wid2')

        await manager.get_workflow_result('wid2', delete=True)

        with pytest.raises(WorkflowNotFound):
            await manager.get_workflow_result('wid2')

        # Trying to delete a non-existing workflow raises WorkflowNotFound as well
        with pytest.raises(WorkflowNotFound):
            await manager.delete_workflow('not_existing')


@pytest.mark.asyncio
@timeout(6)
async def test_workflow_cancellation():
    workflows = {
        "sample_workflow": sample_workflow
    }
    manager = create_in_memory_workflow_manager(workflows=workflows)

    async with manager:
        manager.start_workflow('sample_workflow', 'wid1')
        await asyncio.sleep(0.01)

        # Cancel the workflow
        await manager.delete_workflow('wid1')
        assert not manager.has_workflow('wid1')

        with pytest.raises(WorkflowNotFound):
            await manager.get_workflow_result('wid1')
