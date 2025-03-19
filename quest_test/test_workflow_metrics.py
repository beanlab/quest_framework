import asyncio

import pytest

from quest.manager import WorkflowNotFound
from .utils import timeout, create_in_memory_workflow_manager


async def sample_workflow():
    return "sample workflow result"


async def error_workflow():
    raise Exception("Intended failure")


async def pause_resume_workflow(pause_event: asyncio.Event):
    await pause_event.wait()
    return "resumed successfully"


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

    async with create_in_memory_workflow_manager({'w1': workflow1, 'w2': workflow2}) as manager:
        manager.start_workflow('w1', 'wid1', delete_on_finish=False)
        manager.start_workflow('w2', 'wid2')

        # Scheduled workflows are counted as "running"
        assert len(manager.get_workflow_metrics()) == 2

        # Yield to let the workflows start
        await asyncio.sleep(0.01)

        # Let wf1 finish (but it doesn't self-delete)
        gate1.set()
        await asyncio.sleep(0.01)

        # Finished workflows with a stored result are still "running"
        assert len(manager.get_workflow_metrics()) == 2

        # Allow wid1 to return result and clean up
        await manager.get_workflow_result('wid1', delete=True)

        assert len(manager.get_workflow_metrics()) == 1

        gate2.set()
        await asyncio.sleep(0.01)

        assert len(manager.get_workflow_metrics()) == 0


@pytest.mark.asyncio
@timeout(6)
async def test_three_workflows_and_check_result():
    workflows = {
        "sample_workflow": sample_workflow
    }

    async with create_in_memory_workflow_manager(workflows=workflows) as manager:
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
        future_wid2 = await manager.get_workflow_result('wid2')
        result_wid2 = await future_wid2
        await asyncio.sleep(0.1)
        assert result_wid2 is not None
        assert not manager.has_workflow('wid2')

        await asyncio.sleep(0.1)

        # Check wid3 has been removed
        assert not manager.has_workflow('wid3')

        if manager.has_workflow('wid3'):
            await manager.delete_workflow('wid3')

        with pytest.raises(WorkflowNotFound):
            future_wid3 = await manager.get_workflow_result('wid3')
            await future_wid3

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
            future = await manager.get_workflow_result("wid1")
            await future

        # Check the workflow is removed after failure
        await asyncio.sleep(0.1)
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

        await asyncio.sleep(0.1)

        # Delete the workflow and check the removal
        assert not manager.has_workflow('wid1')

        with pytest.raises(WorkflowNotFound):
            future_wid1 = await manager.get_workflow_result('wid1')
            await future_wid1

        # Start another workflow but don't delete its result immediately
        manager.start_workflow('sample_workflow', 'wid2', delete_on_finish=False)
        future_wid2 = await manager.get_workflow_result('wid2')
        done_result = await future_wid2
        await asyncio.sleep(0.1)
        assert done_result is not None

        await manager.delete_workflow('wid2')

        with pytest.raises(WorkflowNotFound):
            future_wid2 = await manager.get_workflow_result('wid2')
            await future_wid2

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
        await asyncio.sleep(0.1)
        assert not manager.has_workflow('wid1')

        with pytest.raises(WorkflowNotFound):
            future_wid1 = await manager.get_workflow_result('wid1')
            await future_wid1


@pytest.mark.asyncio
@timeout(6)
async def test_rehydration_single_workflow():
    workflows = {
        "pause_resume_workflow": pause_resume_workflow
    }
    manager = create_in_memory_workflow_manager(workflows=workflows)

    pause_event = asyncio.Event()

    async with manager:
        manager.start_workflow("pause_resume_workflow", "wid_1", pause_event, delete_on_finish=False)
        assert manager.has_workflow("wid_1")

        # Workflow paused
        await asyncio.sleep(0.2)
        assert manager.has_workflow("wid_1")

        # Resume workflow
        pause_event.set()

        # Workflow completed
        future_wid1 = await manager.get_workflow_result("wid_1")
        result = await future_wid1
        await asyncio.sleep(0.1)
        assert result is not None

        assert not manager.has_workflow("wid_1")
