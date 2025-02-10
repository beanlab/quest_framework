import asyncio
import pytest

from quest.manager import WorkflowNotFound, WorkflowManager
from quest.persistence import InMemoryBlobStorage, PersistentHistory
from quest.serializer import NoopSerializer
from .utils import timeout, create_in_memory_workflow_manager


async def sample_workflow():
    await asyncio.sleep(0.01)
    return "sample workflow result"


async def error_workflow():
    raise ValueError("Intentional error")


async def paused_workflow(pause_event: asyncio.Event, done_event: asyncio.Event):
    await pause_event.wait()
    await asyncio.sleep(0.01)
    done_event.set()
    return "paused workflow result"


@pytest.mark.asyncio
@timeout(6)
async def test_three_workflows_and_check_result():
    """
    Start Three Workflows
    1. Wait for two workflows to complete and retrieve their results
    2. Check (delete_unfinished=True) workflow does not store result
    3. All workflows are completed and removed from active tracking
    """
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

        # Wait for wid1 to complete and check result
        result_wid1 = await manager.get_workflow_result('wid1')
        assert result_wid1 is not None
        assert not manager.has_workflow('wid1')

        await asyncio.sleep(0.1)  # Allow wid2 to complete

        result_wid2 = await manager.get_workflow_result('wid2')
        assert result_wid2 is not None
        assert not manager.has_workflow('wid2')

        await asyncio.sleep(0.1)  # Allow wid3 to complete

        # wid3 (delete_unfinished=True → no result stored)
        assert not manager.has_workflow('wid3')
        result_wid3 = await manager.get_workflow_result('wid3')
        assert result_wid3 is None

        metrics = manager.get_workflow_metrics()
        assert len(metrics) == 0


@pytest.mark.asyncio
async def test_pause_and_resume():
    """
    Starts a workflow and suspend
    Ensure the workflow resumes and completes after rehydration
    """
    pause_event = asyncio.Event()
    done_event = asyncio.Event()

    workflows = {
        "paused_workflow": lambda: paused_workflow(pause_event, done_event)
    }

    manager = create_in_memory_workflow_manager(workflows=workflows)
    async with manager:
        manager.start_workflow('paused_workflow', 'wid1', delete_on_finish=False)
        await asyncio.sleep(2)
        await manager.suspend_workflow('wid1')
        # Exit context
    async with manager:  # re-enter
        pause_event.set()  # resume paused workflow
        await done_event.wait()
        result = await manager.get_workflow_result('wid1')
        assert result is not None
        assert not manager.has_workflow('wid1')


@pytest.mark.asyncio
@timeout(6)
async def test_exception_handling():
    """ Test Exception Handling """
    workflows = {
        "error_workflow": error_workflow
    }
    manager = create_in_memory_workflow_manager(workflows=workflows)

    async with manager:
        manager.start_workflow('error_workflow', 'wid1')

        # Check any exception is raised
        with pytest.raises(Exception):
            await manager.get_workflow_result("wid1")

        # Ensure workflow removed after the exception
        assert not manager.has_workflow("wid1")


@pytest.mark.asyncio
@timeout(6)
async def test_workflow_deletion():
    """
    Test Workflow Deletion
    1. Delete running workflow
    2. Delete finished workflow
    3. Attempt to delete a not existing workflow => WorkflowNotFound
    """
    workflows = {
        "sample_workflow": sample_workflow
    }
    manager = create_in_memory_workflow_manager(workflows=workflows)

    async with manager:
        # Delete running workflow
        manager.start_workflow('sample_workflow', 'wid_run')
        assert manager.has_workflow('wid_run')
        await manager.delete_workflow('wid_run')
        assert not manager.has_workflow('wid_run')

        # Delete finished workflow
        manager.start_workflow('sample_workflow', 'wid_finished', delete_on_finish=False)
        done_result = await manager.get_workflow_result('wid_finished')
        assert done_result is not None
        await manager.delete_workflow('wid_finished')
        assert not manager.has_workflow('wid_finished')

        # Delete not-existing
        with pytest.raises(WorkflowNotFound):
            await manager.delete_workflow('not_existing')


@pytest.mark.asyncio
@timeout(6)
async def test_workflow_suspension_and_cancellation():
    """
    Start workflow and suspend
    1. Delete the workflow while suspended
    2. Check no result stored
    """
    workflows = {
        "sample_workflow": sample_workflow
    }
    manager = create_in_memory_workflow_manager(workflows=workflows)

    async with manager:
        manager.start_workflow('sample_workflow', 'wid1')
        await manager.suspend_workflow('wid1')

        # Delete while suspended
        await manager.delete_workflow('wid1')
        assert not manager.has_workflow('wid1')

        # Confirm no result is stored
        result = await manager.get_workflow_result('wid1', delete=True)
        assert result is None
