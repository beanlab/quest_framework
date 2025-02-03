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


async def paused_workflow(pause_event: asyncio.Event):
    await pause_event.wait()
    await asyncio.sleep(0.01)
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
        manager.start_workflow('sample_workflow', 'wid1')
        manager.start_workflow('sample_workflow', 'wid2')
        manager.start_workflow('sample_workflow', 'wid3', delete_unfinished=True)

        metrics = manager.get_workflow_metrics()
        assert len(metrics) == 3

        # Wait for wid1 to complete and check result
        result_wid1 = await manager.get_workflow_result('wid1', delete=False)
        assert result_wid1 is not None
        assert not manager.has_workflow('wid1')

        # Wait for wid2 to complete and check result
        result_wid2 = await manager.get_workflow_result('wid2', delete=False)
        assert result_wid2 is not None
        assert not manager.has_workflow('wid2')

        # wid3 (delete_unfinished=True → no result stored)
        await asyncio.sleep(0)
        assert not manager.has_workflow('wid3')
        result_wid3 = await manager.get_workflow_result('wid3', delete=False)
        assert result_wid3 is None

        metrics = manager.get_workflow_metrics()
        assert len(metrics) == 0


@pytest.mark.asyncio
@timeout(6)
async def test_pause_and_resume():
    """
    Starts a workflow and suspend
    Ensure the workflow resumes and completes after rehydration
    """
    pause_event = asyncio.Event()

    workflows = {
        "paused_workflow": lambda: paused_workflow(pause_event)
    }

    # Shared storage - workflows persist across different manager instances
    shared_storage = InMemoryBlobStorage()

    def create_history(wid: str):
        return PersistentHistory(wid, shared_storage)

    def create_workflow(wtype: str):
        return workflows[wtype]

    serializer = NoopSerializer()

    manager1 = WorkflowManager("test", shared_storage, create_history, create_workflow, serializer)
    async with manager1:
        manager1.start_workflow('paused_workflow', 'wid1')
        await asyncio.sleep(2)
        await manager1.suspend_workflow('wid1')
        # manager1 exits, write state to the shared_storage

    # New workflowmanager using the shared storage
    manager2 = WorkflowManager("test", shared_storage, create_history, create_workflow, serializer)
    async with manager2:
        pause_event.set()  # Resume paused workflow
        await asyncio.sleep(2)
        result = await manager2.get_workflow_result('wid1', delete=False)
        assert result is not None  # Workflow completed and have result
        assert not manager2.has_workflow('wid1')


@pytest.mark.asyncio
@timeout(6)
async def test_exception_handling():
    """ Test Exception Handling (_quest_exception_type) """
    workflows = {
        "error_workflow": error_workflow
    }
    manager = create_in_memory_workflow_manager(workflows=workflows)

    async with manager:
        manager.start_workflow('error_workflow', 'wid1')

        # Raise exception
        with pytest.raises(ValueError) as error_info:
            await manager.get_workflow_result('wid1', delete=False)
        assert "Intentional error" in str(error_info.value)

        # Check the stored exception result
        error_result = manager._results.get('wid1')
        assert error_result is not None
        assert "_quest_exception_type" in error_result


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
        manager.delete_workflow('wid_run')
        assert not manager.has_workflow('wid_run')

        # Delete finished workflow
        manager.start_workflow('sample_workflow', 'wid_finished')
        done_result = await manager.get_workflow_result('wid_finished', delete=False)
        assert done_result is not None, "Expected result for wid_finished"
        manager.delete_workflow('wid_finished')
        assert not manager.has_workflow('wid_finished')

        # Delete not-existing
        with pytest.raises(WorkflowNotFound):
            manager.delete_workflow('not_existing')


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
        manager.delete_workflow('wid1')
        assert not manager.has_workflow('wid1')

        # Confirm no result is stored
        result = await manager.get_workflow_result('wid1')
        assert result is None
