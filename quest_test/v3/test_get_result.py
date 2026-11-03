import asyncio

import pytest

from quest.manager import WorkflowNotFound
from .utils import timeout, create_in_memory_workflow_manager


class OurException(Exception):
    pass


@pytest.mark.asyncio
@timeout(3)
async def test_basic_store_result():
    async def workflow1():
        return "done"

    async with create_in_memory_workflow_manager({'w1': workflow1}) as manager:
        manager.start_workflow('w1', 'wid1', delete_on_finish=False)
        await asyncio.sleep(0.1)

        assert await manager.get_workflow_result('wid1') == 'done'
        assert manager.has_workflow('wid1')
        assert await manager.get_workflow_result('wid1', delete=True) == 'done'
        assert not manager.has_workflow('wid1')


@pytest.mark.asyncio
@timeout(3)
async def test_workflows_not_saved_have_no_results():
    async def workflow1():
        return "done"

    async with create_in_memory_workflow_manager({'w1': workflow1}) as manager:
        manager.start_workflow('w1', 'wid1')
        await asyncio.sleep(0.1)
        assert not manager.has_workflow('wid1')


@pytest.mark.asyncio
@timeout(3)
async def test_get_result_on_missing_workflow_raises():
    async def workflow1():
        return "done"

    async with create_in_memory_workflow_manager({'w1': workflow1}) as manager:
        manager.start_workflow('w1', 'wid1')
        await asyncio.sleep(0.1)
        assert not manager.has_workflow('wid1')

        with pytest.raises(WorkflowNotFound):
            await manager.get_workflow_result('wid1')


@pytest.mark.asyncio
@timeout(6)
async def test_get_result_on_running_workflow():
    gate = asyncio.Event()

    async def sample_workflow():
        await gate.wait()
        return "sample workflow result"

    workflows = {
        "sample_workflow": sample_workflow
    }
    manager = create_in_memory_workflow_manager(workflows=workflows)

    async with manager:
        manager.start_workflow('sample_workflow', 'wid1')
        await asyncio.sleep(0.1)

        get_result_task = asyncio.create_task(manager.get_workflow_result('wid1'))

        gate.set()
        result = await get_result_task
        assert result is not None

@pytest.mark.asyncio
@timeout(3)
async def test_exception_store_result():
    async def workflow1():
        raise OurException('died')

    async with create_in_memory_workflow_manager({'w1': workflow1}) as manager:
        manager.start_workflow('w1', 'wid1', delete_on_finish=False)
        await asyncio.sleep(0.1)

        assert manager.has_workflow('wid1')

        with pytest.raises(OurException):
            await manager.get_workflow_result('wid1')

        assert manager.has_workflow('wid1')

        with pytest.raises(OurException):
            await manager.get_workflow_result('wid1', delete=True)

        assert not manager.has_workflow('wid1')
