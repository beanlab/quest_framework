import asyncio
import pytest
import logging

from quest import WorkflowManager
from quest.persistence import InMemoryBlobStorage, PersistentHistory
from src.quest import queue, Historian
from src.quest.manager_wrappers import alias
from utils import timeout


@pytest.mark.asyncio
@timeout(3)
async def test_alias():
    first_pause = asyncio.Event()
    second_pause = asyncio.Event()
    data = []

    async def workflow():
        async with queue('data', None) as q:
            data.append(await q.get())
            await first_pause.wait()
            async with alias('the_foo'):
                data.append(await q.get())
            await second_pause.wait()
            data.append(await q.get())

    storage = InMemoryBlobStorage()
    histories = {}

    def create_history(wid: str):
        if wid not in histories:
            histories[wid] = PersistentHistory(wid, InMemoryBlobStorage())
        return histories[wid]

    def create_workflow():
        return workflow()

    async with WorkflowManager('test_alias', storage, create_history, lambda w_type: create_workflow) as manager:
        manager.start_workflow('workflow', 'wid')

        await asyncio.sleep(0.1)

        await manager.send_event('wid', 'data', None, 'put', 'data 1')

        assert 'data 1' in data

# TODO: test exception on alias dict collision

@pytest.mark.asyncio
@timeout(3)
async def test_alias_trade():
    first_pause = asyncio.Event()
    second_pause = asyncio.Event()
    third_pause = asyncio.Event()
    data_a = []
    data_b = []

    async def workflow_a():
        async with queue('data', None) as q:
            async with alias('the_foo'):
                await first_pause.wait()
                data_a.append(await q.get())
                data_a.append(await q.get())
            await second_pause.wait()
            await third_pause.wait()
            data_a.append(await q.get())

    async def workflow_b():
        async with queue('data', None) as q:
            await first_pause.wait()
            data_b.append(await q.get())
        await second_pause.wait()
        async with alias('the_foo'):
            await third_pause.wait()
            data_b.append(await q.get())
            data_b.append(await q.get())

    async def create_workflow(wid: str):
        match(wid):
            case 'workflow_a':
                return workflow_a()
            case 'workflow_b':
                return workflow_b()

    storage = InMemoryBlobStorage()
    histories = {}

    def create_history(wid: str):
        if wid not in histories:
            histories[wid] = PersistentHistory(wid, InMemoryBlobStorage())
        return histories[wid]

    async with WorkflowManager('test_alias', storage, create_history, lambda w_type: create_workflow) as manager:
        # Gather resources
        manager.start_workflow('workflow_a', 'wid_a')
        manager.start_workflow('workflow_b', 'wid_b')

        logging.info('Workflows started')
        await asyncio.sleep(0.1)

        first_pause.set()
        await manager.send_event('wid_a', 'data', None, 'put', 'data a 1')
        await manager.send_event('wid_b', 'data', None, 'put', 'data b 1')
        await manager.send_event('the_foo', 'data', None, 'put', 'data foo 1')
        await asyncio.sleep(0.1)  # yield to the workflows

        # now both should be waiting on second gate and no one should be the foo
        assert not manager.has_workflow('the_foo')
        second_pause.set()
        await asyncio.sleep(0.1)  # yield

        # now workflow b should be the foo
        await manager.send_event('wid_a', 'data', None, 'put', 'data a 2')
        await manager.send_event('wid_b', 'data', None, 'put', 'data b 2')
        await manager.send_event('the_foo', 'data', None, 'put', 'data foo 2')
        third_pause.set()
        await asyncio.sleep(0.1)  # yield to the workflows

        assert data_a == ['data a 1', 'data foo 1', 'data a 2']
        assert data_b == ['data b 1', 'data b 2', 'data foo 2']



