import asyncio
import pytest

from quest.manager import DuplicateAliasException
from quest import queue, alias
from .utils import timeout, create_in_memory_workflow_manager


@pytest.mark.asyncio
@timeout(3)
async def test_alias():
    first_pause = asyncio.Event()
    second_pause = asyncio.Event()
    data = []

    async def workflow():
        async with queue('data', None) as q:
            data.append(await q.get())
            async with alias('the_foo'):
                # Create the alias but wait for the data to get sent to it
                await first_pause.wait()
                data.append(await q.get())
            await second_pause.wait()
            data.append(await q.get())

    workflows = {
        'workflow': workflow
    }

    async with create_in_memory_workflow_manager(workflows) as manager:
        manager.start_workflow('workflow', 'wid')
        await asyncio.sleep(0.1)

        await manager.send_event('wid', 'data', None, 'put', '1')
        await asyncio.sleep(0.1)

        assert '1' in data

        await manager.send_event('the_foo', 'data', None, 'put', 'foo')
        first_pause.set()
        await asyncio.sleep(0.1)

        assert 'foo' in data

        await manager.send_event('wid', 'data', None, 'put', '2')
        second_pause.set()
        await asyncio.sleep(0.1)

        assert '2' in data


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

    workflows = {
        'workflow_a': workflow_a,
        'workflow_b': workflow_b,
    }

    async with create_in_memory_workflow_manager(workflows) as manager:
        # Gather resources
        manager.start_workflow('workflow_a', 'wid_a')
        await asyncio.sleep(0.1)
        manager.start_workflow('workflow_b', 'wid_b')
        await asyncio.sleep(0.1)

        first_pause.set()
        await manager.send_event('wid_a', 'data', None, 'put', 'data a 1')
        await manager.send_event('wid_b', 'data', None, 'put', 'data b 1')
        await manager.send_event('the_foo', 'data', None, 'put', 'data foo 1')
        await asyncio.sleep(0.1)  # yield to the workflows

        # now both should be waiting on second gate and no one should be the foo
        assert not manager.has_workflow('the_foo')
        assert 'data a 1' in data_a
        assert 'data foo 1' in data_a
        assert 'data b 1' in data_b

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


@pytest.mark.asyncio
@timeout(3)
async def test_alias_exception():
    pause = asyncio.Event()

    async def workflow_a():
        async with alias('the_foo'):
            await pause.wait()

    async def workflow_b():
        try:
            async with alias('the_foo'):
                await pause.wait()
        except DuplicateAliasException:
            return
        pytest.fail('Should have raised DuplicateAliasException')

    workflows = {
        'workflow_a': workflow_a,
        'workflow_b': workflow_b,
    }
    async with create_in_memory_workflow_manager(workflows) as manager:
        manager.start_workflow('workflow_a', 'wid1', delete_on_finish=False)
        manager.start_workflow('workflow_b', 'wid2', delete_on_finish=False)

        await asyncio.sleep(0.1)
        pause.set()
        await asyncio.sleep(0.1)  # Allow workflows to finish

        result_wid1 = await manager.get_workflow_result('wid1', delete=True)
        result_wid2 = await manager.get_workflow_result('wid2', delete=True)
