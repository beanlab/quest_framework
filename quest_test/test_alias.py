import asyncio
import pytest
import logging

from quest import WorkflowManager
from quest.persistence import InMemoryBlobStorage, PersistentHistory
from src.quest import step, queue
from src.quest.historian import Historian
from src.quest.wrappers import task
from src.quest.manager_wrappers import alias

# TODO: test exception on alias dict collision

@pytest.mark.asyncio
async def test_alias():
    pause = asyncio.Event()
    data_a = []
    data_b = []

    async def workflow_a():
        # TODO: Which format is correct?
        nonlocal data_a
        async with alias('the_foo'):
            async with queue('data', None) as q:
                # Take alias first
                data_a.append(await q.get())
        await pause.wait()
        data_a.append(await q.get())

    async def workflow_b():
        nonlocal data_b
        async with queue('data', None) as q:
            await pause.wait()
            # Take alias second
            async with alias('the_foo'):
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

    async with WorkflowManager('test_alias', storage, create_history, lambda w_type: workflow_a) as manager:
        manager.start_workflow('workflow_a', 'wid_a')

        await asyncio.sleep(0.1)

    # async with WorkflowManager('test_alias', storage, create_history, lambda w_type: create_workflow) as manager:
    #
    #     # Start both workflows
    #     manager.start_workflow_background('workflow_a', 'wid_a')
    #     manager.start_workflow_background('workflow_b', 'wid_b')
    #
    #     await asyncio.sleep(0.1)
    #     # await manager.wait_for_completion('wid_a', None)
    #     # await manager.wait_for_completion('wid_b', None)

    async with WorkflowManager('test_alias', storage, create_history, lambda w_type: create_workflow) as manager:
        # Gather resources
        # TODO: Do I need to grab this again after the alias switches?
        foo_resources = await manager.get_resources('the_foo', None)
        a_resources = await manager.get_resources('wid_a', None)
        b_resources = await manager.get_resources('wid_b', None)

        # TODO: Will there be two different queues here? One for A and one for B?
        data_foo = foo_resources['data']
        data_queue_a = a_resources['data']
        data_queue_b = b_resources['data']

        # Put first round info
        await data_queue_a.put('I am Workflow A (1)')
        await data_queue_b.put('I am Workflow B (1)')
        await data_foo.put('I am the FOO')

        # Check while workflow is suspended
        assert 'I am Workflow A (1)' in data_a
        assert 'I am Workflow B (1)' in data_b
        assert 'I am the FOO' in data_a

        # Restart workflows
        pause.set()

        # Send second set of data
        await data_queue_a.put('I am Workflow A (2)')
        await data_queue_b.put('I am Workflow B (2)')
        await data_foo.put('I am the FOO')

        # Check alias switched
        assert 'I am Workflow A (2)' in data_a
        assert 'I am Workflow B (2)' in data_b
        assert 'I am the FOO' in data_b




