import asyncio
import pytest
import logging

from quest import WorkflowManager
from quest.manager import workflow_manager
from quest.persistence import InMemoryBlobStorage, PersistentHistory
from src.quest import step, queue
from src.quest.historian import Historian
from src.quest.wrappers import task
from src.quest.manager_wrappers import alias
from utils import timeout

# TODO: test exception on alias dict collision

@pytest.mark.asyncio
async def test_alias():
    pause = asyncio.Event()
    data_a = []
    data_b = []

    async def workflow_a():
        async with queue('data', None) as q:
            async with alias('the_foo'):
                data_a.append(await q.get())
            await pause.wait()
            data_a.append(await q.get())

    async def workflow_b():
        # TODO: How should I check the data?
        async with queue('data', None) as q:
            await pause.wait()
            async with alias('the_foo'):
                data_b.append(await q.get())
            data_b.append(await q.get())

    storage = InMemoryBlobStorage()
    histories = {}

    def create_history(wid: str):
        if wid not in histories:
            histories[wid] = PersistentHistory(wid, InMemoryBlobStorage())
        return histories[wid]

    async with WorkflowManager('test_alias', storage, create_history, lambda w_type: workflow_a) as manager:
        manager.start_workflow('workflow_a', 'wid_a')
        foo_resources = await manager.get_resources('the_foo', None)
        a_resources = await manager.get_resources('wid_a', None)
        # TODO: Will there be two different queues here?
        data_foo = foo_resources['data']
        data_a = a_resources['data']
        await data_a.put('I am Workflow A')
        await data_foo.put('I am the FOO')

    async with WorkflowManager('test_alias', storage, create_history, lambda w_type: workflow_b) as manager:
        manager.start_workflow('workflow_b', 'wid_b')
        resources = await manager.get_resources('the_foo', None)
        data_foo = resources['data']
        # Now pause the manager and all workflows

    assert 'wid1' in histories
    assert counter_a == 1
    assert counter_b == 0

    async with WorkflowManager('test-manager', storage, create_history, lambda w_type: workflow) as manager:
        # At this point, all workflows should be resumed
        pause.set()
        await asyncio.sleep(0.1)
        result = await manager.get_workflow('wid1')
        assert result == 11

    assert counter_a == 2
    assert counter_b == 1

    history = []
    # TODO: Does historian wrap the workflow stuff?
    historian = Historian(
        'test',
        run_workflows,
        history,
    )

    # TODO: Which way is correct? Here or ln 61
    resources = await historian.get_resources(None)
    data_queue = resources.get('data')
    data_queue.put('foo')

    gate_b.set()
    gate_a.set()

    async with queue('data', None) as q:
        # TODO: Check Bean's messages
        await q.put('foo')
        await q.put('bar')

counters = {}
pauses = {}

@step
async def foobar(text, counter):
    global counters
    counters[counter] += 1
    return 'foo' + text + 'bar'


@task
async def do_the_foo(text, counter):
    text = await foobar(text, counter)
    await pauses[counter].wait()
    text = await foobar(text, counter)
    return text


async def sub_task_workflow(text1, text2, counter):
    task1 = do_the_foo(text1, counter)
    await asyncio.sleep(0.01)

    task2 = do_the_foo(text2, counter)
    await asyncio.sleep(0.01)

    return (await task1) + (await task2)


@pytest.mark.asyncio
@timeout(3)
async def test_basic_tasks():
    global counters
    counters['basic_tasks'] = 0
    pauses['basic_tasks'] = asyncio.Event()

    history = []
    historian = Historian(
        'test',
        sub_task_workflow,
        history,
    )

    # Don't pause
    pauses['basic_tasks'].set()

    result = await historian.run('abc', 'xyz', 'basic_tasks')

    assert counters['basic_tasks'] == 4
    assert result == 'foofooabcbarbarfoofooxyzbarbar'




