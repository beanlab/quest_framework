import asyncio
import pytest

from src.quest import step, queue
from src.quest.historian import Historian
from src.quest.wrappers import task
from src.quest.manager_wrappers import alias
from utils import timeout

# TODO: test exception on alias dict collision

@pytest.mark.asyncio
async def test_alias():
    gate_a = asyncio.Event()
    gate_b = asyncio.Event()

    @task
    async def workflow_a():
        data = []
        async with queue('data', None) as q:
            async with alias('the_foo'):
                data.append(await q.get())
            await gate_a.wait()
            data.append(await q.get())

    @task
    async def workflow_b():
        # TODO: How should I check the data?
        data = []
        async with queue('data', None) as q:
            await gate_b.wait()
            async with alias('the_foo'):
                data.append(await q.get())
            data.append(await q.get())
        return data

    async def run_workflows():
        # TODO: Is this how I should be switching off alias?
        res_a = await workflow_a()
        res_b = await workflow_b()
        gate_b.set()
        gate_a.set()

        print(res_a)
        print(res_b)

    history = []
    # TODO: Does historian wrap the workflow stuff?
    historian = Historian(
        'test',
        run_workflows,
        history,
    )

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




