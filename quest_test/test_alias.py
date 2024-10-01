import asyncio
import pytest

from src.quest import step
from src.quest.historian import Historian
from src.quest.wrappers import task
from src.quest.manager_wrappers import Alias
from utils import timeout

def test_alias():

@step
async def append_data(array: [], data):
    array.append(data)
    return array

@task
async def workflow1(data):
    array = await append_data([], data)
    return array

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




