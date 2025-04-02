import asyncio
import pytest

from history import step
from history.history import History
from history.wrappers import task
from history.serializer import NoopSerializer
from .utils import timeout

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
    historian = History(
        'test',
        sub_task_workflow,
        history,
        serializer=NoopSerializer()
    )

    # Don't pause
    pauses['basic_tasks'].set()

    result = await historian.run('abc', 'xyz', 'basic_tasks')

    assert counters['basic_tasks'] == 4
    assert result == 'foofooabcbarbarfoofooxyzbarbar'


@pytest.mark.asyncio
@timeout(3)
async def test_basic_tasks_resume():
    global counters
    counters['tasks_resume'] = 0
    pauses['tasks_resume'] = asyncio.Event()

    history = []
    historian = History(
        'test',
        sub_task_workflow,
        history,
        serializer=NoopSerializer()
    )

    # Will run and block on the event
    workflow = historian.run('abc', 'xyz', 'tasks_resume')
    await asyncio.sleep(0.1)
    await historian.suspend()

    # Both subtasks should have run the first foobar
    assert counters['tasks_resume'] == 2

    # Don't pause this time
    pauses['tasks_resume'].set()
    result = await historian.run()

    assert counters['tasks_resume'] == 4
    assert result == 'foofooabcbarbarfoofooxyzbarbar'
