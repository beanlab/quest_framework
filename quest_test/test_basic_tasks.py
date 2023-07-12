import asyncio
from functools import wraps

import pytest

from src.quest import step
from src.quest.historian import Historian
from src.quest.wrappers import task


def timeout(delay):
    def decorator(func):
        @wraps(func)
        async def new_func(*args, **kwargs):
            async with asyncio.timeout(delay):
                return await func(*args, **kwargs)

        return new_func

    return decorator


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
    if not pauses[counter].is_set():
        raise asyncio.CancelledError()
    text = await foobar(text, counter)
    return text


async def sub_task_workflow(text1, text2, counter):
    task1 = do_the_foo(text1, counter)
    await asyncio.sleep(0.01)

    task2 = do_the_foo(text2, counter)
    await asyncio.sleep(0.01)

    return (await task1) + (await task2)


@pytest.mark.asyncio
async def test_basic_tasks():
    global counters
    counters['basic_tasks'] = 0
    pauses['basic_tasks'] = asyncio.Event()

    history = []
    unique_ids = {}
    historian = Historian(
        'test',
        sub_task_workflow,
        history,
        unique_ids
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
    unique_ids = {}
    historian = Historian(
        'test',
        sub_task_workflow,
        history,
        unique_ids
    )

    try:
        result = await historian.run('abc', 'xyz', 'tasks_resume')
    except asyncio.CancelledError:
        pass

    # Both subtasks should have run the first foobar
    assert counters['tasks_resume'] == 2

    # Don't pause this time
    pauses['tasks_resume'].set()
    result = await historian.run()

    assert counters['tasks_resume'] == 4
    assert result == 'foofooabcbarbarfoofooxyzbarbar'


@task
async def will_fail(duration):
    await asyncio.sleep(duration)
    raise Exception(f'failed! {duration}')


async def workflow_that_fails():
    task1 = will_fail(5)
    task2 = will_fail(0.1)  # this one should fail first and kill the workflow
    await asyncio.sleep(0.01)
    await task1
    await task2


@pytest.mark.asyncio
async def test_task_exception():
    historian = Historian(
        'test',
        workflow_that_fails,
        [],
        {}
    )
    try:
        await historian.run()
    except ExceptionGroup as ex:
        assert ex.exceptions[0].args[0] == 'failed! 0.1'
