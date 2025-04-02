import asyncio

import pytest

from history import step
from history.history import History
from history.wrappers import task
from history.serializer import NoopSerializer
from .utils import timeout


@step
async def plus_foo(text):
    return text + 'foo'


@task
async def three_foo(text):
    text = await plus_foo(text)
    text = await plus_foo(text)
    text = await plus_foo(text)
    return text


async def fooflow(text1, text2):
    first = three_foo(text1)
    second = three_foo(text2)
    return await first, await second


@pytest.mark.asyncio
async def test_step_concurrency():
    historian = History(
        'test',
        fooflow,
        [],
        serializer=NoopSerializer()
    )

    assert await historian.run('abc', 'xyz') == ('abcfoofoofoo', 'xyzfoofoofoo')


@task
@step
async def double(text):
    return text + text


async def doubleflow(text):
    first = double(text)
    second = double(text[:3])
    return await first + await second


@pytest.mark.asyncio
@timeout(3)
async def test_step_tasks():
    historian = History(
        'test',
        doubleflow,
        [],
        serializer=NoopSerializer()
    )

    assert await historian.run('abcxyz') == 'abcxyzabcxyzabcabc'


# Two tasks running concurrently
# One has a long step.
# The other completes steps within the temporal frame of the first step.
# When pruned, only steps relevant to the first task should be removed.

long_pause = asyncio.Event()
long_result = 0


@step
async def long_step():
    await long_pause.wait()
    global long_result
    long_result += 1
    return long_result


@task
async def do_long_work():
    result = await long_step()
    return result


short_pause = asyncio.Event()
short_result = 0


@step
async def short_step():
    global short_result
    short_result += 1
    return short_result


@task
async def do_fast_work():
    result = (await short_step() + await short_step() + await short_step())
    await short_pause.wait()
    return result  # 1 + 2 + 3 = 6


workflow_pause = asyncio.Event()


async def long_fast_race():
    long_task = do_long_work()
    await asyncio.sleep(0.01)
    fast_task = do_fast_work()
    await asyncio.sleep(0.01)
    long_pause.set()  # allow the long work to finish.
    await workflow_pause.wait()
    short_pause.set()  # allow the short work to finish
    return (await long_task) + (await fast_task)  # 1 + 6 = 7


@pytest.mark.asyncio
@timeout(3)
async def test_long_fast_race():
    records = []
    history = History('test', long_fast_race, records, serializer=NoopSerializer())
    workflow = history.run()
    await asyncio.sleep(1)
    await history.suspend()
    print('original', records)
    print('_history', history._book)

    workflow = history.run()
    workflow_pause.set()
    result = await workflow
    # Short result should be 3 (func called 3x)
    # Long result should be 1 (func called 1x)
    assert [result, short_result, long_result] == [7, 3, 1]
