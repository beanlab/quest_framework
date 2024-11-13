import asyncio

import pytest

from utils import timeout
from src.quest import step
from src.quest.historian import Historian
from quest.serializer import NoopSerializer


#
# Test single step and workflow return value
#

@step
async def make_message(name):
    return 'Hello ' + name


async def workflow(name):
    message = await make_message(name)
    print(message)
    return message


@pytest.mark.asyncio
@timeout(3)
async def test_basic_workflow():
    history = []
    historian = Historian(
        'test',
        workflow,
        history,
        serializer=NoopSerializer()
    )

    result = await historian.run('world')

    assert result == 'Hello world'


#
## Test basic resume
#

double_calls = 0
foo_calls = 0


@step
async def double(text):
    global double_calls
    double_calls += 1
    return text * 2


@step
async def add_foo(text):
    global foo_calls
    foo_calls += 1
    return text + 'foo'


block_workflow = asyncio.Event()


async def longer_workflow(text):
    text = await double(text)
    text = await add_foo(text)
    await block_workflow.wait()
    text = await double(text)
    return text


@pytest.mark.asyncio
@timeout(3)
async def test_resume():
    history = []
    historian = Historian(
        'test',
        longer_workflow,
        history,
        serializer=NoopSerializer()
    )

    workflow = historian.run('abc')
    await asyncio.sleep(0.01)
    await historian.suspend()

    assert history  # should not be empty

    # Allow workflow to proceed
    block_workflow.set()

    # Start the workflow again
    result = await historian.run('abc')

    assert result == 'abcabcfooabcabcfoo'
    assert double_calls == 2
    assert foo_calls == 1


#
## Test nested steps with resume
#

@step
async def foo(text):
    return 'foo' + text


@step
async def bar(text):
    return text + 'bar'


@step
async def foo_then_bar(text):
    text = await foo(text)
    text = await bar(text)
    return text


pause = asyncio.Event()


async def nested_workflow(text1, text2):
    text1 = await foo_then_bar(text1)
    text2 = await foo_then_bar(text2)
    await pause.wait()
    return await foo(text1 + text2)


@pytest.mark.asyncio
@timeout(3)
async def test_nested_steps_resume():
    history = []
    historian = Historian(
        'test',
        nested_workflow,
        history,
        serializer=NoopSerializer()
    )

    workflow = historian.run('abc', 'xyz')
    await asyncio.sleep(0.1)
    await historian.suspend()

    pause.set()
    result = await historian.run()

    assert result == 'foofooabcbarfooxyzbar'


stop = asyncio.Event()


@step
async def do_step1(start):
    a = start + 1
    await stop.wait()
    return a + 1


async def dance(start):
    a = await do_step1(start)
    return a


@pytest.mark.asyncio
@timeout(3)
async def test_resume_mid_step():
    historian = Historian(
        'test',
        dance,
        [],
        serializer=NoopSerializer()
    )

    wtask = historian.run(1)
    await asyncio.sleep(0.1)
    await historian.suspend()
    stop.set()

    wtask = historian.run(1)
    await asyncio.sleep(0.1)

    assert await wtask == 3
