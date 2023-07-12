import asyncio

import pytest

from src.quest import step
from src.quest.historian import Historian


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
async def test_basic_workflow():
    history = []
    historian = Historian(
        'test',
        workflow,
        history
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
    if not block_workflow.is_set():
        raise asyncio.CancelledError()
    text = await double(text)
    return text


@pytest.mark.asyncio
async def test_resume():
    history = []
    historian = Historian(
        'test',
        longer_workflow,
        history
    )

    try:
        # task runs and blocks on 'block_workflow'
        await historian.run('abc')
    except asyncio.CancelledError:
        pass

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
    if not pause.is_set():
        raise asyncio.CancelledError()
    return await foo(text1 + text2)


@pytest.mark.asyncio
async def test_nested_steps_resume():
    history = []
    historian = Historian(
        'test',
        nested_workflow,
        history
    )

    try:
        await historian.run('abc', 'xyz')
    except asyncio.CancelledError:
        pass

    pause.set()

    result = await historian.run()

    assert result == 'foofooabcbarfooxyzbar'
