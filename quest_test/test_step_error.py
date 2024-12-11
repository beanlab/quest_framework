import asyncio
import pytest

from .custom_errors.custom_error import MyError
from quest_test.utils import timeout
from quest import step, NoopSerializer
from quest.historian import Historian

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
    raise MyError(text + "foo")


block_workflow = asyncio.Event()


async def longer_workflow(text):
    text = await double(text)
    try:
        await add_foo(text)
    except MyError as e:
        text = e.message
    except Exception:
        assert False
    await block_workflow.wait()
    text = await double(text)
    return text


@pytest.mark.asyncio
@timeout(10)
async def test_custom_exception():
    history = []
    historian = Historian(
        'test',
        longer_workflow,
        history,
        NoopSerializer()
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

double_calls2 = 0
foo_calls2 = 0


@step
async def double2(text):
    global double_calls2
    double_calls2 += 1
    return text * 2


@step
async def add_foo2(text):
    global foo_calls2
    foo_calls2 += 1
    return 1 / 0

block_workflow2 = asyncio.Event()

async def longer_workflow2(text):
    text = await double2(text)
    try:
        await add_foo2(text)
    except ZeroDivisionError as e:
        pass
    except Exception:
        assert False
    await block_workflow2.wait()
    text = await double2(text)
    return text


@pytest.mark.asyncio
@timeout(10)
async def test_builtin():
    history = []
    historian = Historian(
        'test2',
        longer_workflow2,
        history,
        NoopSerializer()
    )

    workflow2 = historian.run('abc')
    await asyncio.sleep(0.01)
    await historian.suspend()

    assert history  # should not be empty

    # Allow workflow to proceed
    block_workflow2.set()

    # Start the workflow again
    result2 = await historian.run('abc')

    assert result2 == 'abcabcabcabc'
    assert double_calls2 == 2
    assert foo_calls2 == 1