import asyncio

import pytest

from src.quest import step
from src.quest.historian import Historian


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

    task = historian.start_workflow('world')
    result = await task

    assert result == 'Hello world'


@step
def double(text):
    return text * 2


@step
def add_foo(text):
    return text + 'foo'


block_workflow = asyncio.Event()


async def longer_workflow(text):
    text = await double(text)
    text = await add_foo(text)
    await block_workflow.wait()
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

    task = historian.start_workflow('abc')
    # give the other task a chance to run
    await asyncio.sleep(1)

    # task runs and blocks on 'block_workflow'
    # now cancel it
    task.cancel()

    assert history
    block_workflow.set()

    # Make a new historian with the old history
    historian = Historian(
        'test',
        longer_workflow,
        history
    )
    task = historian.start_workflow('abc')
    result = await task

    assert result == 'abcabcfooabcabcfoo'
