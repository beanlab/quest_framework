import asyncio

import pytest

from src.quest.historian import Historian
from src.quest.wrappers import task
from quest_test.utils import timeout

stop = asyncio.Event()
steps = []


async def workflow_will_stop():
    steps.append(1)
    await stop.wait()
    steps.append(2)


@pytest.mark.asyncio
@timeout(3)
async def test_cancel():
    historian = Historian(
        'test',
        workflow_will_stop,
        [],
    )

    workflow = historian.run()
    await asyncio.sleep(0.1)
    await historian.suspend()
    stop.set()
    await asyncio.sleep(0.1)

    assert steps == [1]


stuff = {
    'first': [],
    'second': []
}

block = asyncio.Event()


@task
async def do_stuff(name):
    global stuff
    stuff[name].append(1)
    await block.wait()
    stuff[name].append(2)


async def workflow_with_tasks():
    task1 = do_stuff('first')
    task2 = do_stuff('second')
    await task1
    await task2


@pytest.mark.asyncio
@timeout(3)
async def test_task_cancel():
    historian = Historian(
        'test',
        workflow_with_tasks,
        [],
    )

    workflow = historian.run()
    await asyncio.sleep(0.1)
    await historian.suspend()
    block.set()
    await asyncio.sleep(0.1)

    assert stuff == {'first': [1], 'second': [1]}
