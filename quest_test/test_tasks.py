import asyncio

import pytest

from src.quest import these
from src.quest.json_seralizers import get_local_workflow_manager
from src.quest.wrappers import task, queue, state, Queue


@task
async def hello_world(action, name: str, event: asyncio.Event):
    action(name + ' hello')
    await event.wait()
    action(name + 'world')


async def workflow():
    event1 = asyncio.Event()
    event2 = asyncio.Event()
    messages = []
    first = hello_world(messages.append, 'first', event1)
    second = hello_world(messages.append, 'second', event2)

    event2.set()
    event1.set()

    await first
    await second

    assert messages == [
        'first hello',
        'second hello',
        'second world',
        'first world'
    ]


@pytest.mark.asyncio
async def test_task_flow(tmp_path):
    async with get_local_workflow_manager(tmp_path, workflow) as workflow_manager:
        workflow_manager.start_workflow('test', 'workflow')


@task
async def get_sequence(ident: str, q: Queue):
    async with state('received', identity=ident) as set_received:
        value1 = await q.pop()
        await set_received(value1)
        value2 = await q.pop()
        await set_received(value2)
        return value1, value2


async def workflow2(ident1, ident2):
    async with these([queue('items', identity=idt) for idt in [ident1, ident2]]) as qs:
        value_tasks = [get_sequence(idt, q) for idt, q in zip([ident1, ident2], qs)]
        values = [await t for t in value_tasks]

    return values


@pytest.mark.asyncio
async def test_queues_tasks(tmp_path):
    ident1 = 'first'
    ident2 = 'second'
    wid = '123'
    async with get_local_workflow_manager(tmp_path, workflow2) as wm:
        # Start workflow
        await wm.start_workflow(wid, 'workflow2', ident1, ident2)

        # Intial status
        status = await wm.get_status(wid, identity=ident1)
        assert status.state['received']['value'] is None

        status = await wm.get_status(wid, identity=ident2)
        assert status.state['received']['value'] is None

        # Progress ident2
        await wm.push_queue(wid, 'items', 'a', identity=ident2)
        status = await wm.get_status(wid, identity=ident2)
        assert status.state['received']['value'] == 'a'

        status = await wm.get_status(wid, identity=ident1)
        assert status.state['received']['value'] is None

        # Progress ident2 again - closes state and queue
        await wm.push_queue(wid, 'items', 'b', identity=ident2)
        status = await wm.get_status(wid, identity=ident2)
        assert 'received' not in status.state  # i.e. the 'received' state is gone now

        status = await wm.get_status(wid, identity=ident1)
        assert status.state['received']['value'] is None

        # Now ident1
        await wm.push_queue(wid, 'items', 'x', identity=ident1)
        status = await wm.get_status(wid, identity=ident1)
        assert status.state['received']['value'] == 'x'

        await wm.push_queue(wid, 'items', 'y', identity=ident1)
        status = await wm.get_status(wid, identity=ident1)
        assert 'received' not in status.state

        # Should now be finished
        status = await wm.get_status(wid)
        assert status.state['result']['value'] == [
            ('x', 'y'),
            ('a', 'b'),
        ]

