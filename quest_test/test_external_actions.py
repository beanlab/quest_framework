import asyncio
import logging

import pytest

from src.quest import these
from src.quest.historian import Historian
from src.quest.json_seralizers import get_local_workflow_manager
from src.quest.wrappers import task, step
from src.quest.external import state, queue, identity_queue, event

# External resource tests
# - create the resource
# - act on the resource
# - observe the resource
# - delete the resource
# - resume and observe resource (external events should be accurately replayed)


# Also:
# - identities and visibility
# - should the identity be required along with the resource ID?

# Test state

name_event = asyncio.Event()


async def state_workflow(identity):
    async with state('name', identity, 'Foobar') as name:
        assert await name.get() == 'Foobar'
        await name_event.wait()
        assert await name.get() == 'Barbaz'


@pytest.mark.asyncio
async def test_external_state():
    identity = 'foo_ident'
    historian = Historian('test', state_workflow, [], {})
    workflow = asyncio.create_task(historian.run(identity))
    await asyncio.sleep(0.01)

    # Observe state
    resources = historian.get_resources(None)  # i.e. public resources
    assert not resources  # should be empty

    resources = historian.get_resources(identity)
    assert 'name' in resources
    assert resources['name']['type'] == "src.quest.external.State"
    assert resources['name']['value'] == 'Foobar'

    # Set state
    await historian.record_external_event('name', identity, 'set', 'Barbaz')

    resources = historian.get_resources(identity)
    assert 'name' in resources
    assert resources['name']['type'] == "src.quest.external.State"
    assert resources['name']['value'] == 'Barbaz'

    # Resume
    name_event.set()

    await workflow


async def workflow_with_queue(identity):
    items_received = []
    async with queue('items', identity) as items:
        while len(items_received) < 3:
            item = await items.get()
            items_received.append(item)
    return items_received


@pytest.mark.asyncio
async def test_external_queue():
    identity = 'foo_ident'
    historian = Historian(
        'test',
        workflow_with_queue,
        [],
        {}
    )
    workflow = asyncio.create_task(historian.run(identity))
    await asyncio.sleep(0.01)

    resources = historian.get_resources(None)
    assert not resources

    resources = historian.get_resources(identity)
    assert 'items' in resources
    assert resources['items']['type'] == 'asyncio.queues.Queue'

    await historian.record_external_event('items', identity, 'put', 7)
    await historian.record_external_event('items', identity, 'put', 8)
    await historian.record_external_event('items', identity, 'put', 9)

    assert await workflow == [7, 8, 9]


async def one_of(*coro):
    tasks = [asyncio.create_task(co) for co in coro]
    done, pending = asyncio.wait(tasks)
    for i in range(len(tasks)):
        if tasks[i] in done:
            return i, done
    raise Exception('Unreachable code was reached')


@task
async def get_foos(identity, foo_values: list):
    async with queue('foo', identity) as foos:
        while True:
            foo_values.append(await foos.get())


@task
@step
async def foo_task(identity):
    foo_values = []
    async with event('foo_done', identity) as finished:
        foos = get_foos(identity, foo_values)
        await finished.wait()
        foos.cancel()
        return foo_values


async def queue_task_workflow(id1, id2):
    foos = foo_task(id1)
    bars = foo_task(id2)
    return (await foos) + (await bars)


@pytest.mark.asyncio
async def test_queue_tasks():
    id_foo = 'FOO'
    id_bar = 'BAR'
    historian = Historian(
        'test',
        queue_task_workflow,
        [], {}
    )

    workflow = asyncio.create_task(historian.run(id_foo, id_bar))
    await asyncio.sleep(0.01)

    resources = historian.get_resources(id_foo)
    assert 'foo' in resources
    assert 'foo_done' in resources

    resources = historian.get_resources(id_bar)
    assert 'foo' in resources
    assert 'foo_done' in resources

    await historian.record_external_event('foo', id_bar, 'put', 4)
    await historian.record_external_event('foo', id_foo, 'put', 1)
    await historian.record_external_event('foo', id_foo, 'put', 2)
    await historian.record_external_event('foo', id_bar, 'put', 5)
    await historian.record_external_event('foo_done', id_bar, 'set')
    await historian.record_external_event('foo', id_foo, 'put', 3)
    await historian.record_external_event('foo_done', id_foo, 'set')

    assert await workflow == [1, 2, 3, 4, 5]


"""


@task
async def get_sequence(ident: str, q: Queue):
    async with state('received', identity=ident) as receceived:
        value1 = await q.pop()
        await receceived.set(value1)
        value2 = await q.pop()
        await receceived.set(value2)
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
        await wm.run(wid, 'workflow2', ident1, ident2)

        # Initial status
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


@task
async def fail():
    raise Exception('Epic Fail')


@task
async def do_stuff():
    await asyncio.sleep(1)
    await asyncio.sleep(1)
    await asyncio.sleep(3)
    pytest.fail('This should not have been reached')


async def failflow():
    will_do_stuff = do_stuff()
    will_fail = fail()
    await will_do_stuff
    await will_fail


@pytest.mark.asyncio
async def test_failflow(tmp_path):
    async with get_local_workflow_manager(tmp_path, failflow) as wm:
        try:
            await wm.run('test', 'failflow')
        except asyncio.CancelledError:
            pass
"""
