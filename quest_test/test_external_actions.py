import asyncio

import pytest

from src.quest.external import state, queue, event
from src.quest.historian import Historian
from src.quest.wrappers import task

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
    historian = Historian('test', state_workflow, [])
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


@task
async def get_foos(identity, foo_values: list):
    async with queue('foo', identity) as foos:
        while True:
            foo_values.append(await foos.get())


@task
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
        [],
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


@task
async def level2():
    async with queue('the_queue', None) as the_queue:
        return (await the_queue.get()) + (await the_queue.get())


@task
async def level1():
    job = level2()
    await asyncio.sleep(0.01)
    return await job


async def workflow_nested_tasks():
    job = level1()
    await asyncio.sleep(0.01)
    return await job


@pytest.mark.asyncio
async def test_nested_tasks():
    historian = Historian(
        'test',
        workflow_nested_tasks,
        [],
    )

    workflow = asyncio.create_task(historian.run())
    await asyncio.sleep(0.01)

    await historian.record_external_event('the_queue', None, 'put', 1)
    historian.suspend()

    new_workflow = asyncio.create_task(historian.run())
    await asyncio.sleep(1)
    await historian.record_external_event('the_queue', None, 'put', 2)

    assert await new_workflow == 3


#
# Resuming tasks tests
#

@pytest.mark.asyncio
async def test_queue_tasks_resume():
    id_foo = 'FOO'
    id_bar = 'BAR'
    history = []
    historian = Historian(
        'test',
        queue_task_workflow,
        history
    )

    workflow = asyncio.create_task(historian.run(id_foo, id_bar))
    await asyncio.sleep(1)

    resources = historian.get_resources(id_foo)
    assert 'foo' in resources
    assert 'foo_done' in resources

    resources = historian.get_resources(id_bar)
    assert 'foo' in resources
    assert 'foo_done' in resources

    await historian.record_external_event('foo', id_bar, 'put', 4)
    await historian.record_external_event('foo', id_foo, 'put', 1)
    await historian.record_external_event('foo', id_foo, 'put', 2)

    historian.suspend()

    # Start it over
    workflow = asyncio.create_task(historian.run(id_foo, id_bar))
    await asyncio.sleep(1)

    resources = historian.get_resources(id_foo)
    assert 'foo' in resources
    assert 'foo_done' in resources

    resources = historian.get_resources(id_bar)
    assert 'foo' in resources
    assert 'foo_done' in resources

    await historian.record_external_event('foo', id_bar, 'put', 5)
    await historian.record_external_event('foo_done', id_bar, 'set')
    await historian.record_external_event('foo', id_foo, 'put', 3)
    await historian.record_external_event('foo_done', id_foo, 'set')

    assert await workflow == [1, 2, 3, 4, 5]


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
    historian = Historian(
        'test',
        failflow,
        []
    )
    try:
        await historian.run()
    except ExceptionGroup as ex:
        assert ex.exceptions[0].args[0] == 'Epic Fail'
