import asyncio

import pytest

from quest.historian import Historian
from quest.resources import state, queue, MultiQueue, Queue
from quest.serializer import NoopSerializer
from quest.wrappers import task, step
from .utils import timeout


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

@pytest.mark.asyncio
@timeout(3)
async def test_external_state():
    name_event = asyncio.Event()

    async def state_workflow(identity):
        async with state('name', identity, 'Foobar') as name:
            # External views the name
            await name_event.wait()
            name_event.clear()

            await name.set('Barbaz')
            # External views it again
            await name_event.wait()

    identity = 'foo_ident'
    historian = Historian('test', state_workflow, [], serializer=NoopSerializer())
    workflow = historian.run(identity)

    await asyncio.sleep(0.001)  # defer to the workflow

    # Observe state
    resources = await historian.get_resources(None)  # i.e. public resources
    assert not resources  # should be empty

    resources = await historian.get_resources(identity)
    assert ('state', 'name', 'foo_ident') in resources
    assert resources[('state', 'name', 'foo_ident')] == 'Foobar'
    name_event.set()
    await asyncio.sleep(0.001)

    resources = await historian.get_resources(identity)
    assert ('state', 'name', 'foo_ident') in resources
    assert resources[('state', 'name', 'foo_ident')] == 'Barbaz'

    # Resume
    name_event.set()

    await workflow


@pytest.mark.asyncio
@timeout(3)
async def test_external_queue():
    async def workflow_with_queue(identity):
        items_received = []
        async with queue('items', identity) as items:
            while len(items_received) < 3:
                item = await items.get()
                items_received.append(item)
        return items_received

    identity = 'foo_ident'
    historian = Historian(
        'test',
        workflow_with_queue,
        [],
        serializer=NoopSerializer()
    )
    workflow = historian.run(identity)
    await asyncio.sleep(0.001)  # yield to workflow

    resources = await historian.get_resources(None)
    assert not resources

    resources = await historian.get_resources(identity)
    assert (Queue._rtype, 'items', 'foo_ident') in resources

    await historian.record_external_event(Queue._rtype, 'items', 'foo_ident', 'put', 7)
    await historian.record_external_event(Queue._rtype, 'items', 'foo_ident', 'put', 8)
    await historian.record_external_event(Queue._rtype, 'items', 'foo_ident', 'put', 9)

    assert await workflow == [7, 8, 9]


@task
async def get_foos(identity, foo_values: list):
    async with queue('foo', identity) as foos:
        while True:
            foo_values.append(await foos.get())


@task
async def foo_task(identity):
    foo_values = []
    async with queue('foo_done', identity) as finished:
        foos = get_foos(identity, foo_values)
        await finished.get()
        foos.cancel()
        return foo_values


async def queue_task_workflow(id1, id2):
    foos = foo_task(id1)
    bars = foo_task(id2)
    return (await foos) + (await bars)


@pytest.mark.asyncio
@timeout(3)
async def test_queue_tasks():
    id_foo = 'FOO'
    id_bar = 'BAR'
    historian = Historian(
        'test',
        queue_task_workflow,
        [],
        serializer=NoopSerializer()
    )

    workflow = historian.run(id_foo, id_bar)
    await asyncio.sleep(0.001)

    resources = await historian.get_resources(id_foo)
    assert (Queue._rtype, 'foo', 'FOO') in resources
    assert (Queue._rtype, 'foo_done', 'FOO') in resources

    resources = await historian.get_resources(id_bar)
    assert (Queue._rtype, 'foo', 'BAR') in resources
    assert (Queue._rtype, 'foo_done', 'BAR') in resources

    await historian.record_external_event(Queue._rtype, 'foo', id_bar, 'put', 4)
    await historian.record_external_event(Queue._rtype, 'foo', id_foo, 'put', 1)
    await historian.record_external_event(Queue._rtype, 'foo', id_foo, 'put', 2)
    await historian.record_external_event(Queue._rtype, 'foo', id_bar, 'put', 5)
    await historian.record_external_event(Queue._rtype, 'foo_done', id_bar, 'put', None)
    await historian.record_external_event(Queue._rtype, 'foo', id_foo, 'put', 3)
    await historian.record_external_event(Queue._rtype, 'foo_done', id_foo, 'put', None)

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
@timeout(3)
async def test_nested_tasks():
    historian = Historian(
        'test',
        workflow_nested_tasks,
        [],
        serializer=NoopSerializer()
    )

    workflow = historian.run()
    await asyncio.sleep(0.001)

    await historian.record_external_event(Queue._rtype, 'the_queue', None, 'put', 1)
    await historian.suspend()

    new_workflow = historian.run()
    await asyncio.sleep(1)
    await historian.record_external_event(Queue._rtype, 'the_queue', None, 'put', 2)

    assert await new_workflow == 3


#
# Resuming tasks tests
#

@pytest.mark.asyncio
@timeout(3)
async def test_queue_tasks_resume():
    id_foo = 'FOO'
    id_bar = 'BAR'
    history = []
    historian = Historian(
        'test',
        queue_task_workflow,
        history,
        serializer=NoopSerializer()
    )

    workflow = historian.run(id_foo, id_bar)
    await asyncio.sleep(0.001)

    resources = await historian.get_resources(id_foo)
    assert (Queue._rtype, 'foo', 'FOO') in resources
    assert (Queue._rtype, 'foo_done', 'FOO') in resources

    resources = await historian.get_resources(id_bar)
    assert (Queue._rtype, 'foo', 'BAR') in resources
    assert (Queue._rtype, 'foo_done', 'BAR') in resources

    await historian.record_external_event(Queue._rtype, 'foo', id_bar, 'put', 4)
    await historian.record_external_event(Queue._rtype, 'foo', id_foo, 'put', 1)
    await historian.record_external_event(Queue._rtype, 'foo', id_foo, 'put', 2)

    await historian.suspend()

    # Start it over
    workflow = historian.run(id_foo, id_bar)
    await asyncio.sleep(0.001)
    await asyncio.sleep(1)

    resources = await historian.get_resources(id_foo)
    assert (Queue._rtype, 'foo', 'FOO') in resources
    assert (Queue._rtype, 'foo_done', 'FOO') in resources

    resources = await historian.get_resources(id_bar)
    assert (Queue._rtype, 'foo', 'BAR') in resources
    assert (Queue._rtype, 'foo_done', 'BAR') in resources

    await historian.record_external_event(Queue._rtype, 'foo', id_bar, 'put', 5)
    await historian.record_external_event(Queue._rtype, 'foo_done', id_bar, 'put', None)
    await historian.record_external_event(Queue._rtype, 'foo', id_foo, 'put', 3)
    await historian.record_external_event(Queue._rtype, 'foo_done', id_foo, 'put', None)

    assert await workflow == [1, 2, 3, 4, 5]


@step
async def get_value():
    async with queue('the-queue', None) as q:
        return int(await q.get())


async def interactive_process_with_steps():
    total = 0
    total += await get_value()
    total += await get_value()
    return total


@pytest.mark.asyncio
@timeout(3)
async def test_step_specific_external():
    """
    When an external event occurs on a resources that is specific to the step,
    and the step history is pruned after the step completes,
    then the external event on the now-obsolete resource must also be pruned.
    """
    history = []
    historian = Historian('test', interactive_process_with_steps, history, serializer=NoopSerializer())
    historian.run()
    await asyncio.sleep(0.1)
    resources = await historian.get_resources(None)
    assert (Queue._rtype, 'the-queue', None) in resources
    await historian.record_external_event(Queue._rtype, 'the-queue', None, 'put', 1)
    await asyncio.sleep(0.1)
    await historian.suspend()

    workflow = historian.run()
    await asyncio.sleep(0.1)
    resources = await historian.get_resources(None)
    assert (Queue._rtype, 'the-queue', None) in resources
    await historian.record_external_event(Queue._rtype, 'the-queue', None, 'put', 2)

    assert (await workflow) == 3


@pytest.mark.asyncio
@timeout(3)
async def test_multiqueue_default():
    received = []

    async def player_workflow():
        players = {'p1': 'user1', 'p2': 'user2'}

        async with MultiQueue('chat', players) as mq:
            async for ident, msg in mq:
                received.append((ident, msg))
                # If player sends 'bye', remove their queue after their message is recorded
                if msg == 'bye':
                    await mq.remove(ident)
                # Exit the Multiqueue when 3 messages are recorded
                if len(received) == 3:
                    break
        return received

    historian = Historian('test', player_workflow, [], serializer=NoopSerializer())
    workflow = historian.run()

    await asyncio.sleep(0.1)

    await historian.record_external_event(Queue._rtype, 'chat', 'p1', 'put', 'hello')
    await historian.record_external_event(Queue._rtype, 'chat', 'p2', 'put', 'hi')
    await historian.record_external_event(Queue._rtype, 'chat', 'p1', 'put', 'bye')

    result = await workflow
    assert result == [('p1', 'hello'), ('p2', 'hi'), ('p1', 'bye')]

    # After removing p1 -> when p1 tries to send message, it should raise KeyError
    # with pytest.raises(KeyError):
    #     await historian.record_external_event('chat', 'p1', 'put', 'should not be received')


@pytest.mark.asyncio
@timeout(3)
async def test_multiqueue_single_response():
    received = {}

    async def player_workflow():
        players = {'p1': 'user1', 'p2': 'user2'}
        async with MultiQueue('chat', players, single_response=True) as mq:
            async for ident, msg in mq:
                received[ident] = msg
        return received

    historian = Historian('test', player_workflow, [], serializer=NoopSerializer())
    workflow = historian.run()

    await asyncio.sleep(0.1)

    await historian.record_external_event(Queue._rtype, 'chat', 'p1', 'put', 'hello')
    await historian.record_external_event(Queue._rtype, 'chat', 'p2', 'put', 'hi')
    # Second message from p1 - should be ignored due to single_response = True
    await historian.record_external_event(Queue._rtype, 'chat', 'p1', 'put', 'should not be received')

    result = await workflow
    assert result == {'p1': 'hello', 'p2': 'hi'}


"""

gate = asyncio.Event()
task_fut = asyncio.Future()

async def get_stuck():
    me: asyncio.Task = await task_fut
    print('cr_await', me._coro.cr_await)
    print('cr_running', me._coro.cr_running)
    print('cr_suspended', me._coro.cr_suspended)
    await gate.wait()

async def just_stuck():
    await gate.wait()

@pytest.mark.asyncio
async def test_research():
    task1 = asyncio.create_task(get_stuck())
    print(1, is_waiting(task1), task1.done())
    task_fut.set_result(task1)
    print(2, is_waiting(task1), task1.done())
    await asyncio.sleep(0)  # should let task1 start
    print(3, is_waiting(task1), task1.done())
    task2 = asyncio.create_task(just_stuck())  # shouldn't be started yet
    print('foo')
    gate.set()
    print(4, is_waiting(task1), task1.done())
    await task2
    print(5, is_waiting(task1), task1.done())
    await asyncio.sleep(0)
    print(6, is_waiting(task1), task1.done())
    await task1
    print(7, is_waiting(task1), task1.done())


def is_waiting(task: asyncio.Task):
    coro = task.get_coro()
    return hasattr(coro, 'cr_await') and getattr(coro, 'cr_await') is not None
"""
