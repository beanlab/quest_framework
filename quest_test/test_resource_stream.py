import asyncio
import pytest

from quest import Historian
from quest.external import state, queue, event, identity_queue
from utils import timeout, create_test_historian


async def simple_workflow(phrase1_ident, phrase2_ident):
    async with state('phrase1', phrase1_ident, 'Hello') as phrase1:
        await phrase1.set('World!')

        async with state('phrase2', phrase2_ident, 'Goodbye') as phrase2:
            await phrase2.set('Everyone!')


async def simple_listener(historian, stream_ident=None, phrase1_ident=None, phrase2_ident=None):
    phrase1_fail = True
    phrase2_fail = True
    with historian.get_resource_stream(stream_ident) as resource_stream:
        async for resources in resource_stream:
            if ('phrase1', phrase1_ident) in resources:
                phrase1_fail = False
            if ('phrase2', phrase2_ident) in resources:
                phrase2_fail = False
    if phrase1_fail or phrase2_fail:
        assert False


async def failing_listener(historian: Historian, identity):
    try:
        with historian.get_resource_stream(identity) as resource_stream:
            i = 0
            async for resources in resource_stream:
                if i == 5:
                    raise Exception("Resource Stream listener error")
                i += 1
    except Exception as e:
        assert str(e) == "Resource Stream listener error"


@pytest.mark.asyncio
@timeout(3)
async def test_default():
    async def default_workflow():
        async with state('phrase', None, 'Hello') as phrase1:
            await phrase1.set('World!')

        async with queue('messages', None) as messages:
            await messages.get()

        async with identity_queue('ident_messages') as ident_messages:
            await ident_messages.get()

        async with event('gate', None) as gate:
            await gate.wait()

    historian = create_test_historian(
        'default',
        default_workflow
    )

    w_task = historian.run()

    with historian.get_resource_stream(None) as resource_stream:
        updates = aiter(resource_stream)
        resources = await anext(updates)
        assert not resources

        # Phrase created
        resources = await anext(updates)
        assert ('phrase', None) in resources
        assert await resources[('phrase', None)].value() == 'Hello'

        resources = await anext(updates)
        assert ('phrase', None) in resources
        assert await resources[('phrase', None)].value() == 'World!'

        resources = await anext(updates)  # Phrase deleted
        assert ('phrase', None) not in resources

        # Messages created
        resources = await anext(updates)
        assert ('messages', None) in resources
        await resources[('messages', None)].put('Hello!')

        resources = await anext(updates)  # messages.get()
        assert ('messages', None) in resources

        resources = await anext(updates)  # Messages deleted
        assert ('messages', None) not in resources

        # Identity messages created
        resources = await anext(updates)
        assert ('ident_messages', None) in resources
        await resources[('ident_messages', None)].put('Hello!')

        resources = await anext(updates)  # ident_messages.get()
        assert ('ident_messages', None) in resources

        resources = await anext(updates)  # Identity messages deleted
        assert ('ident_messages', None) not in resources

        # Gate created
        resources = await anext(updates)
        assert ('gate', None) in resources
        await resources[('gate', None)].set()

        resources = await anext(updates)  # gate.wait()
        assert ('gate', None) in resources

        resources = await anext(updates)  # Gate deleted
        assert ('gate', None) not in resources

        try:
            await anext(updates)
            pytest.fail('Should have thrown StopAsyncIteration')
        except StopAsyncIteration:
            pass

    await w_task


@pytest.mark.asyncio
@timeout(3)
async def test_typical():
    historian = create_test_historian(
        'typical',
        lambda: simple_workflow(None, None)
    )

    async def run_workflow():
        w_task = historian.run()
        await w_task

    # Demonstrates what a typical listener would look like
    async def typical_listener():
        reported_resources = []

        with historian.get_resource_stream(None) as resource_stream:
            async for resources in resource_stream:
                reported_resources.append(resources)

        for resources in reported_resources:
            print(resources)

    # Asyncio.gather runs code in different tasks, demonstrating that while the workflow is executing on one task,
    # it is reporting resource updates to the listener on a separate task
    await asyncio.gather(run_workflow(), typical_listener())


# Test a private listener streaming public resources
@pytest.mark.asyncio
@timeout(3)
async def test_private_identity_streaming_public_resources():
    historian = create_test_historian(
        'private_identity_streaming_public_resources',
        lambda: simple_workflow(None, None)
    )

    w_task = historian.run()
    await simple_listener(historian)
    await w_task


# Test that a public listener does not receive updates to private resources
@pytest.mark.asyncio
@timeout(3)
async def test_public_streaming_private_resources():
    historian = create_test_historian(
        'public_streaming_private_resources',
        lambda: simple_workflow('private_identity', 'private_identity')
    )

    w_task = historian.run()
    with historian.get_resource_stream(None) as resource_stream:
        async for resources in resource_stream:
            assert not resources
    await w_task


# Test a resource stream that then errors unexpectedly
@pytest.mark.asyncio
@timeout(3)
async def test_exception():
    historian = create_test_historian(
        'exception',
        lambda: simple_workflow(None, None)
    )

    wtask = historian.run()

    await failing_listener(historian, None)

    assert historian._resource_stream_manager._resource_streams == {}
    await wtask


# Test multiple public listeners
@pytest.mark.asyncio
@timeout(3)
async def test_concurrent_none_streams():
    historian = create_test_historian(
        'concurrent_none_streams',
        lambda: simple_workflow(None, None)
    )

    wtask = historian.run()

    # Run multiple streams concurrently
    await asyncio.gather(simple_listener(historian),
                         simple_listener(historian),
                         simple_listener(historian))

    await wtask


# Test streaming public and private resources by a public and private listener respectively
@pytest.mark.asyncio
@timeout(3)
async def test_mult_identity_workflow():
    async def public_listener():
        with historian.get_resource_stream(None) as resource_stream:
            phrase1_fail = True
            async for resources in resource_stream:
                if ('phrase1', None) in resources:
                    phrase1_fail = False
                if ('phrase2', 'private_identity') in resources:
                    assert False
        if phrase1_fail:
            assert False

    historian = create_test_historian(
        'mult_identity_workflow',
        lambda: simple_workflow(None, 'private_identity')
    )

    w_task = historian.run()
    await asyncio.gather(public_listener(), simple_listener(
        historian,
        'private_identity',
        None,
        'private_identity'
    ))
    await w_task


# Test streaming multiple private resources by two different private listeners
@pytest.mark.asyncio
@timeout(3)
async def test_multiple_private_identity_streams():
    async def ident1_listener():
        with historian.get_resource_stream('ident1') as resource_stream:
            ident1_fail = True
            ident2_fail = False
            async for resources in resource_stream:
                if ('phrase1', 'ident1') in resources:
                    ident1_fail = False
                if ('phrase2', 'ident2') in resources:
                    ident2_fail = True
            if ident1_fail or ident2_fail:
                assert False

    async def ident2_listener():
        with historian.get_resource_stream('ident2') as resource_stream:
            ident1_fail = False
            ident2_fail = True
            async for resources in resource_stream:
                if ('phrase1', 'ident1') in resources:
                    ident1_fail = True
                if ('phrase2', 'ident2') in resources:
                    ident2_fail = False
            if ident1_fail or ident2_fail:
                assert False

    historian = create_test_historian(
        'different_identity_streams',
        lambda: simple_workflow('ident1', 'ident2')
    )

    wtask = historian.run()
    await asyncio.gather(ident1_listener(), ident2_listener())
    await wtask


# Test multiple different listeners and one unexpectedly errors
@pytest.mark.asyncio
@timeout(3)
async def test_closing_different_identity_streams():
    historian = create_test_historian(
        'different_identity_streams',
        lambda: simple_workflow(None, 'private_identity')
    )

    w_task = historian.run()
    await asyncio.gather(
        failing_listener(historian, None),
        simple_listener(historian, 'private_identity', None, 'private_identity')
    )
    await w_task


@pytest.mark.asyncio
@timeout(4)
async def test_suspend_workflow():
    historian = create_test_historian(
        'test_suspend_workflow',
        lambda: simple_workflow(None, None)
    )

    historian.run()

    with historian.get_resource_stream(None) as resource_stream:
        updates = aiter(resource_stream)
        for i in range(4):
            await anext(updates)

        await historian.suspend()

        try:
            await anext(updates)
            pytest.fail('Should have thrown StopAsyncIteration')
        except StopAsyncIteration:
            pass


@pytest.mark.asyncio
@timeout(4)
async def test_suspend_resume_workflow():
    historian = create_test_historian(
        'test_suspend_resume_workflow',
        lambda: simple_workflow(None, None)
    )

    historian.run()
    with historian.get_resource_stream(None) as resource_stream:
        updates = aiter(resource_stream)
        for i in range(3):  # Call anext up to just before creation of phrase2
            await anext(updates)

    await historian.suspend()
    w_task = historian.run()

    with historian.get_resource_stream(None) as resource_stream:
        updates = aiter(resource_stream)
        await anext(updates)  # Get initial snapshot of resources
        resources = await anext(updates)
        assert ('phrase2', None) in resources
        assert await resources[('phrase2', None)].value() == 'Goodbye'

        resources = await anext(updates)
        assert ('phrase2', None) in resources
        assert await resources[('phrase2', None)].value() == 'Everyone!'

        resources = await anext(updates)
        assert ('phrase1', None) in resources
        assert ('phrase2', None) not in resources

        resources = await anext(updates)
        assert not resources

    await w_task


@pytest.mark.asyncio
@timeout(3)
async def test_resources_of_same_name():
    async def identical_resources_workflow():
        async with (state('my_state', None, 'bless') as public_state,
                    state('my_state', 'private_identity', 'curse') as private_state):
            await public_state.set('bless up')
            await private_state.set('curse all')

    historian = create_test_historian(
        'test_resources_of_same_name',
        identical_resources_workflow,
    )

    w_task = historian.run()

    with historian.get_resource_stream('private_identity') as resource_stream:
        resource_name = 'my_state'
        i = 0
        async for resources in resource_stream:
            print(f'resources at iteration {i}: {resources}')
            if 'my_state' in resources:
                print(f'resource value: {await resources[resource_name].value()}')
            i += 1

    await w_task
