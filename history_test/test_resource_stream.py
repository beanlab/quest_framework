import asyncio
import pytest

from history import History
from history.external import state, queue, event, identity_queue, wrap_as_state, wrap_as_queue, wrap_as_identity_queue, \
    wrap_as_event
from .utils import timeout, create_test_history

# A general-use workflow for these tests
async def simple_workflow(phrase1_ident, phrase2_ident):
    async with state('phrase1', phrase1_ident, 'Hello') as phrase1:
        await phrase1.set('World!')

        async with state('phrase2', phrase2_ident, 'Goodbye') as phrase2:
            await phrase2.set('Everyone!')


# A general-use listener that asserts that both resources are seen while streaming
async def simple_listener(historian, stream_ident=None, phrase1_ident=None, phrase2_ident=None):
    saw_phrase1 = False
    saw_phrase2 = False
    with historian.get_resource_stream(stream_ident) as resource_stream:
        async for resources in resource_stream:
            if ('phrase1', phrase1_ident) in resources:
                saw_phrase1 = True
            if ('phrase2', phrase2_ident) in resources:
                saw_phrase2 = True
    if not saw_phrase1 or not saw_phrase2:
        assert False


class StreamListenerError(Exception):
    pass

# A listener that fails streaming before the workflow completes
async def failing_listener(historian: History, identity):
    try:
        with historian.get_resource_stream(identity) as resource_stream:
            i = 0
            async for resources in resource_stream:
                if i == 5:
                    raise StreamListenerError()
                i += 1
    except StreamListenerError:
        pass

# Test stream resources on a workflow that utilizes each type of "resource" as defined in external.py.
# We should receive a resource update when each resource is created, has an action called on it, and deleted.
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

    historian = create_test_history(
        'default',
        default_workflow
    )

    w_task = historian.run()

    with historian.get_resource_stream(None) as resource_stream:
        phrase = wrap_as_state('phrase', None, historian)
        messages = wrap_as_queue('messages', None, historian)
        ident_messages = wrap_as_identity_queue('ident_messages', None, historian)
        gate = wrap_as_event('gate', None, historian)

        updates = aiter(resource_stream)
        resources = await anext(updates)
        assert not resources

        # Phrase created
        resources = await anext(updates)
        assert ('phrase', None) in resources
        assert await phrase.value() == 'Hello'

        resources = await anext(updates)
        assert ('phrase', None) in resources
        assert await phrase.value() == 'World!'

        resources = await anext(updates)  # Phrase deleted
        assert ('phrase', None) not in resources

        # Messages created
        resources = await anext(updates)
        assert ('messages', None) in resources
        await messages.put('Hello!')

        resources = await anext(updates)  # messages.get()
        assert ('messages', None) in resources

        resources = await anext(updates)  # Messages deleted
        assert ('messages', None) not in resources

        # Identity messages created
        resources = await anext(updates)
        assert ('ident_messages', None) in resources
        await ident_messages.put('Hello!')

        resources = await anext(updates)  # ident_messages.get()
        assert ('ident_messages', None) in resources

        resources = await anext(updates)  # Identity messages deleted
        assert ('ident_messages', None) not in resources

        # Gate created
        resources = await anext(updates)
        assert ('gate', None) in resources
        await gate.set()

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


# A demonstration and test of how a typical listener would stream resources using the "async for" syntax.
# The listener should receive 7 total updates during the running of workflow.
@pytest.mark.asyncio
@timeout(3)
async def test_typical():
    historian = create_test_history(
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

        assert len(reported_resources) == 7
        for resources in reported_resources:
            print(resources)

    # Asyncio.gather runs code in different tasks, demonstrating that while the workflow is executing on one task,
    # it is reporting resource updates to the listener on a separate task
    await asyncio.gather(run_workflow(), typical_listener())


# Test a private listener streaming public resources
# The private listener should receive all updates on any public resource.
@pytest.mark.asyncio
@timeout(3)
async def test_private_identity_streaming_public_resources():
    historian = create_test_history(
        'private_identity_streaming_public_resources',
        lambda: simple_workflow(None, None)
    )

    w_task = historian.run()
    await simple_listener(historian, None, None, None)
    await w_task


# Test that a public listener does not receive updates to private resources
@pytest.mark.asyncio
@timeout(3)
async def test_public_streaming_private_resources():
    historian = create_test_history(
        'public_streaming_private_resources',
        lambda: simple_workflow('private_identity', 'private_identity')
    )

    w_task = historian.run()
    with historian.get_resource_stream(None) as resource_stream:
        async for resources in resource_stream:
            assert not resources
    await w_task


# Test a resource stream listener that errors unexpectedly and the resource stream cleans up.
@pytest.mark.asyncio
@timeout(3)
async def test_exception():
    historian = create_test_history(
        'exception',
        lambda: simple_workflow(None, None)
    )

    wtask = historian.run()

    await failing_listener(historian, None)

    assert historian._resource_stream_manager._resource_streams == {}
    await wtask


# Test that multiple public listeners can stream resources separately.
# Each listener should receive an update and be able to process it.
@pytest.mark.asyncio
@timeout(3)
async def test_concurrent_none_streams():
    historian = create_test_history(
        'concurrent_none_streams',
        lambda: simple_workflow(None, None)
    )

    wtask = historian.run()

    # Run multiple streams concurrently
    await asyncio.gather(simple_listener(historian, None, None, None),
                         simple_listener(historian, None, None, None),
                         simple_listener(historian, None, None, None))

    await wtask


# Test streaming public and private resources by a public and private listener respectively.
# The private listener should receive updates of its own private resources as well as the public resource.
# The public listener should only receive updates of the public resource.
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

    historian = create_test_history(
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


# Test streaming multiple private resources by two different private listeners.
# Each private listener should only receive updates of resources it owns.
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

    historian = create_test_history(
        'different_identity_streams',
        lambda: simple_workflow('ident1', 'ident2')
    )

    wtask = historian.run()
    await asyncio.gather(ident1_listener(), ident2_listener())
    await wtask


# Test multiple different listeners and one unexpectedly errors
# If one listener unexpectedly errors while the workflow is running, the resource stream should recover,
# move on and continue reporting updates to any other open streams.
@pytest.mark.asyncio
@timeout(3)
async def test_closing_different_identity_streams():
    historian = create_test_history(
        'different_identity_streams',
        lambda: simple_workflow(None, 'private_identity')
    )

    w_task = historian.run()
    await asyncio.gather(
        failing_listener(historian, None),
        simple_listener(historian, 'private_identity', None, 'private_identity')
    )
    await w_task


# Test suspending the workflow while a listener is streaming resource updates.
# The listener should receive a StopAsyncIteration exception after the workflow suspends.
@pytest.mark.asyncio
@timeout(4)
async def test_suspend_workflow():
    historian = create_test_history(
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


# Test streaming resource before and after a workflow suspend
# The listener should receive updates up to the workflow suspend. Once the workflow "resumes",
# the listener should only receive updates pertaining to the workflow after the suspension.
# In other words, the listeners should not receive resource updates during the "replay" stage of the workflow.
@pytest.mark.asyncio
@timeout(4)
async def test_suspend_resume_workflow():
    historian = create_test_history(
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
        phrase2 = wrap_as_state('phrase2', None, historian)

        updates = aiter(resource_stream)
        await anext(updates)  # Get initial snapshot of resources
        resources = await anext(updates)
        assert ('phrase2', None) in resources
        assert await phrase2.value() == 'Goodbye'

        resources = await anext(updates)
        assert ('phrase2', None) in resources
        assert await phrase2.value() == 'Everyone!'

        resources = await anext(updates)
        assert ('phrase1', None) in resources
        assert ('phrase2', None) not in resources

        resources = await anext(updates)
        assert not resources

    await w_task
