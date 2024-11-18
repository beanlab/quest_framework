import asyncio
from ctypes.wintypes import HTASK

import pytest

from quest import Historian
from quest import step
from quest.external import state, queue
from utils import timeout


@step
async def big_phrase(phrase):
    return phrase * 3


async def workflow(phrase_ident, messages_ident):
    async with state('phrase', phrase_ident, 'woot') as phrase:
        await phrase.set(await big_phrase(await phrase.get()))

        async with queue('messages', messages_ident) as messages:
            new_message = await messages.get()
            await phrase.set((await phrase.get()) + new_message)

        await phrase.set('all done')


async def default_stream_listener(historian: Historian, identity):
    with historian.get_resource_stream(identity) as resource_stream:
        updates = aiter(resource_stream)
        resources = await anext(updates)  # empty - start of workflow
        assert not resources

        resources = await anext(updates)  # phrase created
        assert 'phrase' in resources
        assert await resources['phrase'].value() == 'woot'

        await anext(updates)  # phrase.get()
        resources = await anext(updates)  # phrase.set(big_phrase())
        assert 'phrase' in resources
        assert await resources['phrase'].value() == 'wootwootwoot'

        resources = await anext(updates)  # messages created
        assert 'phrase' in resources
        assert await resources['phrase'].value() == 'wootwootwoot'

        assert 'messages' in resources
        await resources['messages'].put('quuz')

        await anext(updates)  # messages.get()
        await anext(updates)  # phrase.get()
        resources = await anext(updates)  # phrase.set(+ message)
        assert 'phrase' in resources
        assert await resources['phrase'].value() == 'wootwootwootquuz'
        assert 'messages' in resources

        resources = await anext(updates)  # messages deleted
        assert 'phrase' in resources
        assert await resources['phrase'].value() == 'wootwootwootquuz'
        assert 'messages' not in resources

        resources = await anext(updates)  # phrase.set
        assert 'phrase' in resources
        assert await resources['phrase'].value() == 'all done'

        resources = await anext(updates)  # phrase deleted
        assert not resources  # i.e. empty

        try:
            await anext(updates)
            pytest.fail('Should have thrown StopAsyncIteration')
        except StopAsyncIteration:
            pass


async def failing_listener(historian: Historian, identity):
    try:
        with historian.get_resource_stream(identity) as resource_stream:
            i = 0
            put_message = False
            async for resources in resource_stream:
                if 'messages' in resources and not put_message:
                    await resources['messages'].put('quuz')
                if i == 5:
                    raise Exception("Resource Stream listener error")
                i += 1
    except Exception as e:
        assert str(e) == "Resource Stream listener error"


@pytest.mark.asyncio
@timeout(3)
async def test_typical():
    historian = Historian(
        'typical',
        lambda: workflow(None, None),
        []
    )

    async def run_workflow():
        w_task = historian.run()
        await w_task

    # Demonstrates what a typical listener would look like
    async def typical_listener():
        have_put_message = False

        with historian.get_resource_stream(None) as resource_stream:
            async for resources in resource_stream:
                if 'phrase' in resources:
                    phrase = resources['phrase'].value()
                    print(f"Here is the current phrase value: {phrase}")
                if 'messages' in resources and not have_put_message:
                    await resources['messages'].put('quuz')

    # Asyncio.gather runs code in different tasks, demonstrating that while the workflow is executing on one task,
    # it is reporting resource updates to the listener on a separate task
    await asyncio.gather(run_workflow(), typical_listener())


@pytest.mark.asyncio
@timeout(3)
async def test_default():
    historian = Historian(
        'default',
        lambda: workflow(None, None),
        []
    )

    wtask = historian.run()
    await default_stream_listener(historian, None)
    await wtask


# Test a private listener streaming public resources
@pytest.mark.asyncio
@timeout(3)
async def test_private_identity_streaming_public_resources():
    historian = Historian(
        'private_identity_streaming_public_resources',
        lambda: workflow(None, None),
        []
    )

    w_task = historian.run()
    await default_stream_listener(historian, 'private_identity')
    await w_task


# Test that a public listener does not receive updates to private resources
@pytest.mark.asyncio
@timeout(3)
async def test_public_streaming_private_resources():
    historian = Historian(
        'public_streaming_private_resources',
        lambda: workflow('private_identity', 'private_identity'),
        []
    )

    async def public_listener():
        with historian.get_resource_stream(None) as resource_stream:
            async for resources in resource_stream:
                assert not resources

    w_task = historian.run()
    await asyncio.gather(public_listener(), default_stream_listener(historian, 'private_identity'))
    await w_task


# Test a resource stream that then errors unexpectedly
@pytest.mark.asyncio
@timeout(3)
async def test_exception():
    historian = Historian(
        'exception',
        lambda: workflow(None, None),
        []
    )

    wtask = historian.run()

    await failing_listener(historian, None)

    assert historian._resource_stream_manager._resource_streams == {}
    await wtask


# Test multiple public listeners
@pytest.mark.asyncio
@timeout(3)
async def test_concurrent_none_streams():
    historian = Historian(
        'concurrent_none_streams',
        lambda: workflow(None, None),
        []
    )

    wtask = historian.run()

    # Run multiple streams concurrently
    await asyncio.gather(default_stream_listener(historian, None),
                         default_stream_listener(historian, None),
                         default_stream_listener(historian, None))

    await wtask


# Test streaming public and private resources by a public and private listener respectively
@pytest.mark.asyncio
@timeout(3)
async def test_mult_identity_workflow():
    async def public_listener():
        with historian.get_resource_stream(None) as resource_stream:
            updates = aiter(resource_stream)
            resources = await anext(updates)  # empty - start of workflow
            assert not resources

            resources = await anext(updates)  # phrase created
            assert 'phrase' in resources
            assert await resources['phrase'].value() == 'woot'

            await anext(updates)  # phrase.get()
            resources = await anext(updates)  # phrase.set(big_phrase())
            assert 'phrase' in resources
            assert await resources['phrase'].value() == 'wootwootwoot'

            await anext(updates)  # phrase.get()
            resources = await anext(updates)  # phrase.set(+ message)
            assert 'phrase' in resources
            assert await resources['phrase'].value() == 'wootwootwootquuz'

            resources = await anext(updates)  # phrase.set
            assert 'phrase' in resources
            assert await resources['phrase'].value() == 'all done'

            resources = await anext(updates)  # phrase deleted
            assert not resources  # i.e. empty

            try:
                await anext(updates)
                pytest.fail('Should have thrown StopAsyncIteration')
            except StopAsyncIteration:
                pass

    historian = Historian(
        'mult_identity_workflow',
        lambda: workflow(None, 'private_identity'),
        []
    )

    w_task = historian.run()
    await asyncio.gather(public_listener(), default_stream_listener(historian, 'private_identity'))
    await w_task


# Test streaming multiple private resources by two different private listeners
@pytest.mark.asyncio
@timeout(3)
async def test_multiple_private_identity_streams():
    async def ident1_listener():
        with historian.get_resource_stream('ident1') as resource_stream:
            updates = aiter(resource_stream)
            resources = await anext(updates)  # empty - start of workflow
            assert not resources

            resources = await anext(updates)  # phrase created
            assert 'phrase' in resources
            assert await resources['phrase'].value() == 'woot'

            await anext(updates)  # phrase.get()
            resources = await anext(updates)  # phrase.set(big_phrase())
            assert 'phrase' in resources
            assert await resources['phrase'].value() == 'wootwootwoot'

            await anext(updates)  # phrase.get()
            resources = await anext(updates)  # phrase.set(+ message)
            assert 'phrase' in resources
            assert await resources['phrase'].value() == 'wootwootwootquuz'

            resources = await anext(updates)  # phrase.set
            assert 'phrase' in resources
            assert await resources['phrase'].value() == 'all done'

            resources = await anext(updates)  # phrase deleted
            assert not resources  # i.e. empty

            try:
                await anext(updates)
                pytest.fail('Should have thrown StopAsyncIteration')
            except StopAsyncIteration:
                pass

    async def ident2_listener():
        with historian.get_resource_stream('ident2') as resource_stream:
            updates = aiter(resource_stream)
            resources = await anext(updates)  # empty - start of workflow
            assert not resources

            resources = await anext(updates)  # messages created
            assert 'messages' in resources
            await resources['messages'].put('quuz')

            await anext(updates)  # messages.get()
            assert 'messages' in resources

            resources = await anext(updates)  # messages deleted
            assert 'messages' not in resources

            try:
                await anext(updates)
                pytest.fail('Should have thrown StopAsyncIteration')
            except StopAsyncIteration:
                pass

    historian = Historian(
        'different_identity_streams',
        lambda: workflow('ident1', 'ident2'),
        []
    )

    wtask = historian.run()
    await asyncio.gather(ident1_listener(), ident2_listener())
    await wtask


# Test multiple different listeners and one unexpectedly errors
@pytest.mark.asyncio
@timeout(3)
async def test_closing_different_identity_streams():
    historian = Historian(
        'different_identity_streams',
        lambda: workflow(None, 'private_identity'),
        []
    )

    w_task = historian.run()
    await asyncio.gather(
        failing_listener(historian, None),
        default_stream_listener(historian, 'private_identity')
    )
    await w_task


@pytest.mark.asyncio
@timeout(4)
async def test_suspend_workflow():
    historian = Historian(
        'test_suspend_workflow',
        lambda: workflow(None, None),
        []
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
