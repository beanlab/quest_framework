import asyncio
import pytest

from quest import Historian
from quest import step
from quest.external import state, queue
from utils import timeout


# TODO:
#  - Make test to assert public resources do not receive updates to a private resource
#  - Make a test to have a client run on a separate task and just use with: async for...

@step
async def big_phrase(phrase):
    return phrase * 3


async def default_workflow():
    async with state('phrase', None, 'woot') as phrase:
        await phrase.set(await big_phrase(await phrase.get()))

        async with queue('messages', None) as messages:
            new_message = await messages.get()
            await phrase.set((await phrase.get()) + new_message)

        await phrase.set('all done')


async def mult_identity_workflow():  # A workflow with public and private resources
    async with state('phrase', None, 'woot') as phrase:
        await phrase.set(await big_phrase(await phrase.get()))

        async with queue('messages', 'private_identity') as messages:
            new_message = await messages.get()
            await phrase.set((await phrase.get()) + new_message)

        await phrase.set('all done')


async def mult_private_identity_workflow():  # A workflow with two different private resources
    async with state('phrase', 'ident1', 'woot') as phrase:
        await phrase.set(await big_phrase(await phrase.get()))

        async with queue('messages', 'ident2') as messages:
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

        # TODO: Should the workflow complete be a resource event? If so then should workflow suspend be the same?
        # await anext(updates)  # workflow complete

        try:
            await anext(updates)
            pytest.fail('Should have thrown StopAsyncIteration')
        except StopAsyncIteration:
            pass


@pytest.mark.asyncio
@timeout(3)
async def test_default():
    historian = Historian(
        'default',
        default_workflow,
        []
    )

    wtask = historian.run()
    await default_stream_listener(historian, None)
    await wtask


# Test streaming public resources by a private listener
@pytest.mark.asyncio
@timeout(3)
async def test_private_identity_streaming_public_resources():
    historian = Historian(
        'private_identity_streaming_public_resources',
        default_workflow,
        []
    )

    w_task = historian.run()
    await default_stream_listener(historian, 'private_identity')
    await w_task


# Test a resource stream that then errors unexpectedly
@pytest.mark.asyncio
@timeout(3)
async def test_exception():
    historian = Historian(
        'exception',
        default_workflow,
        []
    )

    wtask = historian.run()

    try:
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

            resources = await anext(updates)  # messages created
            assert 'phrase' in resources
            assert await resources['phrase'].value() == 'wootwootwoot'

            assert 'messages' in resources
            await resources['messages'].put('quuz')

            raise Exception('Resource stream listener error')
    except Exception as e:
        assert str(e) == 'Resource stream listener error'

    assert historian._resource_stream_manager._resource_streams == {}
    await wtask


# Test multiple public listeners
@pytest.mark.asyncio
@timeout(3)
async def test_concurrent_none_streams():
    historian = Historian(
        'concurrent_none_streams',
        default_workflow,
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

            # await anext(updates)  # workflow complete

            try:
                await anext(updates)
                pytest.fail('Should have thrown StopAsyncIteration')
            except StopAsyncIteration:
                pass

    historian = Historian(
        'mult_identity_workflow',
        mult_identity_workflow,
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

            # await anext(updates)  # workflow complete

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

            # await anext(updates)  # workflow complete

            try:
                await anext(updates)
                pytest.fail('Should have thrown StopAsyncIteration')
            except StopAsyncIteration:
                pass

    historian = Historian(
        'different_identity_streams',
        mult_private_identity_workflow,
        []
    )

    wtask = historian.run()
    await asyncio.gather(ident1_listener(), ident2_listener())
    await wtask


# Test multiple different listeners and one unexpectedly errors
@pytest.mark.asyncio
@timeout(3)
async def test_closing_different_identity_streams():
    async def public_listener():
        try:
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

                asyncio.create_task(anext(updates))  # Call anext but don't wait for result
                await asyncio.sleep(1)  # Pause execution here so line above can execute
                raise Exception('None listener errored')
        except Exception as e:
            assert str(e) == 'None listener errored'

    historian = Historian(
        'different_identity_streams',
        mult_identity_workflow,
        []
    )

    w_task = historian.run()
    await asyncio.gather(public_listener(), default_stream_listener(historian, 'private_identity'))
    await w_task


@pytest.mark.asyncio
@timeout(4)
async def test_suspend_workflow():
    historian = Historian(
        'test_suspend_workflow',
        default_workflow,
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
