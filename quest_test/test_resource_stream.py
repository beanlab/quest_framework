import asyncio
import pytest

from quest import Historian
from quest import step
from quest.external import state, queue
from utils import timeout

@step
async def big_phrase(phrase):
    return phrase * 3


async def workflow():
    async with state('phrase', None, 'woot') as phrase:
        await phrase.set(await big_phrase(await phrase.get()))

        async with queue('messages', None) as messages:
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

        await anext(updates)  # workflow complete

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
        workflow,
        []
    )

    wtask = historian.run()

    await default_stream_listener(historian, None)

    await wtask

@pytest.mark.asyncio
@timeout(3)
async def test_default_with_identity():
    historian = Historian(
        'default_with_identity',
        workflow,
        []
    )

    w_task = historian.run()
    await default_stream_listener(historian, 'test_identity')
    await w_task

@pytest.mark.asyncio
@timeout(3)
async def test_exception():
    historian = Historian(
        'exception',
        workflow,
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

@pytest.mark.asyncio
@timeout(3)
async def test_concurrent_none_streams():
    historian = Historian(
        'concurrent_none_streams',
        workflow,
        []
    )

    wtask = historian.run()

    # Run multiple streams concurrently
    await asyncio.gather(default_stream_listener(historian, None),
                         default_stream_listener(historian, None),
                         default_stream_listener(historian, None))

    await wtask

@pytest.mark.asyncio
@timeout(3)
async def test_multiple_private_identity_streams():
    async def mult_ident_workflow():
        async with state('phrase', 'ident1', 'woot') as phrase:
            await phrase.set(await big_phrase(await phrase.get()))

            async with queue('messages', 'ident2') as messages:
                new_message = await messages.get()
                await phrase.set((await phrase.get()) + new_message)

            await phrase.set('all done')

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

            await anext(updates)  # workflow complete

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

            await anext(updates)  # workflow complete

            try:
                await anext(updates)
                pytest.fail('Should have thrown StopAsyncIteration')
            except StopAsyncIteration:
                pass

    historian = Historian(
        'different_identity_streams',
        mult_ident_workflow,
        []
    )

    wtask = historian.run()

    await asyncio.gather(ident1_listener(), ident2_listener())

    await wtask

"""
This test shows that with this implementation of closing and opening streams, it is possible for a stream of a one
identity to be tagged for close while an update of another identity attempts to close streams of identities of its own.
Specifically here we call updates on the None identity after there are no more updates to be made for None. This
allows the call to update for None to exit after the stream_gate is set by the call to anext.
Before waiting for a result to anext, the None listener encounters an error and closes, signaling the stream for close.
An update for kyle is called and attempts closing streams tagged for close, one of which is of None identity.
"""
@pytest.mark.asyncio
@timeout(3)
async def test_closing_different_identity_streams():
    async def mult_ident_workflow():
        async with state('phrase', None, 'woot') as phrase:
            await phrase.set(await big_phrase(await phrase.get()))

        # Allow None to be added to list of streams to close before update for 'kyle' happens
        await asyncio.sleep(1)
        async with queue('messages', 'kyle') as messages:
            new_message = await messages.get()

    async def none_listener():
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

                await anext(updates)  # phrase deleted
                asyncio.create_task(anext(updates))  # Force workflow to continue but don't wait on result
                await asyncio.sleep(1)  # Allow time for anext to be called
                raise Exception('None listener errored')
        except Exception as e:
            assert str(e) == 'None listener errored'

    async def kyle_listener():
        with historian.get_resource_stream('kyle') as resource_stream:
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

    historian = Historian(
        'different_identity_streams',
        mult_ident_workflow,
        []
    )

    wtask = historian.run()

    await asyncio.gather(none_listener(), kyle_listener())

    await wtask