import asyncio
import pytest

from quest import Historian
from quest import step
from quest.external import state, queue
from utils import timeout

"""
Problem Notes:
- What if a new listener joins while updates is in the for each loop? Need to process this also after it exits
"""

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

async def default_stream_listener(historian: Historian):
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

    await default_stream_listener(historian)

    await wtask

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

    # Is there a better way to wait a moment for the __exit function to complete?
    await asyncio.sleep(1)
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
    await asyncio.gather(default_stream_listener(historian),
                         default_stream_listener(historian),
                         default_stream_listener(historian))

    await wtask

@pytest.mark.asyncio
@timeout(3)
async def test_different_identity_streams():
    async def mult_ident_workflow():
        async with state('phrase', None, 'woot') as phrase:
            await phrase.set(await big_phrase(await phrase.get()))

            async with queue('messages', 'kyle') as messages:
                new_message = await messages.get()
                await phrase.set((await phrase.get()) + new_message)

            await phrase.set('all done')

    async def none_listener():
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

            await anext(updates)  # workflow complete

            try:
                await anext(updates)
                pytest.fail('Should have thrown StopAsyncIteration')
            except StopAsyncIteration:
                pass

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

            # Problem: This doesn't work because the run_with_args function in historian doesn't notify anyone but None.
            # Is there a better way to close up resource streams when the workflow completes?
            # await anext(updates)  # workflow complete

            # This also does not work properly? Timeout error is raised not StopAsyncIteration
            # try:
            #     await anext(updates)
            #     pytest.fail('Should have thrown StopAsyncIteration')
            # except StopAsyncIteration:
            #     pass

    historian = Historian(
        'different_identity_streams',
        mult_ident_workflow,
        []
    )

    wtask = historian.run()

    await asyncio.gather(none_listener(), kyle_listener())

    await wtask

@pytest.mark.asyncio
@timeout(3)
async def test_closing_different_identity_streams():
    async def mult_ident_workflow():
        async with state('phrase', None, 'woot') as phrase:
            await phrase.set(await big_phrase(await phrase.get()))

            # Trying to force None to be added to list of streams to close before update for 'kyle' happens
            await asyncio.sleep(2)
            async with queue('messages', 'kyle') as messages:
                new_message = await messages.get()
                await phrase.set((await phrase.get()) + new_message)

            await phrase.set('all done')

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