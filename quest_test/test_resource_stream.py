import pytest

from quest import Historian
from quest import step
from quest.external import state, queue
from utils import timeout

# TODO:
#  with exception
#  multiple streams for None
#  Multiple tasks listening
#  Multiple tasks listening and one listener leaves -> the gates should be closed

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

@pytest.mark.asyncio
@timeout(3)
async def test_default():
    historian = Historian(
        'default',
        workflow,
        []
    )

    wtask = historian.run()

    with historian.get_resource_stream(None) as resource_stream:
        updates = aiter(resource_stream)
        resources = await anext(updates)  # empty - start of workflow
        assert not resources

        resources = await anext(updates)  # phrase created
        assert 'phrase' in resources
        assert await resources['phrase'].value() == 'woot'

        resources = await anext(updates)  # phrase.get()
        resources = await anext(updates)  # phrase.set(big_phrase())
        assert 'phrase' in resources
        assert await resources['phrase'].value() == 'wootwootwoot'

        resources = await anext(updates)  # messages created
        assert 'phrase' in resources
        assert await resources['phrase'].value() == 'wootwootwoot'

        assert 'messages' in resources
        await resources['messages'].put('quuz')

        resources = await anext(updates)  # messages.get()
        resources = await anext(updates)  # phrase.get()
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

        resources = await anext(updates)  # workflow complete

        try:
            resources = await anext(updates)
            pytest.fail('Should have thrown StopAsyncIteration')
        except StopAsyncIteration:
            pass

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

            raise Exception('Resource stream listener error')
    except Exception as e:
        assert str(e) == 'Resource stream listener error'

    assert historian._resource_stream_manager._resource_streams == {}
    await wtask

    """
    Problem notes:
    When a resource stream listener raises an error, the with context will close up resources
    with the intent of allowing the workflow to continue and not await on the stream_gate
    in resources. But what if the workflow is already waiting on the stream_gate? How do we let
    it move forward without letting it move so far as to re-enter update() before we can clean up?
    """
