import pytest

from quest import step, Historian
from quest.external import state, queue
from quest.serializer import NoopSerializer


@pytest.mark.asyncio
async def test_resource_stream():
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

    history = []
    historian = Historian('test', workflow, history, serializer=NoopSerializer())
    wtask = historian.run()

    updates = aiter(historian.stream_resources(None))
    resources = await anext(updates)  # empty - start of workflow

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
