import pytest

from src.quest import step, Historian
from src.quest.external import state, queue


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
    historian = Historian('test', workflow, history)
    historian.run()

    index = 0
    async for resources in historian.stream_resources(None):
        if index == 0:  # 'phrase' created
            assert 'phrase' in resources
            assert resources['phrase']['value'] == 'woot'

        elif index == 1:  # phrase.set(big_phrase())
            assert 'phrase' in resources
            assert resources['phrase']['value'] == 'wootwootwoot'

        elif index == 2:  # 'messages' created
            assert 'phrase' in resources
            assert resources['phrase']['value'] == 'wootwootwoot'
            assert 'messages' in resources

            await historian.record_external_event('messages', None, 'put', 'quuz')

        elif index == 3:  # 'phrase.set(+ message)'
            assert 'phrase' in resources
            assert resources['phrase']['value'] == 'wootwootwootquuz'
            assert 'messages' in resources

        elif index == 4:  # 'messages' deleted
            assert 'phrase' in resources
            assert resources['phrase']['value'] == 'wootwootwootquuz'
            assert 'messages' not in resources

        elif index == 5:  # 'phrase.set'
            assert 'phrase' in resources
            assert resources['phrase']['value'] == 'all done'

        elif index == 6:  # 'phrase' deleted
            assert not resources  # i.e. empty
            await historian.suspend()
            return

        index += 1
