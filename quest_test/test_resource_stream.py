import pytest

from src.quest import step, Historian
from src.quest.external import state, queue


#TODO: - make a test that ensures that when an external party reads a queue that the workflow populates, it is sane
# - The workflow puts stuff on a queue
# - An external party reads from the queue
# - The workflow suspends
# - The workflow resumes
# - The external party does not see the already-consumed items on the queue, but does see any outstanding items
# Workflow        External
# 7 -> Q
# 8 -> Q
#                 Q -> 7
# 9 -> Q
# -- Suspend and resume --
#                 Q -> 8  (not a 7!)
#                 Q -> 9

@pytest.mark.asyncio
async def test_resource_stream():
    # Strategy
    # Workflow            External
    # --------------------------------
    # phrase created
    # phrase set
    # messages created
    # messages.get()
    # phrase set
    # messages destroyed
    # phrase set
    # phrase destroyed

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

        elif index == 1:  # skip yield for 'phrase.get'
            pass  # TODO: Assert the same as index == 0

        elif index == 2:  # phrase.set(big_phrase())
            assert 'phrase' in resources
            assert resources['phrase']['value'] == 'wootwootwoot'

        elif index == 3:  # 'messages' created
            assert 'phrase' in resources
            assert resources['phrase']['value'] == 'wootwootwoot'
            assert 'messages' in resources

            await historian.record_external_event('messages', None, 'put', 'quuz')

        elif index == 4:  # skip yield for 'messages.get'
            pass

        elif index == 5:  # skip yield for 'phrase.get'
            pass

        elif index == 6:  # 'phrase.set(+ message)'
            assert 'phrase' in resources
            assert resources['phrase']['value'] == 'wootwootwootquuz'
            assert 'messages' in resources

        elif index == 7:  # 'messages' deleted
            assert 'phrase' in resources
            assert resources['phrase']['value'] == 'wootwootwootquuz'
            assert 'messages' not in resources

        elif index == 8:  # 'phrase.set'
            assert 'phrase' in resources
            assert resources['phrase']['value'] == 'all done'

        elif index == 9: # 'phrase' deleted
            assert not resources  # i.e. empty
            await historian.suspend()
            return

        index += 1
