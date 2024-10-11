
import pytest

from quest import Historian
from quest import step
from quest.external import state, queue
from utils import timeout


# @pytest.mark.asyncio
# async def test_resource_stream():
#     # vanilla
#     # with exception
#     # multiple streams for None
#     # Multiple tasks listening
#     # Multiple tasks listening and one listener leaves -> the gates should be closed
#
#     @step
#     async def big_phrase(phrase):
#         return phrase * 3
#
#     async def workflow():
#         async with state('phrase', None, 'woot') as phrase:
#             await phrase.set(await big_phrase(await phrase.get()))
#
#             # async with queue('messages', None) as messages:
#             #     new_message = await messages.get()
#             #     await phrase.set((await phrase.get()) + new_message)
#
#             await phrase.set('all done')
#
#     w_id = 'input-' + str(uuid.uuid1())
#     async with create_filesystem_manager(
#             Path('test_state'),
#             'resource_stream_test',
#             lambda workflow_id: workflow
#     ) as manager:
#         manager.start_workflow('', w_id)
#         # TODO: Discuss whether stream_resources should be async or not. (And make so in manager, historian, resources)
#         index = 0
#         async for resources in manager.stream_resources(w_id, None):
#             if index == 0:
#                 manager.start_workflow('', w_id)
#             print(f'Resources: {resources}')
#             index += 1
#
#         # This isn't implemented in manager...
#         # await manager.wait_for_completion(w_id, None)


# This is from old version of test stream resources

@pytest.mark.asyncio
@timeout(3)
async def test_vanilla():
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

    historian = Historian(
        'vanilla',
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
