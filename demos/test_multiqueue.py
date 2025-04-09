import pytest
import asyncio
from multi_guess_server import MultiQueue, SingleResponseMultiQueue
from quest.external import Queue
from quest_test.utils import create_in_memory_workflow_manager


@pytest.mark.asyncio
async def test_multiqueue_multiple_responses():
    players = ['a', 'b']
    responses_per_player = 2

    queues = {ident: Queue() for ident in players}
    for i in range(responses_per_player):
        for ident in players:
            await queues[ident].put(f'{ident}-{i}')

    results = []
    expected_responses = len(players) * responses_per_player

    async def dummy_workflow():
        async with MultiQueue(queues) as mq:
            async for ident, val in mq:
                results.append((ident, val))
                if len(results) == expected_responses:  # 2 responses per 2 players
                    break

    workflows = {"dummy_workflow": dummy_workflow}
    manager = create_in_memory_workflow_manager(workflows=workflows)

    async with manager:
        manager.start_workflow("dummy_workflow", "dummy-id")
        await manager.get_workflow_result("dummy-id")

    assert len(results) == expected_responses


@pytest.mark.asyncio
async def test_singleresponsemultiqueue_one_response_only():
    players = ['x', 'y', 'z']
    queues = {ident: Queue() for ident in players}

    for ident in players:
        await queues[ident].put(f'{ident}-guess')
        await queues[ident].put(f'{ident}-No')  # should not be received to results

    results = []

    async def dummy_workflow():
        async with SingleResponseMultiQueue(queues) as mq:
            async for ident, val in mq:
                results.append((ident, val))

    workflows = {"dummy_workflow": dummy_workflow}
    manager = create_in_memory_workflow_manager(workflows=workflows)

    async with manager:
        manager.start_workflow("dummy_workflow", "dummy-id")
        await manager.get_workflow_result("dummy-id")

    # One response per player
    assert len(results) == len(players)

    # Ensure unnecessary guesses were not received
    assert all("-No" not in val for _, val in results)


@pytest.mark.asyncio
async def test_multiqueue_remove():
    players = ['x', 'y']
    responses_per_player = 2
    queues = {ident: Queue() for ident in players}

    for i in range(responses_per_player):
        for ident in players:
            await queues[ident].put(f'{ident}-{i}')

    results = []

    async def dummy_workflow():
        async with MultiQueue(queues) as mq:
            async for ident, val in mq:
                results.append((ident, val))

                if ident == 'x' and 'x' in mq.ident_to_task:
                    mq.remove('x')

                if len(results) == 3:
                    break

    workflows = {"dummy_workflow": dummy_workflow}
    manager = create_in_memory_workflow_manager(workflows=workflows)

    async with manager:
        manager.start_workflow("dummy_workflow", "dummy-id")
        await manager.get_workflow_result("dummy-id")

    assert ('x', 'x-0') in results
    assert ('x', 'x-1') not in results
    y_vals = [val for ident, val in results if ident == 'y']
    assert len(y_vals) == 2
#
# @pytest.mark.asyncio
# async def test_multiqueue_multiple_responses():
#     players = ['a', 'b']
#     responses_per_player = 2
#     queues = {ident: Queue() for ident in players}
#
#     # Put multiple items into each queue
#     for i in range(responses_per_player):
#         for ident in players:
#             await queues[ident].put(f'{ident}-{i}')
#
#     results = []
#
#     expected_responses = len(players) * responses_per_player
#
#     async with MultiQueue(queues) as mq:
#         async for ident, val in mq:
#             results.append((ident, val))
#             if len(results) == expected_responses:  # 2 responses per 2 players
#                 break
#
#     assert len(results) == 4
#
#
# @pytest.mark.asyncio
# async def test_singleresponsemultiqueue_one_response_only():
#     players = ['x', 'y', 'z']
#     queues = {ident: Queue() for ident in players}
#
#     for ident in players:
#         await queues[ident].put(f'{ident}-guess')
#         await queues[ident].put(f'{ident}-No')  # should not be received to results
#
#     results = []
#
#     async with SingleResponseMultiQueue(queues) as mq:
#         async for ident, val in mq:
#             results.append((ident, val))
#
#     # One response per player
#     assert len(results) == len(players)
#
#     # Ensure unnecessary guesses were not received
#     assert all("-No" not in val for _, val in results)
#
#
# @pytest.mark.asyncio
# async def test_multiqueue_remove():
#     players = ['x', 'y']
#     responses_per_player = 2
#     queues = {ident: Queue() for ident in players}
#
#     for i in range(responses_per_player):
#         for ident in players:
#             await queues[ident].put(f'{ident}-{i}')
#
#     results = []
#     remove_next_time = False  # flag for delaying removal
#
#     async with MultiQueue(queues) as mq:
#         async for ident, val in mq:
#             results.append((ident, val))
#
#             # Delay remove -> 'x' has been re-added
#             if remove_next_time:
#                 mq.remove('x')
#                 remove_next_time = False
#
#             # After first x message, mark x for removal
#             if ident == 'x':
#                 remove_next_time = True
#
#             if len(results) == 3:
#                 break
#
#     assert ('x', 'x-0') in results
#     assert ('x', 'x-1') not in results
#     y_vals = [val for ident, val in results if ident == 'y']
#     assert len(y_vals) == 2
#
