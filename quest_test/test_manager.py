import asyncio
import logging

import pytest

from src.quest import PersistentHistory, queue
from src.quest.manager import WorkflowManager
from src.quest.persistence import InMemoryBlobStorage


@pytest.mark.asyncio
async def test_manager():
    storage = InMemoryBlobStorage()
    histories = {}

    def create_history(wid: str):
        if wid not in histories:
            histories[wid] = PersistentHistory(wid, InMemoryBlobStorage())
        return histories[wid]

    pause = asyncio.Event()
    counter_a = 0
    counter_b = 0

    async def workflow(arg):
        nonlocal counter_a, counter_b

        logging.info('workflow started')
        counter_a += 1

        await pause.wait()
        logging.info('workflow passed pause')
        counter_b += 1

        return 7 + arg

    async with WorkflowManager('test-manager', storage, create_history, lambda w_type: workflow) as manager:
        manager.start_workflow('workflow', 'wid1', 4)
        await asyncio.sleep(0.1)
        # Now pause the manager and all workflows

    assert 'wid1' in histories
    assert counter_a == 1
    assert counter_b == 0

    async with WorkflowManager('test-manager', storage, create_history, lambda w_type: workflow) as manager:
        # At this point, all workflows should be resumed
        pause.set()
        await asyncio.sleep(0.1)
        result = await manager.get_workflow('wid1')
        assert result == 11

    assert counter_a == 2
    assert counter_b == 1


@pytest.mark.asyncio
async def test_manager_events():
    storage = InMemoryBlobStorage()
    histories = {}

    def create_history(wid: str):
        if wid not in histories:
            histories[wid] = PersistentHistory(wid, InMemoryBlobStorage())
        return histories[wid]

    counter_a = 0
    counter_b = 0

    async def workflow(arg: int):
        nonlocal counter_a, counter_b
        total = arg

        logging.info('workflow started')
        counter_a += 1

        async with queue('messages', None) as Q:
            while True:
                message = await Q.get()
                logging.info(f'message received: {message}')
                counter_b += 1

                if message == 0:
                    return total

                total += message

    async with WorkflowManager('test-manager', storage, create_history, lambda w_type: workflow) as manager:
        manager.start_workflow('workflow', 'wid1', 1)
        await asyncio.sleep(0.1)
        await manager.send_event('wid1', 'messages', None, 'put', 2)
        await asyncio.sleep(0.1)
        # Now pause the manager and all workflows

    assert 'wid1' in histories
    assert counter_a == 1
    assert counter_b == 1

    async with WorkflowManager('test-manager', storage, create_history, lambda w_type: workflow) as manager:
        # At this point, all workflows should be resumed
        await asyncio.sleep(0.1)
        await manager.send_event('wid1', 'messages', None, 'put', 3)
        await manager.send_event('wid1', 'messages', None, 'put', 0)  # i.e. end the workflow
        result = await manager.get_workflow('wid1')
        assert result == 6

    assert counter_a == 2
    assert counter_b == 4  # 2, replay 2, 3, 0


@pytest.mark.asyncio
async def test_manager_background():
    storage = InMemoryBlobStorage()
    histories = {}

    def create_history(wid: str):
        if wid not in histories:
            histories[wid] = PersistentHistory(wid, InMemoryBlobStorage())
        return histories[wid]

    counter_a = 0
    counter_b = 0
    total = 0

    async def workflow(arg: int):
        nonlocal counter_a, counter_b, total
        total = arg

        logging.info('workflow started')
        counter_a += 1

        async with queue('messages', None) as Q:
            while True:
                message = await Q.get()
                logging.info(f'message received: {message}')
                counter_b += 1

                if message == 0:
                    return

                total += message

    async with WorkflowManager('test-manager', storage, create_history, lambda w_type: workflow) as manager:
        manager.start_workflow_background('workflow', 'wid1', 1)
        await asyncio.sleep(0.1)
        await manager.send_event('wid1', 'messages', None, 'put', 2)
        await asyncio.sleep(0.1)
        # Now pause the manager and all workflows

    assert 'wid1' in histories
    assert counter_a == 1
    assert counter_b == 1

    async with WorkflowManager('test-manager', storage, create_history, lambda w_type: workflow) as manager:
        # At this point, all workflows should be resumed
        await asyncio.sleep(0.1)
        await manager.send_event('wid1', 'messages', None, 'put', 3)
        await manager.send_event('wid1', 'messages', None, 'put', 0)  # i.e. end the workflow
        await asyncio.sleep(0.1)  # workflow now finishes and removes itself
        assert not manager.has_workflow('wid1')
        assert total == 6

    assert counter_a == 2
    assert counter_b == 4  # 2, replay 2, 3, 0
