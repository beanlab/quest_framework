import asyncio
from history.utils import history_logger
import pytest

from history import PersistentList, queue, state, event
from history.historian import Historian
from history.persistence import InMemoryBlobStorage
from history.serializer import NoopSerializer


@pytest.mark.asyncio
async def test_historian():
    storage = InMemoryBlobStorage()
    books = {}

    def create_book(wid: str):
        if wid not in books:
            books[wid] = PersistentList(wid, InMemoryBlobStorage())
        return books[wid]

    pause = asyncio.Event()
    counter_a = 0
    counter_b = 0

    async def workflow(arg):
        nonlocal counter_a, counter_b

        history_logger.info('workflow started')
        counter_a += 1

        await pause.wait()
        history_logger.info('workflow passed pause')
        counter_b += 1

        return 7 + arg

    async with Historian('test-manager', storage, create_book, lambda w_type: workflow,
                         serializer=NoopSerializer()) as historian:
        historian.start_soon('workflow', 'wid1', 4, delete_on_finish=False)
        await asyncio.sleep(0.1)
        # Now pause the historian and all workflows

    assert 'wid1' in books
    assert counter_a == 1
    assert counter_b == 0

    async with Historian('test-manager', storage, create_book, lambda w_type: workflow,
                         serializer=NoopSerializer()) as historian:
        # At this point, all workflows should be resumed
        pause.set()
        await asyncio.sleep(0.1)
        result = await historian.get_result('wid1')
        assert result == 11

    assert counter_a == 2
    assert counter_b == 1


@pytest.mark.asyncio
async def test_historian_events():
    storage = InMemoryBlobStorage()
    books = {}

    def create_book(wid: str):
        if wid not in books:
            books[wid] = PersistentList(wid, InMemoryBlobStorage())
        return books[wid]

    counter_a = 0
    counter_b = 0

    async def workflow(arg: int):
        nonlocal counter_a, counter_b
        total = arg

        history_logger.info('workflow started')
        counter_a += 1

        async with queue('messages', None) as Q:
            while True:
                message = await Q.get()
                history_logger.info(f'message received: {message}')
                counter_b += 1

                if message == 0:
                    return total

                total += message

    async with Historian('test-manager', storage, create_book, lambda w_type: workflow,
                         serializer=NoopSerializer()) as historian:
        historian.start_soon('workflow', 'wid1', 1, delete_on_finish=False)
        await asyncio.sleep(0.1)
        await historian.send_event('wid1', 'messages', None, 'put', 2)
        await asyncio.sleep(0.1)
        # Now pause the manager and all workflows

    assert 'wid1' in books
    assert counter_a == 1
    assert counter_b == 1

    async with Historian('test-manager', storage, create_book, lambda w_type: workflow,
                         serializer=NoopSerializer()) as historian:
        # At this point, all workflows should be resumed
        await asyncio.sleep(0.1)
        await historian.send_event('wid1', 'messages', None, 'put', 3)
        await historian.send_event('wid1', 'messages', None, 'put', 0)  # i.e. end the workflow
        result = await historian.get_result('wid1')
        assert result == 6

    assert counter_a == 2
    assert counter_b == 4  # 2, replay 2, 3, 0


@pytest.mark.asyncio
async def test_historian_background():
    storage = InMemoryBlobStorage()
    books = {}

    def create_book(wid: str):
        if wid not in books:
            books[wid] = PersistentList(wid, InMemoryBlobStorage())
        return books[wid]

    counter_a = 0
    counter_b = 0
    total = 0

    async def workflow(arg: int):
        nonlocal counter_a, counter_b, total
        total = arg

        history_logger.info('workflow started')
        counter_a += 1

        async with queue('messages', None) as Q:
            while True:
                message = await Q.get()
                history_logger.info(f'message received: {message}')
                counter_b += 1

                if message == 0:
                    return

                total += message

    async with Historian('test-manager', storage, create_book, lambda w_type: workflow,
                         serializer=NoopSerializer()) as historian:
        historian.start_soon('workflow', 'wid1', 1)
        await asyncio.sleep(0.1)
        await historian.send_event('wid1', 'messages', None, 'put', 2)
        await asyncio.sleep(0.1)
        # Now pause the historian and all workflows

    assert 'wid1' in books
    assert counter_a == 1
    assert counter_b == 1

    async with Historian('test-manager', storage, create_book, lambda w_type: workflow,
                         serializer=NoopSerializer()) as historian:
        # At this point, all workflows should be resumed
        await asyncio.sleep(0.1)
        await historian.send_event('wid1', 'messages', None, 'put', 3)
        await historian.send_event('wid1', 'messages', None, 'put', 0)  # i.e. end the workflow
        await asyncio.sleep(0.1)  # workflow now finishes and removes itself
        assert not historian.has('wid1')
        assert total == 6

    assert counter_a == 2
    assert counter_b == 4  # 2, replay 2, 3, 0


@pytest.mark.asyncio
async def test_get_queue():
    async def workflow():
        async with queue('messages', None) as q, \
                state('result', None, None) as result, \
                event('finish', None) as finish:
            a = await q.get()
            b = await q.get()
            await result.set(a + b)
            await finish.wait()

    storage = InMemoryBlobStorage()

    def create_book(wid: str):
        return PersistentList(wid, InMemoryBlobStorage())

    async with Historian('test', storage, create_book, lambda wid: workflow,
                         serializer=NoopSerializer()) as historian:
        historian.start_soon('workflow', 'wid', delete_on_finish=False)
        await asyncio.sleep(0.1)
        q = await historian.get_queue('wid', 'messages', None)
        result = await historian.get_state('wid', 'result', None)
        finish = await historian.get_event('wid', 'finish', None)

        assert await result.get() is None
        await q.put(3)
        await q.put(4)
        await asyncio.sleep(0.1)
        assert await result.get() == 7
        await finish.set()
