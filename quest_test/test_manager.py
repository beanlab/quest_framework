import asyncio

import pytest

from src.quest import PersistentHistory
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

    async def workflow(arg):
        await pause.wait()
        return 7 + arg

    async with WorkflowManager('test-manager', storage, create_history, lambda w_type: workflow) as manager:
        manager.start_workflow('workflow', 'wid1', 4)

    assert 'wid1' in histories

    async with WorkflowManager('test-manager', storage, create_history, lambda w_type: workflow) as manager:
        pause.set()
        resources = manager.get_resources('wid1', None)
        assert resources

