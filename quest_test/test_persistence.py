import asyncio
from pathlib import Path

import pytest

from src.quest import step
from src.quest.historian import Historian
from src.quest.persistence import PersistentHistory, LocalFileSystemBlobStorage


@step
async def simple_step():
    return 7


pause = asyncio.Event()


async def simple_workflow():
    foo = await simple_step()
    await pause.wait()
    foo += await simple_step()
    return foo


@pytest.mark.asyncio
async def test_persistence_basic(tmp_path: Path):
    storage = LocalFileSystemBlobStorage(tmp_path)
    history = PersistentHistory('test', storage)
    historian = Historian(
        'test',
        simple_workflow,
        history
    )

    workflow = asyncio.create_task(historian.run())
    await asyncio.sleep(0.01)
    await historian.suspend()

    pause.set()
    history = PersistentHistory('test', storage)
    historian = Historian(
        'test',
        simple_workflow,
        history
    )
    result = await historian.run()
    assert result == 14
