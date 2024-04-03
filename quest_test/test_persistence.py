import asyncio
from pathlib import Path

import pytest

from src.quest import step
from src.quest.historian import Historian
from src.quest.persistence import LinkedPersistentHistory, ListPersistentHistory, LocalFileSystemBlobStorage
from quest_test.utils import timeout


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
@timeout(3)
async def test_persistence_basic_linked(tmp_path: Path):
    storage = LocalFileSystemBlobStorage(tmp_path)
    history = LinkedPersistentHistory('test', storage)
    historian = Historian(
        'test',
        simple_workflow,
        history
    )

    workflow = historian.run()
    await asyncio.sleep(0.01)
    await historian.suspend()

    pause.set()
    history = LinkedPersistentHistory('test', storage)
    historian = Historian(
        'test',
        simple_workflow,
        history
    )
    result = await historian.run()
    assert result == 14

@pytest.mark.asyncio
@timeout(3)
async def test_persistence_basic_list(tmp_path: Path):
    storage = LocalFileSystemBlobStorage(tmp_path)
    history = ListPersistentHistory('test', storage)
    historian = Historian(
        'test',
        simple_workflow,
        history
    )

    workflow = historian.run()
    await asyncio.sleep(0.01)
    await historian.suspend()

    pause.set()
    history = ListPersistentHistory('test', storage)
    historian = Historian(
        'test',
        simple_workflow,
        history
    )
    result = await historian.run()
    assert result == 14

event = asyncio.Event()

@step
async def blocks():
    await event.wait()
    return 7


async def resume_this_workflow():
    await blocks()


@pytest.mark.asyncio
async def test_resume_step_persistence_linked(tmp_path: Path):
    storage = LocalFileSystemBlobStorage(tmp_path)
    history = LinkedPersistentHistory('test', storage)
    historian = Historian(
        'test',
        resume_this_workflow,
        history
    )

    workflow = historian.run()
    await asyncio.sleep(0.01)
    await historian.suspend()

    event.set()

    storage = LocalFileSystemBlobStorage(tmp_path)
    history = LinkedPersistentHistory('test', storage)
    historian = Historian(
        'test',
        resume_this_workflow,
        history
    )

    await historian.run()

event = asyncio.Event()

@step
async def blocks():
    await event.wait()
    return 7


async def resume_this_workflow():
    await blocks()


@pytest.mark.asyncio
async def test_resume_step_persistence_list(tmp_path: Path):
    storage = LocalFileSystemBlobStorage(tmp_path)
    history = ListPersistentHistory('test', storage)
    historian = Historian(
        'test',
        resume_this_workflow,
        history
    )

    workflow = historian.run()
    await asyncio.sleep(0.01)
    await historian.suspend()

    event.set()

    storage = LocalFileSystemBlobStorage(tmp_path)
    history = ListPersistentHistory('test', storage)
    historian = Historian(
        'test',
        resume_this_workflow,
        history
    )

    await historian.run()
