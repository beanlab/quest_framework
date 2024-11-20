import asyncio
import os
from pathlib import Path

import pytest

from quest import step
from quest.historian import Historian
from quest.persistence import PersistentHistory, LocalFileSystemBlobStorage
from quest.serializer import NoopSerializer
from utils import timeout


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
async def test_persistence_basic(tmp_path: Path):
    storage = LocalFileSystemBlobStorage(tmp_path)
    history = PersistentHistory('test', storage)
    historian = Historian(
        'test',
        simple_workflow,
        history,
        serializer=NoopSerializer()
    )

    workflow = historian.run()
    await asyncio.sleep(0.01)
    await historian.suspend()

    pause.set()
    history = PersistentHistory('test', storage)
    historian = Historian(
        'test',
        simple_workflow,
        history,
        serializer=NoopSerializer()
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
async def test_resume_step_persistence(tmp_path: Path):
    storage = LocalFileSystemBlobStorage(tmp_path)
    history = PersistentHistory('test', storage)
    historian = Historian(
        'test',
        resume_this_workflow,
        history,
        serializer=NoopSerializer()
    )

    workflow = historian.run()
    await asyncio.sleep(0.01)
    await historian.suspend()

    event.set()

    storage = LocalFileSystemBlobStorage(tmp_path)
    history = PersistentHistory('test', storage)
    historian = Historian(
        'test',
        resume_this_workflow,
        history,
        serializer=NoopSerializer()
    )

    await historian.run()


def check_directory_empty(root_directory):
    for dirpath, dirnames, filenames in os.walk(root_directory):
        if filenames:
            return False
    return True


@pytest.mark.asyncio
async def test_workflow_cleanup():
    path = Path('test')
    storage = LocalFileSystemBlobStorage(path)
    history = PersistentHistory('test', storage)
    historian = Historian(
        'test',
        simple_workflow,
        history,
        serializer=NoopSerializer()
    )

    pause.set()
    await historian.run()

    assert check_directory_empty(path)
