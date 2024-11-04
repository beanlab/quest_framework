import asyncio
from pathlib import Path
from typing import Callable
from dotenv import load_dotenv # TODO: Remove, how should we load env in the test?

import pytest

from quest import step
from quest.historian import Historian
from quest.persistence import PersistentHistory, LocalFileSystemBlobStorage, SQLDatabase, DynamoDB, SqlBlobStorage, DynamoDBBlobStorage
from utils import timeout, mock_aws_session

def create_filesystem_storage(path: Path):
    return LocalFileSystemBlobStorage(path)

def create_sql_storage(path: Path):
    database = SQLDatabase('sqlite:///:memory:')
    return SqlBlobStorage(path.name, database.get_session())

def create_dynamodb_storage(path: Path):
    env_path = Path('../.env')
    load_dotenv(dotenv_path=env_path)
    dynamodb = DynamoDB(mock_aws_session) # TODO: Not quite sure how to test this without loading the env
    return DynamoDBBlobStorage(path.name, dynamodb.get_table())

storage_funcs = [
    # create_filesystem_storage,
    # create_sql_storage,
    create_dynamodb_storage
]

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
@pytest.mark.parametrize("storage_func", storage_funcs)
@timeout(6)
async def test_persistence_basic(tmp_path: Path, storage_func: Callable):
    storage = storage_func(tmp_path)
    history = PersistentHistory('test', storage)
    historian = Historian(
        'test',
        simple_workflow,
        history
    )

    workflow = historian.run()
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


event = asyncio.Event()


@step
async def blocks():
    await event.wait()
    return 7


async def resume_this_workflow():
    await blocks()


@pytest.mark.asyncio
@pytest.mark.parametrize("storage_func", storage_funcs)
async def test_resume_step_persistence(tmp_path: Path, storage_func: Callable):
    storage = storage_func(tmp_path)
    history = PersistentHistory('test', storage)
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
    history = PersistentHistory('test', storage)
    historian = Historian(
        'test',
        resume_this_workflow,
        history
    )

    await historian.run()
