import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Callable
from dotenv import load_dotenv

import pytest

from quest import SqlBlobStorage
from quest import step
from quest.historian import Historian
from quest.persistence import PersistentHistory, LocalFileSystemBlobStorage, DynamoDB
from utils import timeout
from quest.sql import SQLDatabase, SqlBlobStorage


def create_filesystem_storage(path: Path):
    return LocalFileSystemBlobStorage(path)

def create_sql_storage(path: Path):
    database = SQLDatabase('sqlite:///:memory:')
    return SqlBlobStorage(path.name, database.get_session())

def create_dynamodb_storage(path: Path):
    env_path = Path('.env.integration')
    load_dotenv(dotenv_path=env_path)
    dynamodb = DynamoDB()
    return DynamoDBBlobStorage(path.name, dynamodb.get_table())

storage_funcs = [
    # create_filesystem_storage,
    create_sql_storage,
    # create_dynamodb_storage
]

class FileSystemStorageContext:
    def __enter__(self):
        self.tmp_dir = Path(TemporaryDirectory().__enter__())
        storage = LocalFileSystemBlobStorage(self.tmp_dir)
        return storage

    def __exit__(self, *args):
        return self.tmp_dir.__exit__(*args)

class SqlStorageContext:
    def __enter__(self):
        database = SQLDatabase('sqlite:///:memory:')
        storage = SqlBlobStorage('test', database.get_session())
        return storage

    def __exit__(self, *args):
        return True

class DynamoDBStorageContext:
    def __enter__(self):
        env_path = Path('.env.integration')
        load_dotenv(dotenv_path=env_path)
        dynamodb = DynamoDB()
        storage = DynamoDBBlobStorage('test', dynamodb.get_table())
        return storage

    def __exit__(self, *args):
        return True

storages = [
    FileSystemStorageContext,
    SqlStorageContext,
    # DynamoDBStorageContext
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
@pytest.mark.parametrize(storages)
@timeout(3)
async def test_persistence_basic(storage_ctx):
    with storage_ctx as storage:

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
