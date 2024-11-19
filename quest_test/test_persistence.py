import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from dotenv import load_dotenv

import pytest

from quest import step
from quest.historian import Historian
from quest.serializer import NoopSerializer
from quest.persistence import PersistentHistory, LocalFileSystemBlobStorage
from .utils import timeout
from quest.extras.sql import SQLDatabase, SqlBlobStorage
from quest.extras.aws import S3BlobStorage, S3Bucket, DynamoDB, DynamoDBBlobStorage


class TestFileSystemStorageContext:
    def __enter__(self):
        self.tmp_dir = Path('test_dir')
        storage = LocalFileSystemBlobStorage(self.tmp_dir)
        return storage

    def __exit__(self, *args):
        return True


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
        env_path = Path('.integration.env')
        load_dotenv(dotenv_path=env_path)
        dynamodb = DynamoDB()
        storage = DynamoDBBlobStorage('test', dynamodb.get_table())
        return storage

    def __exit__(self, *args):
        return True


class S3StorageContext:
    def __enter__(self):
        env_path = Path('.integration.env')
        load_dotenv(dotenv_path=env_path)
        s3 = S3Bucket()
        storage = S3BlobStorage('test', s3.get_s3_client(), s3.get_bucket_name())
        return storage

    def __exit__(self, *args):
        return True


storages = [
    pytest.param(FileSystemStorageContext(), marks=pytest.mark.unit, id="file"),
    # pytest.param(SqlStorageContext(), marks=pytest.mark.unit, id="sql"),
    # pytest.param(DynamoDBStorageContext(), marks=pytest.mark.integration, id="dynamodb"),
    # pytest.param(S3StorageContext(), marks=pytest.mark.integration, id="s3"),
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


@pytest.mark.parametrize('storage_ctx', storages)
async def test_resume_step_persistence(storage_ctx):
    with storage_ctx as storage:
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

    with storage_ctx as storage:
        history = PersistentHistory('test', storage)
        historian = Historian(
            'test',
            resume_this_workflow,
            history,
            serializer=NoopSerializer()
        )

        await historian.run()
