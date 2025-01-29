import asyncio
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from dotenv import load_dotenv

import pytest

from quest import step
from quest.historian import Historian
from quest.serializer import NoopSerializer
from quest.persistence import PersistentHistory, LocalFileSystemBlobStorage
from .utils import timeout

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
async def test_persistence_local():
    class FileSystemStorageContext:
        def __enter__(self):
            self.tmp_dir = Path(TemporaryDirectory().__enter__())
            storage = LocalFileSystemBlobStorage(self.tmp_dir)
            return storage

        def __exit__(self, *args):
            return self.tmp_dir.__exit__(*args)

        def cleanup_check(self):
            return ~self.tmp_dir.exists()

    await persistence_basic(FileSystemStorageContext())
    await resume_step_persistence(FileSystemStorageContext())
    await workflow_cleanup_suspend(FileSystemStorageContext())
    await workflow_cleanup_basic(FileSystemStorageContext())

@pytest.mark.asyncio
@timeout(3)
async def test_persistence_sql():
    from quest.extras.sql import SQLDatabase, SqlBlobStorage, RecordModel

    class SqlStorageContext:
        _wid = 'test'
        def __enter__(self):
            self._database = SQLDatabase('sqlite:///:memory:')
            storage = SqlBlobStorage(self._wid, self._database.get_session())
            return storage

        def __exit__(self, *args):
            return True

        def cleanup_check(self):
            return self._database.get_session().query(RecordModel).filter(RecordModel.wid == self._wid).one_or_none() is None

    await persistence_basic(SqlStorageContext())
    await resume_step_persistence(SqlStorageContext())
    await workflow_cleanup_suspend(SqlStorageContext())
    await workflow_cleanup_basic(SqlStorageContext())

@pytest.mark.asyncio
@timeout(6)
@pytest.mark.integration
async def test_persistence_aws():
    from quest.extras.aws import S3BlobStorage, S3Bucket, DynamoDB, DynamoDBBlobStorage
    class DynamoDBStorageContext:
        _wid = 'test'

        def __enter__(self):
            self._dynamodb = DynamoDB()
            storage = DynamoDBBlobStorage(self._wid, self._dynamodb.get_table())
            return storage

        def __exit__(self, *args):
            return True

        def cleanup_check(self):
            response = self._dynamodb._table.query(
                KeyConditionExpression=f"wid = :wid_value",
                ExpressionAttributeValues={
                    ":wid_value": self._wid
                },
                Limit=1  # Fetch only one item
            )
            return response['Count'] == 0  # Returns True if a record exists, False otherwise


    class S3StorageContext:
        def __enter__(self):
            s3 = S3Bucket()
            self._storage = S3BlobStorage('test', s3.get_s3_client(), s3.get_bucket_name())
            return self._storage

        def __exit__(self, *args):
            return True

        def cleanup_check(self):
            response = self._storage._s3_client.list_objects_v2(Bucket=self._storage._bucket_name, Prefix=self._storage._wid)
            return 'Contents' not in response

    await persistence_basic(S3StorageContext())
    await resume_step_persistence(S3StorageContext())
    await workflow_cleanup_suspend(S3StorageContext())

    await persistence_basic(DynamoDBStorageContext())
    await resume_step_persistence(DynamoDBStorageContext())
    await workflow_cleanup_suspend(DynamoDBStorageContext())



async def persistence_basic(storage_ctx):
    with storage_ctx as storage:
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

    with storage_ctx as storage:
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

async def resume_step_persistence(storage_ctx):
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


async def workflow_cleanup_suspend(storage_ctx):
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

        assert storage_ctx.cleanup_check()


async def workflow_cleanup_basic(storage_ctx):
    with storage_ctx as storage:
        history = PersistentHistory('test', storage)
        historian = Historian(
            'test',
            simple_workflow,
            history,
            serializer=NoopSerializer()
        )

        pause.set()
        await historian.run()

        assert storage_ctx.cleanup_check()

