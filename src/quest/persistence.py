# Enable event histories to be persistent
import json
import os

import boto3
from boto3.dynamodb.conditions import Key
from hashlib import md5
from pathlib import Path
from typing import Protocol, Union, TypedDict

from botocore.exceptions import ClientError
from sqlalchemy.dialects.mssql.information_schema import key_constraints

from .history import History
from .quest_types import EventRecord

Blob = Union[dict, list, str, int, bool, float]


class BlobStorage(Protocol):
    def write_blob(self, key: str, blob: Blob): ...

    def read_blob(self, key: str) -> Blob: ...

    def has_blob(self, key: str) -> bool: ...

    def delete_blob(self, key: str): ...


class PersistentHistory(History):
    def __init__(self, namespace: str, storage: BlobStorage):
        self._namespace = namespace
        self._storage = storage
        # TODO - use linked list instead of array list
        # master stores head/tail keys
        # each blob is item, prev, next
        # only need to modify blobs for altered nodes
        # on add/delete, and rarely change master head/tail blob
        self._items = []
        self._keys: list[str] = []

        if storage.has_blob(namespace):
            self._keys = storage.read_blob(namespace)
            for key in self._keys:
                self._items.append(storage.read_blob(key))

    def _get_key(self, item: EventRecord) -> str:
        return self._namespace + '.' + md5((item['timestamp'] + item['step_id'] + item['type']).encode()).hexdigest()

    def append(self, item: EventRecord):
        self._items.append(item)
        self._keys.append(key := self._get_key(item))
        self._storage.write_blob(key, item)
        self._storage.write_blob(self._namespace, self._keys)

    def remove(self, item: EventRecord):
        self._items.remove(item)
        self._keys.remove(key := self._get_key(item))
        self._storage.delete_blob(key)
        self._storage.write_blob(self._namespace, self._keys)

    def __iter__(self):
        return iter(self._items)

    def __reversed__(self):
        return reversed(self._items)


class LocalFileSystemBlobStorage(BlobStorage):
    def __init__(self, root_folder: Path):
        root_folder.mkdir(parents=True, exist_ok=True)
        self._root = root_folder

    def _get_file(self, key: str) -> Path:
        return self._root / (key + '.json')

    def write_blob(self, key: str, blob: Blob):
        self._get_file(key).write_text(json.dumps(blob))

    def read_blob(self, key: str) -> Blob:
        return json.loads(self._get_file(key).read_text())

    def has_blob(self, key: str) -> bool:
        return self._get_file(key).exists()

    def delete_blob(self, key: str):
        self._get_file(key).unlink()

class DynamoDBTableCreationException(Exception):
    pass

class DynamoDB:
    def __init__(self):
        self.session = boto3.session.Session(
            os.environ['AWS_ACCESS_KEY_ID'],
            os.environ['AWS_SECRET_ACCESS_KEY'],
            os.environ['AWS_REGION']
        )

        self._table_name = 'quest_records'
        self._dynamodb = self.session.resource('dynamodb')
        self._table = self._prepare_table()

    def get_table(self):
        return self._table

    def _prepare_table(self):
        try:
            # Check if table already exists
            table = self._dynamodb.Table(self._table_name)
            table.load()
            return table
        except ClientError as e:
            # If it doesn't, create it
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                table = self._dynamodb.create_table(
                    TableName=self._table_name,
                    KeySchema=[
                        {
                            'AttributeName': 'name',
                            'KeyType': 'HASH'
                        },
                        {
                            'AttributeName': 'key',
                            'KeyType': 'RANGE'
                        }
                    ],
                    AttributeDefinitions=[
                        {
                            'AttributeName': 'name',
                            'KeyType': 'S'
                        },
                        {
                            'AttributeName': 'key',
                            'KeyType': 'S'
                        }
                    ],
                    ProvisionedThroughput={
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    },
                )
                table.meta.client.get_waiter('table_exists').wait(TableName=self._table_name)
                return table
            else:
                raise DynamoDBTableCreationException(f'Error creating DynamoDB table: {e}')

class DynamoDBBlobStorage(BlobStorage):
    def __init__(self, name: str, table):
        self._name = name
        self._table = table

    def write_blob(self, key: str, blob: Blob):
        record = {
            'name': self._name,
            'key': key,
            'blob': blob
        }
        self._table.put_item(Item=record)

    def read_blob(self, key: str) -> Blob:
        primary_key = {
            'name': self._name,
            'key': key
        }
        item = self._table.get_item(Key=primary_key)
        return item.get('blob')

    def has_blob(self, key: str):
        primary_key = {
            'name': self._name,
            'key': key
        }
        item = self._table.get_item(Key=primary_key)
        if item.get('blob'):
            return True
        return False

    def delete_blob(self, key: str):
        primary_key = {
            'name': self._name,
            'key': key
        }
        self._table.delete_item(Key=primary_key)

class InMemoryBlobStorage(BlobStorage):
    def __init__(self):
        self._data = {}

    def write_blob(self, key: str, blob: Blob):
        self._data[key] = blob

    def read_blob(self, key: str) -> Blob:
        return self._data[key]

    def has_blob(self, key: str) -> bool:
        return key in self._data

    def delete_blob(self, key: str):
        del self._data[key]
