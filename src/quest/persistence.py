# Enable event histories to be persistent
import json
from hashlib import md5
from pathlib import Path
from typing import Protocol, Union
import asyncio

from .history import History
from .quest_types import EventRecord
from .serializer import MasterSerializer

Blob = Union[dict, list, str, int, bool, float]


class BlobStorage(Protocol):
    def write_blob(self, key: str, blob: Blob): ...

    def read_blob(self, key: str) -> Blob: ...

    def has_blob(self, key: str) -> bool: ...

    def delete_blob(self, key: str): ...


class PersistentHistory(History):
    def __init__(self, namespace: str, storage: BlobStorage, master_serializer):
        self._namespace = namespace
        self._storage = storage
        self._master_serializer = master_serializer
        # TODO - use linked list instead of array list
        # master stores head/tail keys
        # each blob is item, prev, next
        # only need to modify blobs for altered nodes
        # on add/delete, and rarely change master head/tail blob
        self._items = []  # for deserialized event records
        self._keys: list[str] = []
        self._serialized_items = []  # for serialized event records

    async def load_history(self):
        if self._storage.has_blob(self._namespace):
            self._keys = self._storage.read_blob(self._namespace)
            self._serialized_items = [self._storage.read_blob(key) for key in self._keys]
            self._items = []
            for serialized_item in self._serialized_items:
                item = await self._master_serializer.deserialize(serialized_item)
                self._items.append(item)

    def _get_key(self, item: EventRecord) -> str:
        return self._namespace + '.' + md5((item['timestamp'] + item['step_id'] + item['type']).encode()).hexdigest()

    async def append(self, item: EventRecord, master_serializer):
        self._items.append(item)
        self._keys.append(key := self._get_key(item))
        serialized_item = await master_serializer.serialize(item)
        self._serialized_items.append(serialized_item)
        self._storage.write_blob(key, serialized_item)
        self._storage.write_blob(self._namespace, self._keys)

    def remove(self, item: EventRecord):
        index = self._items.index(item)
        self._items.remove(item)
        self._keys.remove(key := self._get_key(item))
        key = self._keys.pop(index)
        self._serialized_items.pop(index)  # _keys, _items, _serialized_items share same index for same EventRecord
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
