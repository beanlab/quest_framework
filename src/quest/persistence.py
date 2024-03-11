# Enable event histories to be persistent
import json
from hashlib import md5
from pathlib import Path
from typing import Protocol, Union

from .history import History
from .types import EventRecord

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

#------------------------------------------
class HistoryNode():
    def __init__(self):
        self.prev: HistoryNode = None
        self.next: HistoryNode = None
        self.item = None

class PersistentHistory2(History):
    def __init__(self, namespace:str, storage: BlobStorage):
        self._namespace = namespace
        self._storage = storage
        self._keys: list[str] = []
        self._nodes: dict[str, HistoryNode]

        self._head: HistoryNode = None
        self._tail: HistoryNode = None

        if storage.has_blob(namespace):
            self._keys = storage.read_blob(namespace) # these are keys for the json files
            for key in self._keys:
                self._insert_item(storage.read_blob(key))

    def _get_key(self, item: EventRecord):
        pass

    def _insert_item(self, item: Blob):
        """Inserts blob into the linked list of HistoryNodes"""
        pass

    def append(self, item: EventRecord):
        pass

    def __iter__(self):
        # generate an array of the items starting from self._head using the next attribute
        pass

    def __reversed__(self):
        # generate an array of the items starting from self._tail using the prev attribute
        pass

#-------------------------------------------

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
