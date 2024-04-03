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

class ListPersistentHistory(History):
    def __init__(self, namespace: str, storage: BlobStorage):
        self._namespace = namespace
        self._storage = storage
        self._records: dict[str, EventRecord] = {}

        if storage.has_blob(namespace):
            keys = storage.read_blob(namespace)
            for key in keys:
                self._records[key] = storage.read_blob(key)

    def _get_key(self, item: EventRecord) -> str:
        return self._namespace + '.' + md5((item['timestamp'] + item['step_id'] + item['type']).encode()).hexdigest()
    
    def append(self, item: EventRecord):
        self._records[key := self._get_key(item)] = item
        self._storage.write_blob(key, item)
        self._storage.write_blob(self._namespace, list(self._records.keys()))

    def remove(self, item: EventRecord):
        del self._records[key := self._get_key(item)]
        self._storage.delete_blob(key)
        self._storage.write_blob(self._namespace, list(self._records.keys()))

    def __iter__(self):
        return iter(self._records.values())
    
    def __reversed__(self):
        return reversed(self._records.values())

class LinkedPersistentHistory(History):
    class HistoryNode():
        def __init__(self):
            self.prev: self.HistoryNode = None
            self.next: self.HistoryNode = None
            self.item = None

    def __init__(self, namespace: str, storage: BlobStorage):
        self._namespace = namespace
        self._storage = storage
        self._nodes: dict[str, self.HistoryNode] = {}

        self._head: self.HistoryNode = None
        self._tail: self.HistoryNode = None

        if storage.has_blob(namespace):
            keys = storage.read_blob(namespace)
            for key in keys:
                self.append(storage.read_blob(key))

    def _get_key(self, item: EventRecord) -> str:
        return self._namespace + '.' + md5((item['timestamp'] + item['step_id'] + item['type']).encode()).hexdigest()

    def _insert_node(self, nodeKey: str, item: Blob):
        """Inserts blob into the linked list of HistoryNodes"""
        new_node: self.HistoryNode = self.HistoryNode()
        new_node.item = item
        new_node.prev = self._tail

        if(self._tail is not None):
            self._tail.next = new_node
        else:
            self._head = new_node

        self._tail = new_node

        self._nodes[nodeKey] = new_node

    def _remove_node(self, nodeKey: str):
        # TODO: should I be key checking here first? Probably
        toDelete: self.HistoryNode = self._nodes[nodeKey]

        if toDelete.prev is not None:
            toDelete.prev.next = toDelete.next

        if toDelete.next is not None:
            toDelete.next.prev = toDelete.prev

        if toDelete is self._head:
            self._head = toDelete.next

        if toDelete is self._tail:
            self._tail = toDelete.prev

        del self._nodes[nodeKey]

    def append(self, item: EventRecord):
        key = self._get_key(item)
        self._insert_node(key, item)
        self._storage.write_blob(key, item)
        self._storage.write_blob(self._namespace, list(self._nodes.keys()))

    def remove(self, item: EventRecord):
        key = self._get_key(item)
        self._remove_node(key)
        self._storage.delete_blob(key)
        self._storage.write_blob(self._namespace, list(self._nodes.keys()))

    def __iter__(self):
        node: self.HistoryNode = self._head
        while node is not None:
            yield node.item
            node = node.next
    
    def __reversed__(self):
        node: self.HistoryNode = self._tail
        while node is not None:
            yield node.item
            node = node.prev

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
