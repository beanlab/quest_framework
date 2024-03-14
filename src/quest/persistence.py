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

class HistoryNode():
    def __init__(self):
        self.prev: HistoryNode = None
        self.next: HistoryNode = None
        self.item = None

class PersistentHistory(History):
    def __init__(self, namespace: str, storage: BlobStorage):
        self._namespace = namespace
        self._storage = storage
        self._nodes: dict[str, HistoryNode] = {}

        self._head: HistoryNode = None
        self._tail: HistoryNode = None

        if storage.has_blob(namespace):
            keys = storage.read_blob(namespace)
            for key in keys:
                self.append(storage.read_blob(key))

    def _get_key(self, item: EventRecord) -> str:
        return self._namespace + '.' + md5((item['timestamp'] + item['step_id'] + item['type']).encode()).hexdigest()

    def _insert_node(self, nodeKey: str, item: Blob):
        """Inserts blob into the linked list of HistoryNodes"""
        newNode: HistoryNode = HistoryNode()
        newNode.item = item
        newNode.prev = self._tail

        if(self._tail != None):
            self._tail.next = newNode
        else:
            self._head = newNode

        self._tail = newNode

        self._nodes[nodeKey] = newNode

    def _remove_node(self, nodeKey: str):
        # TODO: should I be key checking here first? Probably
        toDelete: HistoryNode = self._nodes[nodeKey]

        if toDelete.prev is not None:
            toDelete.prev.next = toDelete.next

        if toDelete.next is not None:
            toDelete.next.prev = toDelete.prev

        if toDelete == self._head:
            self._head = toDelete.next

        if toDelete == self._tail:
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

    class HistoryIterator():
        def __init__(self, currentNode: HistoryNode, direction: str):
            self.current: HistoryNode = currentNode
            self.direction = direction

        def __next__(self):
            if self.current == None:
                raise StopIteration
            else:
                result = self.current
                if self.direction == 'prev':
                    self.current = self.current.prev
                else:
                    self.current = self.current.next
                return result.item

    def __iter__(self):
        return self.HistoryIterator(self._head, 'next')
    
    def __reversed__(self):
        return self.HistoryIterator(self._tail, 'prev')

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
