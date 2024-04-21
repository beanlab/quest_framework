# Enable event histories to be persistent
import json
from hashlib import md5
from pathlib import Path
from typing import Protocol, Union
import copy
import platform
import signal

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
        self._items = []
        self._keys: list[str] = []
        self._implements_signals = True
        if "Windows" in platform.platform():
            self._implements_signals = False
        self._pertinent_signals = [signal.SIGABRT, signal.SIGINT, signal.SIGTERM]
        self._old_signal_mask = None

        if storage.has_blob(namespace):
            self._keys = storage.read_blob(namespace)
            for key in self._keys:
                self._items.append(storage.read_blob(key))

    def _get_key(self, item: EventRecord) -> str:
        return self._namespace + '.' + md5((item['timestamp'] + item['step_id'] + item['type']).encode()).hexdigest()
    
    def _refuse_signals(self):
        self._old_signal_mask = signal.pthread_sigmask(signal.SIG_BLOCK, self._pertinent_signals)

    def _allow_signals(self):
        signal.pthread_sigmask(signal.SIG_SETMASK, self._old_signal_mask)

    def append(self, item: EventRecord):
        self._refuse_signals()
        self._items.append(item)
        self._keys.append(key := self._get_key(item))
        # writing the keys first is important. It doesn't cause an error to have an extra file, but no key to it,
            # but it is a problem if we have a key entry to a file that doesn't exist
        self._storage.write_blob(self._namespace, self._keys)
        self._storage.write_blob(key, item)
        self._allow_signals()

    def remove(self, item: EventRecord):
        self._refuse_signals()
        self._items.remove(item)
        self._keys.remove(key := self._get_key(item))
        self._storage.write_blob(self._namespace, self._keys)
        self._storage.delete_blob(key)
        self._allow_signals()

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
        self._get_file(key).write_text(json.dumps(blob, indent=2))

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
        self._data[key] = copy.deepcopy(blob)

    def read_blob(self, key: str) -> Blob:
        return self._data[key]

    def has_blob(self, key: str) -> bool:
        return key in self._data

    def delete_blob(self, key: str):
        del self._data[key]
