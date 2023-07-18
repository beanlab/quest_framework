# Enable event histories and unique ID dictionaries to be persistent
import json
from pathlib import Path
from typing import Iterable

from .events import UniqueEvent
from .historian import History, UniqueEvents


class JsonHistory(History):
    def __init__(self, filename: Path):
        self._filename: Path = filename
        self._history = []

    def __enter__(self):
        if self._filename.exists():
            self._history = json.loads(self._filename.read_text())
        else:
            self._history = []
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with open(self._filename, 'r') as file:
            json.dump(self._history, file)

    def append(self, item):
        return self._history.append(item)

    def __iter__(self):
        return self._history.__iter__()

    def __getitem__(self, item):
        return self._history.__getitem__(item)

    def __len__(self):
        return self._history.__len__()


class JsonDictionary(UniqueEvents):
    def __init__(self, filename: Path):
        self._filename: Path = filename
        self._events: dict[str, UniqueEvent] = {}

    def __enter__(self):
        if self._filename.exists():
            self._events = json.loads(self._filename.read_text())
        else:
            self._events = {}
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with open(self._filename, 'r') as file:
            json.dump(self._events, file)

    def __setitem__(self, key: str, value: UniqueEvent):
        return self._events.__setitem__(key, value)

    def __getitem__(self, item: str) -> UniqueEvent:
        return self._events.__getitem__(item)

    def __contains__(self, item: UniqueEvent):
        return self.__contains__(item)

    def values(self) -> Iterable[UniqueEvent]:
        return self._events.values()
