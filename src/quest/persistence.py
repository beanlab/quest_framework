# Enable event histories and unique ID dictionaries to be persistent
import json
from pathlib import Path

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


    def append(self, item): ...

    def __iter__(self): ...

    def __getitem__(self, item): ...

    def __len__(self): ...