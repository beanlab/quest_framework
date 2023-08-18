import json
from pathlib import Path
from typing import TypeVar

from .historian import History

WT = TypeVar('WT')


class LocalJsonHistory(History):
    def __init__(self, folder: Path):
        self._folder: Path = folder
        self._history = []

        folder.mkdir(parents=True, exist_ok=True)
        self._load()

    def _load(self):
        self._history = [
            json.loads(file.read_text())
            for file in sorted(self._folder.iterdir())
        ]

    def _write_item(self, item):
        file = self._folder / f'l{len(self._history):04}.json'
        file.write_text(json.dumps(item))

    def append(self, item):
        self._history.append(item)
        self._write_item(item)

    def __iter__(self):
        return self._history.__iter__()

    def __getitem__(self, item):
        return self._history.__getitem__(item)

    def __len__(self):
        return self._history.__len__()
