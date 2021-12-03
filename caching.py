import logging
import os
import pickle
import re
from pathlib import Path
from typing import TypeVar, Callable, Protocol

T = TypeVar('T')
Q = TypeVar('Q')
P = TypeVar('P')
Action = Callable[[], T]


class RetryCheckpoint(Exception):
    pass


class CheckpointFailed(Exception):
    pass


class CacheTool(Protocol):
    def exists(self, name: str) -> bool:
        pass

    def save(self, name: str, payload: P) -> P:
        pass

    def load(self, name: str):
        pass

    def invalidate(self, prefix: str):
        pass


class PickleCacheTool(CacheTool):
    cache_path: Path

    def __init__(self, cache_path: Path):
        self.cache_path = cache_path
        self.PRESERVE_INVALID_CACHE = False

    def _safe_name(self, name: str) -> str:
        return re.sub(r'\W+', "_", name)

    def _get_cache_path(self, name: str) -> Path:
        return self.cache_path / (self._safe_name(name) + '.pickle')

    def exists(self, name: str) -> bool:
        return self._get_cache_path(name).exists()

    def save(self, name: str, payload: P) -> P:
        self._get_cache_path(name).parent.mkdir(exist_ok=True, parents=True)
        with open(self._get_cache_path(name), 'wb') as f:
            pickle.dump(payload, f)
        return payload

    def load(self, name: str):
        with open(self._get_cache_path(name), 'rb') as f:
            return pickle.load(f)

    def invalidate(self, prefix: str):
        logging.debug(f"Deleting cache for {prefix}")
        parent = self.cache_path if self.cache_path.is_dir() else self.cache_path.parent
        for dir_path, _, files in os.walk(parent):
            for file in files:
                full_path = os.path.join(dir_path, file)
                if full_path.startswith(str(self.cache_path / prefix)):
                    if self.PRESERVE_INVALID_CACHE:
                        new_path = full_path.replace(".pickle", ".invalid.pickle")
                        logging.debug(f"Moving {full_path} to {new_path}")
                        os.rename(full_path, new_path)
                    else:
                        logging.debug(f"Deleting {full_path}")
                        os.remove(full_path)
