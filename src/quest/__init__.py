from pathlib import Path
from typing import Callable

from .context import these
from .wrappers import step, task
from .historian import Historian
from .persistence import LocalFileSystemBlobStorage, PersistentHistory


def create_filesystem_historian(save_folder: Path, historian_id: str, function: Callable) -> Historian:
    storage = LocalFileSystemBlobStorage(save_folder)
    history = PersistentHistory(historian_id, storage)
    return Historian(
        historian_id,
        function,
        history
    )
