from pathlib import Path
from typing import Callable

from .context import these
from .external import state, queue, identity_queue, event
from .history import History
from .book import Book
from .historian import Historian, WorkflowFactory
from .manager_wrappers import alias
from .persistence import LocalFileSystemBlobStorage, PersistentList, BlobStorage, Blob
from .serializer import StepSerializer, MasterSerializer, NoopSerializer
from .utils import ainput
from .versioning import version, get_version
from .wrappers import step, task, wrap_steps


def create_filesystem_history(save_folder: Path, history_id: str, function: Callable,
                              serializer: StepSerializer = None) -> History:
    storage = LocalFileSystemBlobStorage(save_folder)
    history = PersistentList(history_id, storage)
    serializer = serializer or NoopSerializer()
    return History(
        history_id,
        function,
        history,
        serializer=serializer
    )


def create_filesystem_historian(
        save_folder: Path,
        namespace: str,
        factory: WorkflowFactory,
        serializer: StepSerializer = NoopSerializer()
) -> Historian:
    def create_book(wid: str) -> Book:
        return PersistentList(wid, LocalFileSystemBlobStorage(save_folder / namespace / wid))

    workflow_manager_storage = LocalFileSystemBlobStorage(save_folder / namespace)

    return Historian(namespace, workflow_manager_storage, create_book, factory, serializer=serializer)
