from pathlib import Path
from typing import Callable

from .context import these
from .external import state, queue, identity_queue, event
from .historian import Historian, suspendable
from .history import History
from .manager import WorkflowManager, WorkflowFactory
from .manager_wrappers import alias
from .persistence import LocalFileSystemBlobStorage, PersistentHistory, BlobStorage, Blob
from .serializer import StepSerializer, MasterSerializer, NoopSerializer
from .utils import ainput
from .versioning import version, get_version
from .wrappers import step, task, wrap_steps


def create_filesystem_historian(save_folder: Path, historian_id: str, function: Callable,
                                serializer: StepSerializer = None) -> Historian:
    storage = LocalFileSystemBlobStorage(save_folder)
    history = PersistentHistory(historian_id, storage)
    serializer = serializer or NoopSerializer()
    return Historian(
        historian_id,
        function,
        history,
        serializer=serializer
    )


def create_filesystem_manager(
        save_folder: Path,
        namespace: str,
        factory: WorkflowFactory,
        serializer: StepSerializer = NoopSerializer()
) -> WorkflowManager:
    def create_history(wid: str) -> History:
        return PersistentHistory(wid, LocalFileSystemBlobStorage(save_folder / namespace / wid))

    workflow_manager_storage = LocalFileSystemBlobStorage(save_folder / namespace)

    return WorkflowManager(namespace, workflow_manager_storage, create_history, factory, serializer=serializer)
