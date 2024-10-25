from pathlib import Path
from typing import Callable

from .context import these
from .wrappers import step, task
from .external import state, queue, identity_queue, event
from .historian import Historian
from .history import History
from .persistence import LocalFileSystemBlobStorage, SqlBlobStorage, PersistentHistory, SQLDatabase
from .versioning import version, get_version
from .manager import WorkflowManager, WorkflowFactory
from .utils import ainput
from .manager_wrappers import alias


def create_filesystem_historian(save_folder: Path, historian_id: str, function: Callable) -> Historian:
    storage = LocalFileSystemBlobStorage(save_folder)
    history = PersistentHistory(historian_id, storage)
    return Historian(
        historian_id,
        function,
        history
    )

def create_filesystem_manager(
        save_folder: Path,
        namespace: str,
        factory: WorkflowFactory
) -> WorkflowManager:
    storage = LocalFileSystemBlobStorage(save_folder / namespace)

    def create_history(wid: str) -> History:
        return PersistentHistory(wid, LocalFileSystemBlobStorage(save_folder / namespace / wid))

    return WorkflowManager(namespace, storage, create_history, factory)

def create_sql_manager(
        db_url: str,
        namespace: str,
        factory: WorkflowFactory
) -> WorkflowManager:

    database = SQLDatabase(db_url)

    storage = SqlBlobStorage(namespace, database.get_engine())

    def create_history(wid: str) -> History:
        return PersistentHistory(wid, SqlBlobStorage(wid, database.get_engine()))

    return WorkflowManager(namespace, storage, create_history, factory)
