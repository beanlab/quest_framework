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


def create_filesystem_historian(save_folder: Path, historian_id: str, function: Callable) -> Historian:
    storage = LocalFileSystemBlobStorage(save_folder)
    history = PersistentHistory(historian_id, storage)
    return Historian(
        historian_id,
        function,
        history
    )

# TODO: you can probably copy this function and pass in a sql db connection or something
def create_filesystem_manager(
        save_folder: Path,
        namespace: str,
        factory: WorkflowFactory
) -> WorkflowManager:
    storage = LocalFileSystemBlobStorage(save_folder / namespace)

    def create_history(wid: str) -> History:
        return PersistentHistory(wid, LocalFileSystemBlobStorage(save_folder / namespace / wid))

    return WorkflowManager(namespace, storage, create_history, factory)

def create_sql_manager( # This is what I want
        db_url: str, # TODO authentication?
        namespace: str,
        factory: WorkflowFactory
) -> WorkflowManager:

    # TODO This is basically just a wrapper for the engine/table creation, do I need this?
    database = SQLDatabase(db_url)

    # TODO Each blobstorage carries a reference to the engine and uses it to create sessions when needed
    storage = SqlBlobStorage(namespace, database.get_engine())

    def create_history(wid: str) -> History:
        return PersistentHistory(wid, SqlBlobStorage(wid, database.get_engine()))

    return WorkflowManager(namespace, storage, create_history, factory)
