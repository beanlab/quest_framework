from pathlib import Path
from typing import Callable

from .context import these
from .wrappers import step, task
from .external import state, queue, identity_queue, event
from .historian import Historian
from .history import History
from .persistence import LocalFileSystemBlobStorage, SqlBlobStorage, PersistentHistory
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
    # TODO: We will just need to create a SqlBlobStorage
    storage = LocalFileSystemBlobStorage(save_folder / namespace)

    def create_history(wid: str) -> History:
        return PersistentHistory(wid, LocalFileSystemBlobStorage(save_folder / namespace / wid))

    return WorkflowManager(namespace, storage, create_history, factory)

def create_sql_manager(
        db_path: Path,
        namespace: str,
        factory: WorkflowFactory
) -> WorkflowManager:
    # TODO
    # Create storage db that will hold tables
    # create_history should make a table within the storage db

    storage = SqlBlobStorage(db_path / namespace) # TODO: Test this as local filesystem
    # This looks like it is a place to store each namespace
    # This should probably be the database that accepts new tables


    def create_history(wid: str) -> History:
        # TODO: And this as sql? Create a db per workflow rn?
        return PersistentHistory(wid, SqlBlobStorage(db_path / namespace / wid))
        # This looks like it is what is used to create a single history for a workflow
        # This should probably just produce a table that can accept blobs

    return WorkflowManager(namespace, storage, create_history, factory)
