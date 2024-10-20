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

def create_sql_manager( # This is what I want
        db_url: str,
        namespace: str,
        factory: WorkflowFactory
) -> WorkflowManager:

    # TODO I want this to create a database/connection to the url the user provides
    database = Database(db_url)

    storage = database.create_workflow_table(namespace)

    def create_history(wid: str) -> History:
        # TODO I want this to create the table and return a reference to a BlobStorage object that can add, remove, etc.
        return PersistentHistory(wid, database.create_workflow_table(wid))

    return WorkflowManager(namespace, storage, create_history, factory)
