from pathlib import Path
from typing import Callable

from .context import these
from .wrappers import step, task
from .external import state, queue, identity_queue, event
from .historian import Historian
from .history import History
from .sql import SQLDatabase, SqlBlobStorage
from .persistence import LocalFileSystemBlobStorage, PersistentHistory, DynamoDB, DynamoDBBlobStorage, BlobStorage, Blob
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

try:
    from .sql import create_sql_manager
except ImportError:
    raise ImportError("quest[sql] is not installed. To use SQL databases, install quest with the sql extra: pip install quest[sql]")

def create_dynamodb_manager(
        namespace: str,
        factory: WorkflowFactory,
) -> WorkflowManager:
    dynamodb = DynamoDB()

    storage = DynamoDBBlobStorage(namespace, dynamodb.get_table())

    def create_history(wid: str) -> History:
        return PersistentHistory(wid, DynamoDBBlobStorage(wid, dynamodb.get_table()))

    return WorkflowManager(namespace, storage, create_history, factory)
