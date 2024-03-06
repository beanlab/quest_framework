import os
import json
from pathlib import Path
from typing import Callable

from .context import these
from .wrappers import step, task
from .external import state, queue, identity_queue, event
from .historian import Historian, History
from .persistence import LocalFileSystemBlobStorage, PersistentHistory
from .versioning import version, get_version
from .manager import WorkflowManager, WorkflowFactory


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

def print_directory_json(folder: Path):
    for file in sorted(folder.iterdir()):
        if os.path.isdir(file):
            print_directory_json(file)
        else:
            print(f"Current Directory: {folder.name}")
            content = json.loads(file.read_text())
            print(json.dumps(content, indent=2))
            print("\n")

