import asyncio
from collections.abc import Collection


external_task_name = 'external'
known_task_names: Collection[str]


def get_task_id() -> str:
    """Return the current task's known name, or the external-task fallback."""
    try:
        task = asyncio.current_task()
    except RuntimeError:  # No running event loop in this thread.
        task = None

    if task is not None:
        name = task.get_name()
        if name in known_task_names:
            return name

    return external_task_name
