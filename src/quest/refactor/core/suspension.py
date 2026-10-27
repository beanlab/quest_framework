from asyncio import CancelledError

SUSPENDED = '__QUEST_SUSPENDED__'


def is_suspension(cancel: CancelledError) -> bool:
    return cancel.args and cancel.args[0] == SUSPENDED


def suspend_task(task):
    task.cancel(SUSPENDED)