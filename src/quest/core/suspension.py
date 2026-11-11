from asyncio import CancelledError

SUSPENDED = '__QUEST_SUSPENDED__'


def is_suspension(cancel: BaseException) -> bool:
    return isinstance(cancel, CancelledError) and cancel.args and cancel.args[0] == SUSPENDED


def is_interrupt_suspension(exc: BaseException) -> bool:
    return is_suspension(exc) and len(exc.args) > 1 and exc.args[1]


def suspend_task(task):
    task.cancel(SUSPENDED)


def raise_suspension(is_interrupt=False):
    raise CancelledError(SUSPENDED, is_interrupt)
