import asyncio
from asyncio import Task
from functools import wraps

from .ledger import Ledger
from .logging import quest_logger
from .replay_stream import ReplayStream
from .suspension import suspend_task
from .unique_ids import UniqueIdHandler
from .utils import get_function_name


class TaskManager:
    def __init__(self, ledger: Ledger, unique_id_handler: UniqueIdHandler,
                 replay_stream: ReplayStream):
        self._unique_id_handler = unique_id_handler
        self._ledger = ledger
        self._replay_stream = replay_stream

        # We keep track of all open tasks so we can properly suspend them
        self._open_tasks: list[Task] = []

    async def start_task(self, parent_task_id, func, args, kwargs, task_name: str | None, task_factory=asyncio.create_task):
        task_id = task_name or self._unique_id_handler.get_unique_id(parent_task_id, get_function_name(func))
        quest_logger.debug(f'Requested {task_id} start')

        @wraps(func)
        async def _func(*a, **kw):
            quest_logger.debug(f'Starting task {task_id}')

            if (next_record := await self._replay_stream.get_next_record(task_id)) is None:
                await self._ledger.add_record(
                    task_id=task_id,
                    entry_type='start_task',
                    entry_id=task_id + '.start',
                    data=None
                )
            else:
                with next_record as record:
                    assert record['entry_type'] == 'start_task'
                    assert record['task_id'] == task_id

            result = await func(*a, **kw)

            if (next_record := await self._replay_stream.get_next_record(task_id)) is None:
                await self._ledger.add_record(
                    task_id=task_id,
                    entry_type='complete_task',
                    entry_id=task_id + '.complete',
                    data=None
                )

            else:
                with next_record as record:
                    assert record['entry_type'] == 'complete_task'
                    assert record['task_id'] == task_id

            quest_logger.debug(f'Completing task {task_id}')

            return result

        task = task_factory(
            _func(*args, **kwargs),
            name=task_id,
        )

        self._unique_id_handler.start_prefix(task_id)
        self._open_tasks.append(task)
        task.add_done_callback(lambda t: self._open_tasks.remove(t) if t in self._open_tasks else None)

        return task

    def signal_suspend(self):
        # Cancelling these in reverse order is important
        # If a parent thread cancels, it will cancel a child.
        # We want to be the one that cancels every task,
        #  so we cancel the children before the parents.
        for task in list(reversed(self._open_tasks)):
            if not task.done() or task.cancelled() or task.cancelling():
                quest_logger.debug(f'Suspending task {task.get_name()}')
                suspend_task(task)

    async def suspend(self):
        # Once each task has been marked for cancellation
        #  we await each task in order to allow the cancellation
        #  to play out to completion.
        self.signal_suspend()

        for task in list(self._open_tasks):
            try:
                await task
            except asyncio.CancelledError:
                quest_logger.debug(f'Task {task.get_name()} was cancelled')
                pass
