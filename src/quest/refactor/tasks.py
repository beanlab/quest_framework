import asyncio
from asyncio import Task
from functools import wraps

from .ledger import Ledger
from .logging import quest_logger
from .replay_stream import ReplayStream
from .unique_ids import UniqueIdHandler
from .utils import get_function_name


class TaskManager:
    def __init__(self, workflow_id: str, unique_id_handler: UniqueIdHandler, ledger: Ledger,
                 replay_stream: ReplayStream):
        self._workflow_id = workflow_id
        self._unique_id_handler = unique_id_handler
        self._ledger = ledger
        self._replay_stream = replay_stream

        # We keep track of all open tasks so we can properly suspend them
        self._open_tasks: list[Task] = []

    def start_task(self, parent_task_id, func, args, kwargs, task_name: str | None, task_factory):
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
        self._replay_stream.start_task_replay(task_id)
        self._open_tasks.append(task)
        task.add_done_callback(lambda t: self._open_tasks.remove(t) if t in self._open_tasks else None)

        return task
