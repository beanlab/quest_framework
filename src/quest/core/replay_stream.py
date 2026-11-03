import asyncio
import datetime
import traceback
from typing import Callable, AsyncGenerator

from .ledger import LedgerEntry
from .utils import get_id, get_current_timestamp
from ..logs import quest_logger


class NextRecord:
    def __init__(self, current_record: LedgerEntry, on_close: Callable):
        self.current_record = current_record
        self.on_close = on_close

    def __enter__(self) -> LedgerEntry:
        return self.current_record

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.on_close(self.current_record, exc_type, exc_val, exc_tb)


class ReplayStream:
    """A ReplayStream should be constructed anew for each replay"""

    def __init__(self, history: list[LedgerEntry]):
        self._history = history

        # Each task needs to replay its separate event records,
        #  but the records are all interleaved.
        #  We use _task_replay_records to create a stream of records
        #  that belong to a specific task.
        # _replay_records stores the generators for each task ID
        # See also _next_record
        self._replay_records: dict[str, AsyncGenerator[NextRecord, None] | None] = {}

        self._current_record_timestamp: dict[str, datetime.datetime] = {}

        # To ensure that past events are replayed in the exact order
        #  they were created, a record gate exists for each record.
        #  When a task finishes with a record, it opens the gate
        #  which allows every task to move to the next record
        # If something goes wrong while processing a record,
        #  the error is put here, leading to the replay failing
        self._record_gates: dict[str, asyncio.Future] = {
            get_id(record): asyncio.Future()
            for record in self._history
        }

        self._last_record_gate: asyncio.Future | None = None
        for self._last_record_gate in self._record_gates.values():
            pass  # i.e. when this loop finishes, _last_record_gate will be the last one

    def when(self) -> datetime.datetime:
        return (
            max(self._current_record_timestamp.values())
            if self._current_record_timestamp
            else get_current_timestamp()
        )

    async def replay_complete(self):
        if self._last_record_gate is not None:
            await self._last_record_gate

    async def get_next_record(self, task_id) -> NextRecord | None:
        if task_id not in self._replay_records:
            self._replay_records[task_id] = await self._start_task_replay(task_id)

        if self._replay_records[task_id] is None:
            # The replay record stream will be None once all records for this task are consumed
            # This signals that live play will begin for this task
            if task_id in self._current_record_timestamp:
                del self._current_record_timestamp[task_id]
            return None

        try:
            current_record = await self._replay_records[task_id].asend(None)
            self._current_record_timestamp[task_id] = datetime.datetime.fromisoformat(
                current_record.current_record['timestamp']
            )
            return current_record

        except StopAsyncIteration:
            self._replay_records[task_id] = None
            del self._current_record_timestamp[task_id]
            return None

    async def _start_task_replay(self, task_id):
        self._replay_records[task_id] = self._get_task_replay_records(task_id)

    async def _get_task_replay_records(self, task_id):
        """Yield the tasks for this task ID"""
        for record in self._history:
            # If the record belongs to another task, we need to wait
            #  for that other task to finish with the record
            #  before we move on
            if record['task_id'] != task_id:
                if (gate := self._record_gates[get_id(record)]).done():
                    if gate.exception() is not None:
                        quest_logger.debug(f'{task_id} found {record} errored: {gate.exception()}')
                    else:
                        quest_logger.debug(f'{task_id} found {record} completed')
                else:
                    quest_logger.debug(f'{task_id} waiting on {record}')
                # We await either way: if the gate is pending, we wait;
                # if the gate is errored, the wait is short
                await gate

            else:  # task ID matches
                def complete(r, exc_type, exc_val, exc_tb):
                    if exc_type is not None:
                        exc_info = "".join(traceback.format_exception(exc_type, exc_val, exc_tb))
                        quest_logger.debug(f'Noting that record {r} raised: \n{exc_info}')
                    # Note:
                    # While these are Futures, the record gates are only used as gates
                    # The return values are never used
                    # Thus, even if there was an error when the task completed
                    # we simply want to indicate the gate is finished
                    # The relevant error will be raised in handle_step
                    quest_logger.debug(f'{task_id} completing {r}')
                    self._record_gates[get_id(r)].set_result(None)

                # noinspection PyUnboundLocalVariable
                quest_logger.debug(f'{task_id} replaying {record}')
                yield NextRecord(record, complete)

        quest_logger.debug(f'Replay for {task_id} complete')
        await self.replay_complete()
