from copy import deepcopy
from typing import Any, TypedDict

from .suspension import is_suspension
from .history import History
from .utils import get_current_timestamp


class LedgerEntry(TypedDict):
    entry_type: str
    timestamp: str
    task_id: str
    entry_id: str  # step id, resource id, etc.
    data: Any


class Ledger:
    # TODO - whether here or a layer down (probably down?) we need to buffer in memory and write intermittently
    # in order to maintain performance in the code execution
    # This affects adding

    def __init__(self, history: History[LedgerEntry]):
        self._history = history
        self._gates = {}
        # Indicate when all gates have been cleared, i.e. replay is complete

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # When the workflow is complete, meaning it exits cleanly or raises a non-suspending exception,
        #  we clean up the remaining records in the ledger.
        # Most of the records are cleaned up by the step pruning,
        #  but step pruning always leaves behind the closing "step end" record
        #  which we want to delete once the workflow is finished
        #  (so we don't have a memory leak hanging on to obsolete records)

        # So, we clear the history:
        # 1) after the workflow finishes cleanly
        # 2) after the workflow finished with an Exception of any kind
        # 3) EXCEPT a suspending cancellation

        if exc_type is None or not is_suspension(exc_val):
            await self.clear()
            
    async def add_record(self, task_id, entry_type: str, entry_id: str, data):
        self._history.append(LedgerEntry(
            entry_type=entry_type,
            timestamp=get_current_timestamp().isoformat(),
            task_id=task_id,
            entry_id=entry_id,
            data=data
        ))

    async def get_records(self) -> list[LedgerEntry]:
        return deepcopy(self._history)

    async def remove_records(self, records_to_remove: list[LedgerEntry]):
        for record in records_to_remove:
            self._history.remove(record)

    async def clear(self):
        self._history.clear()
