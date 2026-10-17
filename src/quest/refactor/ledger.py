from copy import deepcopy
from typing import Any, TypedDict

from .tasks import get_task_id
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

    def __init__(self):
        self._history = []
        self._gates = {}
        # Indicate when all gates have been cleared, i.e. replay is complete

    async def add_record(self, entry_type: str, entry_id: str, data):
        self._history.append(LedgerEntry(
            entry_type=entry_type,
            timestamp=get_current_timestamp(),
            task_id=get_task_id(),
            entry_id=entry_id,
            data=data
        ))

    async def get_records(self) -> list[LedgerEntry]:
        return deepcopy(self._history)

    # This method is used by pruning in step_handler.py
    # Pruning is an essential part of successful replay logic
    # and involves an intimate manipulation of the ledger
    async def _remove_records(self, records_to_remove: list[LedgerEntry]):
        for record in records_to_remove:
            self._history.remove(record)
