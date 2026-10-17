from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .tasks import get_task_id


def _get_current_timestamp() -> str:
    return datetime.utcnow().isoformat()


@dataclass
class LedgerEntry:
    type: str
    timestamp: str
    task_id: str
    id: str  # step id, resource id, etc.
    data: Any


class Ledger:
    def __init__(self):
        self._history = []
        self._gates = {}
        # Indicate when all gates have been cleared, i.e. replay is complete

    async def get_next_record(self): ...  # await gates until getting to next task_id record

    async def add_record(self, record_type: str, operation_id: str, data):
        self._history.append(LedgerEntry(
            type=record_type,
            timestamp=_get_current_timestamp(),
            task_id=get_task_id(),
            id=operation_id,
            data=data
        ))

    async def remove_records(self, records_to_remove): ...  # to be used by prune
