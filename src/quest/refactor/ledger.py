class Ledger:
    def __init__(self):
        self._history = []
        self._gates = {}
        # Indicate when all gates have been cleared, i.e. replay is complete

    async def get_next_record(self, task_id): ...  # await gates until getting to next task_id record

    async def add_record(self, task_id, record): ...  # add record to ledger

    async def remove_records(self, records_to_remove): ...  # to be used by prune
