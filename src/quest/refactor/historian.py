from typing import Callable

from .ledger import Ledger
from .replay_stream import ReplayStream
from .step_handler import StepHandler
from .unique_ids import UniqueIdHandler


class Historian:

    def __init__(self, workflow_id: str, entrypoint: Callable, ledger: Ledger):
        self._workflow_id = workflow_id
        self._entrypoint = entrypoint

        self._ledger = ledger
        self._unique_id_handler: UniqueIdHandler = None
        self._step_handler: StepHandler = None
        self._replay_stream: ReplayStream = None

    async def _prepare_replay(self):
        # Called at the beginning of run
        self._unique_id_handler = UniqueIdHandler(self._workflow_id)
        self._replay_stream = ReplayStream(await self._ledger.get_records())
        self._step_handler = StepHandler(self._ledger, self._unique_id_handler, self._replay_stream)

    # Versioning of code
    async def when(self):
        return self._replay_stream.when()

    # Steps

    async def run_step(self, func_name, func: Callable, *args, **kwargs):
        return await self._step_handler.run_step(func_name, func, *args, **kwargs)

    # Resources
    async def add_state(self, ident, name, get_func):
        ...

    async def remove_state(self, ident, name):
        ...

    async def add_action(self, ident, name, action_func):
        ...

    async def remove_action(self, ident, name):
        ...

    async def get_state(self, ident):
        ...  # i.e. get_resources

    async def call_action(self, ident, name, *args, **kwargs):
        ...  # i.e. handle_external_event

    # Tasks
    async def start_task(self, task_name, callable, *args, **kwargs) -> Task:
        ...

    # Workflow entrypoint
    async def run(self):
        ...

    async def suspend(self):
        ...
