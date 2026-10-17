from typing import Callable

from .ledger import Ledger
from .step_handler import StepHandler
from .tasks import get_task_id


class PrefixHandler:
    def __init__(self):
        self.reset()

    def reset(self):
        self._unique_ids: set[str] = set()
        self._prefix = {}

    def _get_prefixed_name(self, event_name: str) -> str:
        return '.'.join(self._prefix[get_task_id()]) + '.' + event_name

    def get_unique_id(self, event_name: str) -> str:
        prefixed_name_root = self._get_prefixed_name(event_name)
        prefixed_name = prefixed_name_root
        counter = 0
        while prefixed_name in self._unique_ids:
            counter += 1
            prefixed_name = f'{prefixed_name_root}_{counter}'
        self._unique_ids.add(prefixed_name)
        return prefixed_name

    def push_prefix(self, unique_step_name: str):
        self._prefix[get_task_id()].append(unique_step_name)

    def pop_prefix(self):
        self._prefix[get_task_id()].pop(-1)


class Historian:
    """This class embodies how the Ledger is used to create the Quest experience"""

    def __init__(self, ledger: Ledger, entrypoint: Callable):
        self._ledger = ledger
        self._prefix_handler = PrefixHandler()
        self._step_handler = StepHandler(ledger, self._prefix_handler)

    # Versioning of code
    async def when(self):
        ...  # Return the timestamp of the currently held record

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
