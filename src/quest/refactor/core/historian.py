import asyncio
import contextvars
from asyncio import Task
from contextvars import ContextVar
from typing import Callable

from .actions import ActionHandler
from .ledger import Ledger
from .replay_stream import ReplayStream
from .serializer import StepSerializer
from .state import StateHandler
from .step_handler import StepHandler
from .tasks import TaskManager
from .unique_ids import UniqueIdHandler

historian_context = ContextVar('historian')
task_id = ContextVar('task_id')


class Historian:

    def __init__(self, workflow_id: str, entrypoint: Callable, ledger: Ledger, serializer: StepSerializer):
        self._workflow_id = workflow_id
        self._external_task_id = f'{workflow_id}._external'
        self._entrypoint = entrypoint

        self._ledger = ledger
        self._serializer = serializer
        self._unique_id_handler: UniqueIdHandler = None
        self._step_handler: StepHandler = None
        self._state_handler: StateHandler = None
        self._action_handler: ActionHandler = None
        self._replay_stream: ReplayStream = None

    async def _prepare_replay(self):
        # Called at the beginning of run
        self._unique_id_handler = UniqueIdHandler(self._workflow_id, self._external_task_id)
        self._replay_stream = ReplayStream(await self._ledger.get_records())
        self._step_handler = StepHandler(self._ledger, self._unique_id_handler, self._replay_stream, self._serializer)
        self._state_handler = StateHandler()
        self._action_handler = ActionHandler()
        self._task_manager = TaskManager(self._workflow_id, self._unique_id_handler, self._ledger, self._replay_stream)

    def _get_current_task_id(self):
        return task_id.get(default=self._external_task_id)

    # Versioning of code
    async def when(self):
        return self._replay_stream.when()

    # Steps

    async def run_step(self, func_name, func: Callable, *args, **kwargs):
        task_id = self._get_current_task_id()
        return await self._step_handler.run_step(task_id, func_name, func, *args, **kwargs)

    # Resources
    async def add_state(self, ident, name, value):
        return self._state_handler.set_state(ident, name, value)

    async def remove_state(self, ident, name):
        return self._state_handler.delete_state(ident, name)

    async def get_state(self, ident, name=None):
        return self._state_handler.get_state(ident, name=name)

    async def add_action(self, ident, name, action_func):
        return self._action_handler.add_action(ident, name, action_func)

    async def remove_action(self, ident, name):
        return self._action_handler.remove_action(ident, name)

    async def call_action(self, ident, name, *args, **kwargs):
        action = self._action_handler.get_action(ident, name)
        task = self.start_task(action, args, kwargs, task_name=name)
        return await task

    # Tasks
    async def start_task(self, func, args, kwargs, task_name=None, task_factory=asyncio.create_task) -> Task:
        # Normally, we generate the new task name ourselves, but the main and external event tasks will supply their own

        # Usually, we want normal asyncio tasks. However, the external event task and the main task need to be coupled.
        # Historian.run will launch these two tasks via a TaskGroup.create factory

        parent_task_id = self._get_current_task_id()
        return self._task_manager.start_task(
            parent_task_id,
            func,
            args,
            kwargs,
            task_name=task_name,
            task_factory=self._task_factory_with_historian_context(task_factory),
        )

    def _task_factory_with_historian_context(self, task_factory):
        def create_task(coroutine, name=None):
            ctx = contextvars.copy_context()
            ctx.run(historian_context.set, self)
            ctx.run(task_id.set, name)
            return task_factory(
                coroutine,
                name=name,
                context=ctx,
            )

        return create_task

    async def _external_handler(self):
        try:
            quest_logger.debug(f'External event handler {self._get_task_name()} starting')
            async for next_record in self._task_replay_records(self._get_external_task_name()):
                with next_record as record:
                    if record['type'] == 'external':
                        await self._resource_runtime.replay_external_event(record)

                    elif record['type'] == 'set_version':
                        self._evolution_runtime.replay_version(record)

                    elif record['type'] == 'configuration':
                        await self._evolution_runtime.run_configuration(record)

            quest_logger.debug(f'External event handler {self._get_task_name()} completed')
        except Exception:
            quest_logger.exception('Error in _external_handler')
            raise

    # Workflow entrypoint
    async def run(self):
        ...

    async def suspend(self):
        ...
