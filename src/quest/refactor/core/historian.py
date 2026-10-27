import asyncio
import contextvars
from asyncio import Task
from contextvars import ContextVar
from functools import wraps
from typing import Callable

from quest.refactor.core.serializer import NoOpSerializer

from .history import History
from .ledger import Ledger, LedgerEntry
from .replay_stream import ReplayStream
from .resources import ResourceManager
from .serializer import StepSerializer
from .step_handler import StepHandler
from .suspension import is_suspension
from .tasks import TaskManager
from .unique_ids import UniqueIdHandler
from .utils import get_function_name

historian_context = ContextVar('historian')
current_task_id = ContextVar('task_id')


def find_historian() -> 'Historian':
    return historian_context.get()


class Historian:
    """Invokes a workflow durably while exposing the dynamic workflow resource API"""

    # Historian's job is to take a function and a ledger
    #  and replay what needs to be replayed and run what needs to run
    # It is the interface to the running workflow, where steps, tasks, and resources
    #  are registered and accessed

    def __init__(
            self,
            workflow_id: str,
            func: Callable,
            *args,
            serializer: StepSerializer = NoOpSerializer,
            storage: History[LedgerEntry] = None,
            **kwargs
    ):
        self._workflow_id = workflow_id
        self._func = func
        self._args = args
        self._kwargs = kwargs
        self._storage = storage if storage is not None else []
        self._serializer = serializer

    async def __aenter__(self):
        unique_id_handler = UniqueIdHandler()
        self._ledger = Ledger(self._storage)
        self._replay_stream = ReplayStream(await self._ledger.get_records())
        self._step_handler = StepHandler(self._ledger, unique_id_handler, self._replay_stream, self._serializer)
        self._task_manager = TaskManager(self._ledger, unique_id_handler, self._replay_stream)
        self._resource_manager = ResourceManager(self._ledger, self._replay_stream)

        self._workflow_task = self.run()

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # We are outside the historian's context
        # If something has gone wrong on this side, we want the historian to suspend
        # Or if we are exiting the context before the historian finishes, we want to suspend
        await self.suspend()

    def __await__(self):
        return self._workflow_task.__await__()

    def _get_current_task_id(self):
        return current_task_id.get(self._workflow_id)

    # Versioning of code
    async def when(self):
        return self._replay_stream.when()

    # Steps
    async def run_step(self, func_name, func: Callable, *args, **kwargs):
        task_id = self._get_current_task_id()
        return await self._step_handler.run_step(task_id, func_name, func, *args, **kwargs)

    # Resources
    async def set_state(self, ident, name, value, external):
        return await self._resource_manager.set_state(self._get_current_task_id(), ident, name, value, external)

    async def get_state(self, ident, name):
        return await self._resource_manager.get_state(self._get_current_task_id(), ident, name)

    async def remove_state(self, ident, name):
        return await self._resource_manager.delete_state(self._get_current_task_id(), ident, name)

    async def add_action(self, ident, name, action_func, external):
        return await self._resource_manager.add_action(self._get_current_task_id(), ident, name, action_func, external)

    async def remove_action(self, ident, name):
        return await self._resource_manager.remove_action(self._get_current_task_id(), ident, name)

    async def invoke_action(self, ident, name, *args, **kwargs):
        return await self._resource_manager.invoke_action(self._get_current_task_id(), ident, name, *args, **kwargs)

    async def external_invoke_action(self, ident, name, *args, **kwargs) -> Task:
        # This is the function that should be called from tasks that are not managed by the Historian
        # Yes, an async function returning a Task. See start_task.

        task = await self.start_task(
            self._resource_manager.invoke_external_action,
            (self._get_current_task_id(), ident, name, *args),
            kwargs,
            task_name=name
        )
        return task

    def get_resource_stream(self, identities: list[str]):
        return self._resource_manager.resource_stream(identities)

    # Tasks
    async def start_task(self, func, args, kwargs, task_name=None, task_factory=asyncio.create_task) -> Task:
        # Yes, an async function returning a Task
        # Getting the Task started (submitted) requires a little waiting,
        # but then the caller can continue without the Task work having completed

        # Normally, we generate the new task name ourselves, but the main and external event tasks will supply their own

        # Usually, we want normal asyncio tasks. However, the external event task and the main task need to be coupled.
        # Historian.run will launch these two tasks via a TaskGroup.create factory

        parent_task_id = self._get_current_task_id()
        return await self._task_manager.start_task(
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
            ctx.run(current_task_id.set, name)
            return task_factory(
                coroutine,
                name=name,
                context=ctx,
            )

        return create_task

    #
    # Run
    #
    def _wrap_with_args_remembered(self, func, args, kwargs):
        @wraps(func)
        async def wrapped():
            nonlocal args, kwargs
            args = await self.run_step('args', lambda: args)
            kwargs = await self.run_step('kwargs', lambda: kwargs)
            result = await self.run_step(get_function_name(func), func, *args, **kwargs)
            # TODO - after refactor resource streams, wire this concept back in
            # self._resource_runtime.notify_of_workflow_stop()
            return result

        return wrapped

    def _wrap_with_history_cleanup(self, func):
        # When the workflow is complete, we clean up the remaining records in ledger
        # Most of the records are cleaned up by the step pruning,
        #  but step pruning always leaves behind the closing "step end" record
        #  which we want to delete once the workflow is finished
        #  (so we don't have a memory leak hanging on to obsolete records)

        # So, we clear the history:
        # 1) after the workflow finishes cleanly
        # 2) after the workflow finished with an Exception of any kind
        # 3) EXCEPT a suspending cancellation
        # We can't use a `finally` because there is a condition (suspension)
        #  in which it shouldn't be cleared.

        @wraps(func)
        async def wrapped(*args, **kwargs):
            try:
                await func(*args, **kwargs)
                await self._ledger.clear()

            except asyncio.CancelledError as cancel:
                if not is_suspension(cancel):
                    await self._ledger.clear()
                raise

            except:
                # Any other
                await self._ledger.clear()
                raise

        return wrapped

    def _wrap_with_resource_stream_handling(self, func):
        @wraps(func)
        async def wrapped(*args, **kwargs):
            try:
                result = await func(*args, **kwargs)
                self._resource_manager.signal_workflow_complete()
                return result

            except BaseException as exc:
                self._resource_manager.signal_workflow_error(exc)
                raise

        return wrapped

    async def run(self):
        # TODO - add debug logging throughout
        # quest_logger.debug(f'Running workflow {self.workflow_id}')

        # Wrap func in error handling
        # The order of wrapping is important
        # We want the records for remembering args and running the workflow
        #  to be cleaned up
        # And we want errors in any of these to properly close resource streams
        func = self._wrap_with_args_remembered(self._func, self._args, self._kwargs)
        func = self._wrap_with_history_cleanup(func)
        func = self._wrap_with_resource_stream_handling(func)

        # We use a TaskGroup here to ensure that the external_replay task
        #  exits when the main task fails
        # The external_replay will exit normally when replay is complete
        try:
            async with asyncio.TaskGroup() as _task_factory:
                external_task_id = f'{self._workflow_id}.external'
                # noinspection PyTypeChecker
                # The typechecker is confused about the signature of create_task
                # What it expects and what it sees are identical, but it still raises a warning
                await self.start_task(
                    self._resource_manager.external_action_handler,
                    (external_task_id,),
                    {},
                    task_name=external_task_id,
                    task_factory=_task_factory.create_task
                )

                # noinspection PyTypeChecker
                return await self.start_task(
                    func,
                    (),
                    {},
                    task_factory=_task_factory.create_task
                )

        except ExceptionGroup as eg:
            if len(eg.exceptions) == 1:
                raise eg.exceptions[0]
            else:
                raise

        # Workflow logic completed

    def signal_suspend(self):
        # This function is synchronous because it is called by the SIGINT handler,
        #  which requires a synchronous function

        # quest_logger.debug(f'-- Suspending {self.workflow_id} --')

        self._resource_manager.signal_suspension()
        self._task_manager.signal_suspend()

    async def suspend(self):
        self.signal_suspend()
        await self._task_manager.suspend()
