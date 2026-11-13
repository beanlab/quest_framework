import asyncio
import contextvars
from asyncio import CancelledError
from contextvars import ContextVar
from functools import wraps
from typing import Callable

from .history import History
from .ledger import Ledger, LedgerEntry
from .replay_stream import ReplayStream
from .resources import ResourceManager
from .serializer import NoOpSerializer
from .serializer import StepSerializer
from .signal_handling import track_workflow, untrack_workflow
from .step_handler import StepHandler
from .suspension import is_suspension, is_interrupt_suspension
from .tasks import TaskManager
from .unique_ids import UniqueIdHandler
from .utils import get_function_name
from ..logs import current_task_id, quest_logger

historian_context = ContextVar('historian')


def find_historian() -> 'Historian':
    return historian_context.get()


def logged(func):
    @wraps(func)
    def wrapped_with_logs(*args, **kwargs):
        quest_logger.debug('CALL %s(%s, %s)', get_function_name(func), args, kwargs)
        result = func(*args, **kwargs)
        quest_logger.debug('RETURN %s(%s, %s) -> %s', get_function_name(func), args, kwargs, result)
        return result

    return wrapped_with_logs


def alogged(func):
    @wraps(func)
    async def wrapped_with_logs(*args, **kwargs):
        quest_logger.debug('CALL %s(%s, %s)', get_function_name(func), args, kwargs)
        result = await func(*args, **kwargs)
        quest_logger.debug('RETURN %s(%s, %s) -> %s', get_function_name(func), args, kwargs, result)
        return result

    return wrapped_with_logs


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
            serializer: StepSerializer = NoOpSerializer(),
            storage: History[LedgerEntry] = None,
            **kwargs
    ):
        # public for `alias`
        self.workflow_id = workflow_id

        self._func = func
        self._args = args
        self._kwargs = kwargs
        self._storage = storage if storage is not None else []
        self._serializer = serializer

    async def __aenter__(self):
        self._unique_id_handler = UniqueIdHandler()
        self._ledger = Ledger(self._storage)
        history = await self._ledger.get_records()
        self._external_events = [event for event in history if event['entry_type'] == 'invoke']
        self._other_events = [event for event in history if event['entry_type'] != 'invoke']
        self._replay_stream = ReplayStream(self._other_events)
        self._step_handler = StepHandler(self._ledger, self._unique_id_handler, self._replay_stream, self._serializer)
        self._task_manager = TaskManager(self._ledger, self._unique_id_handler, self._replay_stream)
        self._resource_manager = ResourceManager(self._ledger, self._replay_stream)

        track_workflow(self)

        # We don't await the task because we want to decouple
        # the parent task from the workflow work,
        # allowing them to run independently
        self.workflow_task: asyncio.Task = self.run()
        self._workflow_complete = False

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        quest_logger.debug('Exiting historian: %s %s', exc_type, exc_val)
        # We are outside the historian's context
        # If something has gone wrong on this side, we want the historian to suspend
        # Or if we are exiting the context before the historian finishes, we want to suspend
        if not self._workflow_complete:  # TODO - was this necessary?
            quest_logger.debug('Workflow not complete; suspending')
            await self.suspend()

        untrack_workflow(self)

        try:
            await self.workflow_task  # TODO - was this necessary?
        except CancelledError as cancel:
            if not is_suspension(cancel):
                raise

        # Reconvert SIGINT back into the interrupts the outside world expected
        if is_interrupt_suspension(exc_val):
            raise KeyboardInterrupt from exc_val

        # Swallow regular suspensions as the Historian is their boundary
        #  and the Historian is now exiting
        if exc_val and is_suspension(exc_val):
            return True

    def __await__(self):
        return self.workflow_task.__await__()

    def _get_current_task_id(self):
        return current_task_id.get(self.workflow_id)

    # Versioning of code
    @alogged
    async def when(self):
        return self._replay_stream.when()

    # Steps
    @alogged
    async def run_step(self, func_name, func: Callable, *args, **kwargs):
        task_id = self._get_current_task_id()
        return await self._step_handler.run_step(task_id, func_name, func, *args, **kwargs)

    # Resources
    @alogged
    async def set_state(self, ident, name, value, external):
        return await self._resource_manager.set_state(self._get_current_task_id(), ident, name, value, external)

    @alogged
    async def get_state(self, ident, name):
        return await self._resource_manager.get_state(self._get_current_task_id(), ident, name)

    @alogged
    async def remove_state(self, ident, name):
        return await self._resource_manager.delete_state(self._get_current_task_id(), ident, name)

    @alogged
    async def add_action(self, ident, name, action_func, external):
        return await self._resource_manager.add_action(self._get_current_task_id(), ident, name, action_func, external)

    @alogged
    async def remove_action(self, ident, name):
        return await self._resource_manager.remove_action(self._get_current_task_id(), ident, name)

    @alogged
    async def invoke_action(self, ident, name, *args, **kwargs):
        return await self._resource_manager.invoke_action(self._get_current_task_id(), ident, name, *args, **kwargs)

    @logged
    def external_invoke_action(self, ident, name, *args, **kwargs) -> asyncio.Task:
        # This is the function that should be called from tasks that are not managed by the Historian
        self._ledger.add_record(
            self.workflow_id,
            'invoke',
            action_id := self._unique_id_handler.get_unique_id(self.workflow_id, name),
            (action_id, ident, name, args, kwargs)
        )
        return self._external_invoke_action(action_id, ident, name, args, kwargs)

    def _external_invoke_action(self, action_id, ident, name, args, kwargs) -> asyncio.Task:
        async def action(*_args, **_kwargs):
            return await self._resource_manager.invoke_action(
                *_args, **_kwargs
            )

        action.__name__ = name

        quest_logger.debug(f'External event starting: %s %s %s', action_id, ident, name)
        task = self.start_task(
            action,
            (action_id, ident, name, *args),
            kwargs,
            # task_name=name
        )
        return task

    async def _external_action_handler(self, external_action_records: list[LedgerEntry]):
        # In live play, external events come from other tasks
        # During replay, we need to perform these actions again
        # Most events in the ledger are encountered through the natural flow of the code in replay.
        #  In these cases, we simply confirm that the event happening matches what was written in the ledger.
        # However, the external events are not triggered through the workflow code, so we can't rely on workflow
        #  code to invoke the events at the right times.
        # Instead, we use the ledger to identify when external events happened and replay them.
        # So, this method looks through the ledger for records created by `invoke_external_action` to replay them.
        try:
            for record in external_action_records:
                action_id, identity, name, args, kwargs = record['data']
                self._external_invoke_action(action_id, identity, name, args, kwargs)

        except Exception:
            quest_logger.exception('Error in _external_handler')
            raise

    @logged
    def get_resource_stream(self, identities: list[str]):
        return self._resource_manager.resource_stream(identities)

    # Tasks
    @logged
    def start_task(self, func, args, kwargs, task_name=None, task_factory=asyncio.create_task) -> asyncio.Task:
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

    def _wrap_with_exception_handling(self, func):
        @wraps(func)
        async def wrapped(*args, **kwargs):
            try:
                result = await func(*args, **kwargs)
                self._resource_manager.signal_workflow_complete()
                self._workflow_complete = True  # TODO - was this necessary?
                return result

            except BaseException as exc:
                if not is_suspension(exc):
                    self._resource_manager.signal_workflow_error(exc)
                    self._workflow_complete = True  # TODO - was this necessary?
                raise

        return wrapped

    async def _run(self):
        quest_logger.debug(f'Running workflow {self.workflow_id}')

        # Wrap func in error handling
        # The order of wrapping is important
        # We want the records for remembering args and running the workflow
        #  to be cleaned up
        # And we want errors in any of these to properly close resource streams
        func = self._wrap_with_args_remembered(self._func, self._args, self._kwargs)
        func = self._wrap_with_history_cleanup(func)
        func = self._wrap_with_exception_handling(func)

        # We use a TaskGroup here to ensure that the external_replay task
        #  exits when the main task fails
        # The external_replay will exit normally when replay is complete

        try:
            async with asyncio.TaskGroup() as _task_factory:
                external_task_id = f'{self.workflow_id}.external'
                # noinspection PyTypeChecker
                # The typechecker is confused about the signature of create_task
                # What it expects and what it sees are identical, but it still raises a warning
                self.start_task(
                    self._external_action_handler,
                    (self._external_events,),
                    {},
                    task_name=self.workflow_id + '.external_replay',
                    task_factory=_task_factory.create_task
                )

                # noinspection PyTypeChecker
                return await self.start_task(
                    func,
                    (),
                    {},
                    task_name=self.workflow_id,
                    task_factory=_task_factory.create_task
                )

        except CancelledError as cancel:
            if not is_suspension(cancel):
                raise
        # TODO - is this important? Users can use except* to catch sub exceptions
        # except ExceptionGroup as eg:
        #     if len(eg.exceptions) == 1:
        #         raise eg.exceptions[0]
        #     else:
        #         raise

        # Workflow logic completed

    @logged
    def run(self) -> asyncio.Task:
        create_task = self._task_factory_with_historian_context(asyncio.create_task)
        return create_task(self._run(), name=self.workflow_id + '.init')

    # TODO - capture from the thoughts below the details that are still relevant to the current design
    # Historian manages a ledger
    # It runs a single task that waits for action invocations
    # Each action is started on a new task with arg/kwarg remembered
    # Replay simply kicks off these tasks in order.
    # Perhaps: a queue provides action invocation requests; on replay the queue is repopulated with requests;
    #  the external task grabs these replayed requests and runs them; each task starts, but is immediately gated
    #  on the task start records and ultimately replay at the correct times.

    # We need the idea of a core external action: it is run within the same task group as the core historian task.
    # If any of these fail, they are all canceled together. The main workflow function should be core.
    # Could a system include multiple core workflows in the same Historian scope?

    # If the main workflow function is just a special action,
    # should it be treated special besides being the first to be called?
    # What identity should it be registered with?
    # Or should Historian support invoking any provided function? `invoke(func, *args, **kwargs)` simply calls
    #  the provided function within the context of the Historian.
    # External actions (or internal actions) are just looking up a registered function by name and calling `invoke`.

    # Each external action has the workflow ID + function name, uniquified. These are recorded as external events.
    # Right at the start of replay, tasks are started for each external action from these records.
    # These records are ignored when creating the record gates.
    # These tasks are started immediately, but then gate on the start_task record at the beginning of each.

    # We need special handling around the core workflow functions.
    # Errors need to propagate to other core functions.
    #  This involves running in the same task group and in signalling errors to the replay stream.
    # When all core functions are successfully completed, we clean up the ledger.

    # All external actions need arg/kwarg memory before the task is started.
    # OR, we remember the args, kwargs in the external event record.
    # Storing them in the record is more atomic. If a suspension happens before these are stored, we'd have a problem.

    def init(self):
        for event in self._ledger.get_records():  # get 'invoke' records
            self._external_events.put(event)
        create_task(self.event_handler)

    async def event_handler(self):
        async for event in self._external_events:
            self.invoke(*event)

    def invoke(self, func, *args, **kwargs) -> asyncio.Task:
        # Remember args, kwargs in ledger
        self._ledger.add_record(
            self._get_current_task_id(),
            'invoke',
            self._unique_ids.get(get_function_name(func)),
            (args, kwargs)
        )
        self._external_events.put((func, args, kwargs))
        # Where does the Task come from?

    async def main(self, func, *args, **kwargs):
        self.schedule_external_event(func, args, kwargs)

    # It may be that we're not far off already.
    # Just make sure that some `run` logic is used in external event handling
    # and that `run` uses external event to call the main function.

    # We need to ensure that __init__ setups up the run to use the replay correctly
    # Maybe this: calling run starts the external thread and posts to the event queue?

    # The difference between the primary entry point and other external events is that
    # The primary entry point is explicitly called on replay (this call is what starts the replay),
    #  whereas other external events need to be kicked off by the historian from records in the ledger.
    # Thus, all primary entry points would need to be invoked together at the beginning, otherwise
    #  the subsequent events would need to be replayed by historian (you can't rely on outside the historian
    #  to replay the other primary events at the right times).
    # If they are all going to be invoked at the same time, then you might as well just have one function,
    #  and it can be designed to do however many parallel tasks as desired.
    # SO, we only need one primary task.

    @logged
    def signal_suspend(self):
        # This function is synchronous because it is called by the SIGINT handler,
        #  which requires a synchronous function

        # quest_logger.debug(f'-- Suspending {self.workflow_id} --')

        self._resource_manager.signal_suspension()
        self._task_manager.signal_suspend()

    @alogged
    async def suspend(self):
        self.signal_suspend()
        await self._task_manager.suspend()
