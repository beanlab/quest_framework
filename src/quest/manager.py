import asyncio
import logging
import signal
from contextvars import ContextVar
from functools import wraps
from typing import Protocol, Callable, TypeVar, Any
from datetime import datetime

from .external import State, IdentityQueue, Queue, Event
from .historian import Historian, _Wrapper, SUSPENDED
from .history import History
from .persistence import BlobStorage
from .serializer import StepSerializer

from .utils import (
    serialize_exception,
    deserialize_exception,
)


class WorkflowNotFound(Exception):
    pass


class HistoryFactory(Protocol):
    def __call__(self, workflow_id: str) -> History: ...


class WorkflowFactory(Protocol):
    def __call__(self, workflow_type: str) -> Callable: ...


T = TypeVar('T')

workflow_manager = ContextVar('workflow_manager')


class DuplicateAliasException(Exception):
    ...


class WorkflowManager:
    """
    Runs workflow tasks
    It remembers which tasks are still active and resumes them on replay
    """

    def __init__(self, namespace: str, storage: BlobStorage, create_history: HistoryFactory,
                 create_workflow: WorkflowFactory, serializer: StepSerializer):
        self._namespace = namespace
        self._storage = storage
        self._create_history = create_history
        self._create_workflow = create_workflow
        self._workflow_data = []  # Tracks all workflows
        self._workflows: dict[str, Historian] = {}
        self._workflow_tasks: dict[str, asyncio.Task] = {}
        self._alias_dictionary = {}
        self._serializer: StepSerializer = serializer
        self._results: dict[str, Any] = {}
        self._futures: dict[str, asyncio.Future] = {}  # Track futures for workflow results

    async def __aenter__(self) -> 'WorkflowManager':
        """Load the workflows and get them running again"""

        def our_handler(sig, frame):
            self._quest_signal_handler(sig, frame)
            raise asyncio.CancelledError(SUSPENDED)

        signal.signal(signal.SIGINT, our_handler)

        if self._storage.has_blob(self._namespace):
            self._workflow_data = self._storage.read_blob(self._namespace)

        # Check storage to load stored workflow results from persistent storage
        if self._storage.has_blob(f'{self._namespace}_results'):
            self._results = self._storage.read_blob(f'{self._namespace}_results')

        # Rehydration
        for wtype, wid, args, kwargs, delete_on_finish, start_time in self._workflow_data:
            print(f"[DEBUG] In __aenter__, rehydrating workflow: {wid}")

            # if future exists and done -> check SUSPENDED
            if wid in self._futures and self._futures[wid].done():
                future_result = self._futures[wid].result()

                if future_result == SUSPENDED:
                    print(f"[DEBUG] Workflow {wid} was suspended. Resuming execution.")
                    self._start_workflow(wtype, wid, args, kwargs, delete_on_finish=delete_on_finish)
                    continue  # Skip recreating future

            # Set new future if it doesn't exist or cancelled
            if wid not in self._futures or self._futures[wid].cancelled():
                print(f"[DEBUG] Workflow {wid} was cancelled, recreating future")
                self._futures[wid] = asyncio.Future()

            print(f"[DEBUG] Starting workflow {wid}.")
            self._start_workflow(wtype, wid, args, kwargs, delete_on_finish=delete_on_finish)

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Save whatever state is necessary before exiting"""
        self._storage.write_blob(self._namespace, self._workflow_data)
        self._storage.write_blob(f'{self._namespace}_results', self._results)
        for wid, historian in self._workflows.items():
            await historian.suspend()
        # clear cancelled futures
        to_remove = [wid for wid, future in self._futures.items() if future.cancelled()]
        for wid in to_remove:
            del self._futures[wid]

    def _quest_signal_handler(self, sig, frame):
        logging.debug(f'Caught KeyboardInterrupt: {sig}')
        for wid, historian in self._workflows.items():
            historian.signal_suspend()

    def _get_workflow(self, workflow_id: str):
        workflow_id = self._alias_dictionary.get(workflow_id, workflow_id)
        return self._workflows[workflow_id]

    def _remove_workflow(self, workflow_id: str):
        if workflow_id not in self._workflows:
            raise WorkflowNotFound(f"Workflow '{workflow_id}' not found.")

        self._workflows.pop(workflow_id)
        self._workflow_tasks.pop(workflow_id)
        removable = [d for d in self._workflow_data if d[1] == workflow_id]
        for item in removable:
            self._workflow_data.remove(item)

    def _start_workflow(self,
                        workflow_type: str, workflow_id: str, workflow_args, workflow_kwargs,
                        delete_on_finish: bool = True):

        if workflow_id not in self._futures or self._futures[workflow_id].cancelled():
            self._futures[workflow_id] = asyncio.Future()

        workflow_function = self._create_workflow(workflow_type)

        workflow_manager.set(self)

        history = self._create_history(workflow_id)
        historian: Historian = Historian(workflow_id, workflow_function, history, serializer=self._serializer)
        self._workflows[workflow_id] = historian

        self._workflow_tasks[workflow_id] = (task := historian.run(*workflow_args, **workflow_kwargs))

        task.add_done_callback(lambda t: self._store_result(workflow_id, t, delete_on_finish))

    async def delete_workflow(self, workflow_id: str):
        """
        Remove a workflow whether running or completed.
        If the workflow is not completed, it is suspended, then deleted.
        If not found, raises WorkflowNotFound error.
        """
        in_workflow_data = any(d[1] == workflow_id for d in self._workflow_data)
        active = workflow_id in self._workflows
        results = workflow_id in self._results or workflow_id in self._futures

        if not (in_workflow_data or active or results):
            raise WorkflowNotFound(f"Workflow '{workflow_id}' not found.")

        # Check if the task is running, if running, cancel right away
        if workflow_id in self._workflow_tasks:
            task = self._workflow_tasks[workflow_id]
            if not task.done():
                await self._get_workflow(workflow_id).suspend()

        if workflow_id in self._workflows:
            self._remove_workflow(workflow_id)
        if workflow_id in self._futures:
            del self._futures[workflow_id]
        if workflow_id in self._results:
            self._results.pop(workflow_id)

    def _store_result(self, workflow_id: str, task: asyncio.Task, delete_on_finish: bool):
        """Store the result or exception of a completed workflow"""
        print(f"[DEBUG] _store_result called for workflow_id={workflow_id}")
        if workflow_id not in self._futures:
            raise WorkflowNotFound(f"Workflow '{workflow_id}' not found in futures.")

        future = self._futures[workflow_id]

        if future is None:
            print("[DEBUG] Future is None, returning early.")
            return

        try:
            result = task.result()
            print(f"[DEBUG] Task completed successfully with result={result!r}")
            if delete_on_finish:
                if not future.done():
                    future.set_result(None)
            else:
                # store the result and set it on future
                self._results[workflow_id] = result
                if not future.done():
                    future.set_result(result)

        # task cancelled
        except asyncio.CancelledError as c:
            print(f"[DEBUG] Task cancelled error, c.args={c.args}")
            if c.args and c.args[0] == SUSPENDED:
                if not future.done():
                    future.set_result(SUSPENDED)
                # For suspension, remove only the active references - workflow data is preserved
                if workflow_id in self._workflow_tasks:
                    self._workflow_tasks.pop(workflow_id)
                if workflow_id in self._workflows:
                    self._workflows.pop(workflow_id)
                print(
                    "[DEBUG] Workflow suspended, references removed but future not deleted (successful handling cancellederror suspended).")
                # Exit without deleting future for rehydration later
                return
            # pass exception to future for other reason
            if not future.done():
                future.set_exception(c)

        except Exception as e:
            print(f"[DEBUG] Task raised general exception e={e}")
            serialized_exception = serialize_exception(e)
            serialized_exception["_quest_exception_type"] = serialized_exception.get("type", "generalException")
            self._results[workflow_id] = serialized_exception
            future.set_exception(e)

        finally:
            # remove active workflow tracked from the manager
            if not task.cancelled() and workflow_id in self._workflows:
                print(f"[DEBUG] Removing completed workflow {workflow_id}.")
                self._remove_workflow(workflow_id)

    def start_workflow(self, workflow_type: str, workflow_id: str, *workflow_args, delete_on_finish: bool = True,
                       **workflow_kwargs):
        """Start the workflow"""
        start_time = datetime.utcnow().isoformat()
        self._workflow_data.append(
            (workflow_type, workflow_id, workflow_args, workflow_kwargs, delete_on_finish, start_time))
        self._start_workflow(workflow_type, workflow_id, workflow_args, workflow_kwargs,
                             delete_on_finish=delete_on_finish)

    def has_workflow(self, workflow_id: str) -> bool:
        workflow_id = self._alias_dictionary.get(workflow_id, workflow_id)
        return workflow_id in self._workflows

    def get_workflow(self, workflow_id: str) -> asyncio.Task:
        workflow_id = self._alias_dictionary.get(workflow_id, workflow_id)
        return self._workflow_tasks[workflow_id]

    async def suspend_workflow(self, workflow_id: str):
        # Reset the future to avoid CancelledError blocking resumption
        if workflow_id in self._futures:
            future = self._futures[workflow_id]
            if not future.done():
                future.set_result(SUSPENDED)

        self._get_workflow(workflow_id)._individual_suspend = True
        await self._get_workflow(workflow_id).suspend()

    async def get_resources(self, workflow_id: str, identity):
        return await self._get_workflow(workflow_id).get_resources(identity)

    def get_resource_stream(self, workflow_id: str, identity):
        return self._get_workflow(workflow_id).get_resource_stream(identity)

    async def wait_for_completion(self, workflow_id: str, identity):
        return await self._get_workflow(workflow_id).wait_for_completion(identity)

    async def send_event(self, workflow_id: str, name: str, identity, action, *args, **kwargs):
        return await self._get_workflow(workflow_id).record_external_event(name, identity, action, *args, **kwargs)

    def _make_wrapper_func(self, workflow_id: str, name: str, identity, field, attr):
        # Why have _make_wrapper_func?
        # See https://stackoverflow.com/questions/3431676/creating-functions-or-lambdas-in-a-loop-or-comprehension

        @wraps(field)  # except that we need to make everything async now
        async def new_func(*args, **kwargs):
            # TODO - handle case where this wrapper is used in a workflow and should be stepped
            return await self.send_event(workflow_id, name, identity, attr, *args, **kwargs)

        return new_func

    def _wrap(self, resource: T, workflow_id: str, name: str, identity) -> T:
        dummy = _Wrapper()
        for attr in dir(resource):
            if attr.startswith('_'):
                continue

            field = getattr(resource, attr)
            if not callable(field):
                continue

            # Replace with function that routes call to historian
            new_func = self._make_wrapper_func(workflow_id, name, identity, field, attr)
            setattr(dummy, attr, new_func)

        return dummy

    async def _check_resource(self, workflow_id: str, name: str, identity):
        if (name, identity) not in await self.get_resources(workflow_id, identity):
            raise Exception(f'{name} is not a valid resource for {workflow_id}')
            # TODO - custom exception

    async def get_queue(self, workflow_id: str, name: str, identity) -> Queue:
        await self._check_resource(workflow_id, name, identity)
        return self._wrap(asyncio.Queue(), workflow_id, name, identity)

    async def get_state(self, workflow_id: str, name: str, identity: str | None) -> State:
        await self._check_resource(workflow_id, name, identity)
        return self._wrap(State(None), workflow_id, name, identity)

    async def get_event(self, workflow_id: str, name: str, identity: str | None) -> Event:
        await self._check_resource(workflow_id, name, identity)
        return self._wrap(asyncio.Event(), workflow_id, name, identity)

    async def get_identity_queue(self, workflow_id: str, name: str, identity: str | None) -> IdentityQueue:
        await self._check_resource(workflow_id, name, identity)
        return self._wrap(IdentityQueue(), workflow_id, name, identity)

    async def _register_alias(self, alias: str, workflow_id: str):
        if alias not in self._alias_dictionary:
            self._alias_dictionary[alias] = workflow_id
        else:
            raise DuplicateAliasException(f'Alias "{alias}" already exists')

    async def _deregister_alias(self, alias: str):
        if alias in self._alias_dictionary:
            del self._alias_dictionary[alias]

    def get_workflow_metrics(self):
        """Return metrics for active workflows"""
        metrics = []
        for wtype, wid, args, kwargs, delete_on_finish, start_time in self._workflow_data:
            metrics.append({
                "workflow_id": wid,
                "workflow_type": wtype,
                "start_time": start_time
            })
        return metrics

    # Should not be used for background tasks
    async def get_workflow_result(self, workflow_id: str, delete: bool = False):
        print(f"[DEBUG] get_workflow_result called for workflow_id={workflow_id}, delete={delete}")
        if workflow_id not in self._futures:
            print("[DEBUG] No future found, returning None.")
            return None

        future = self._futures[workflow_id]
        try:
            # Await future to get the workflow result
            result = await future
            print(f"[DEBUG] Future awaited, got result={result}")

            # Return suspended for suspended workflows
            if result == SUSPENDED:
                print("[DEBUG] Workflow was suspended, returning SUSPENDED.")
                return SUSPENDED

            if isinstance(result, dict) and "type" in result:  # Check if it is exception
                exception_obj = deserialize_exception(result)
                raise exception_obj
            return result

        # future raises cancelled error
        except asyncio.CancelledError as c:
            print(f"[DEBUG] get_workflow_result saw CancelledError with c.args={c.args}")
            if c.args and c.args[0] == SUSPENDED:
                # workflow was suspended - return suspended
                return SUSPENDED
            raise

        finally:
            if delete:
                del self._futures[workflow_id]
                if workflow_id in self._results:
                    del self._results[workflow_id]
                print("[DEBUG] Result deleted after retrieval.")


def find_workflow_manager() -> WorkflowManager:
    if (manager := workflow_manager.get()) is not None:
        return manager
