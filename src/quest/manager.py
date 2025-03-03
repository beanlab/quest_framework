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


class WorkflowNotFound(Exception):
    pass


class WorkflowCancelled(Exception):
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

        # TODO - add cancel api - get cancel from historian api
        signal.signal(signal.SIGINT, our_handler)

        if self._storage.has_blob(self._namespace):
            self._workflow_data = self._storage.read_blob(self._namespace)

        # Check storage to load stored workflow results from persistent storage
        if self._storage.has_blob(f'{self._namespace}_results'):
            self._results = self._storage.read_blob(f'{self._namespace}_results')

        # Rehydrate only non-canceled workflows
        for wtype, wid, args, kwargs, delete_on_finish, start_time in self._workflow_data:
            # Prevent rehydration of canceled workflows
            if wid in self._results and isinstance(self._results[wid], WorkflowCancelled):
                continue

            if wid not in self._workflows:
                # Create new future if it does not exist or got cancelled.
                if wid not in self._futures or self._futures[wid].cancelled():
                    self._futures[wid] = asyncio.Future()
                self._start_workflow(wtype, wid, args, kwargs, delete_on_finish=delete_on_finish)

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Save whatever state is necessary before exiting"""
        self._storage.write_blob(self._namespace, self._workflow_data)
        self._storage.write_blob(f'{self._namespace}_results', self._results)

        for wid, historian in self._workflows.items():
            await historian.suspend()

        # All unfinished futures raise workflowCancelled when going out of scope
        for wid, future in self._futures.items():
            if not future.done():
                future.set_exception(
                    WorkflowCancelled(
                        f"WorkflowManager exited before {wid} completed.")
                )

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
        self._workflow_tasks.pop(workflow_id, None)
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
        Cancel and remove a workflow, ensuring it is not rehydrated in the future.
        """
        if workflow_id not in self._workflows and workflow_id not in self._futures:
            raise WorkflowNotFound(f"Workflow '{workflow_id}' not found.")

        # If the workflow is actively running, cancel its task
        if workflow_id in self._workflow_tasks:
            task = self._workflow_tasks[workflow_id]
            if not task.done():
                task.cancel()

        # Cancel and remove future if it exists
        if workflow_id in self._futures:
            future = self._futures[workflow_id]
            if not future.done():
                future.cancel()
            del self._futures[workflow_id]

        # Remove workflow from persistent storage list
        self._workflow_data = [d for d in self._workflow_data if d[1] != workflow_id]

        if workflow_id in self._workflows:
            del self._workflows[workflow_id]

        if workflow_id in self._workflow_tasks:
            del self._workflow_tasks[workflow_id]

        if workflow_id in self._results:
            del self._results[workflow_id]

    def _store_result(self, workflow_id: str, task: asyncio.Task, delete_on_finish: bool):
        """Store the result or exception of a completed workflow"""
        future = self._futures[workflow_id]

        try:
            # Retrieve the workflow result if it completed successfully
            result = task.result()

            self._results[workflow_id] = result

            if not future.done():
                future.set_result(result)

        # task cancelled
        except asyncio.CancelledError:
            if not future.done():
                future.set_exception(asyncio.CancelledError())

        except Exception as e:
            if not future.done():
                future.set_exception(e)

        finally:
            # Remove workflow from _workflow_data to ensure get_workflow_metrics() is accurate
            self._workflow_data = [d for d in self._workflow_data if d[1] != workflow_id]

            # Completed workflow - remove the workflow from active workflows if delete_on_finish is True
            if delete_on_finish:
                self._remove_workflow(workflow_id)  # Cleanup

                if workflow_id in self._results:
                    del self._results[workflow_id]

            if workflow_id in self._workflows:
                del self._workflows[workflow_id]

            if workflow_id in self._workflow_tasks:
                del self._workflow_tasks[workflow_id]

            if workflow_id in self._futures:
                del self._futures[workflow_id]

    def start_workflow(self, workflow_type: str, workflow_id: str, *workflow_args, delete_on_finish: bool = True,
                       **workflow_kwargs):
        """Start the workflow, but do not restart previously canceled ones"""
        start_time = datetime.utcnow().isoformat()

        # Prevent restarting previously canceled workflow
        if workflow_id in self._results and isinstance(self._results[workflow_id], WorkflowCancelled):
            return

        self._workflow_data.append(
            (workflow_type, workflow_id, workflow_args, workflow_kwargs, delete_on_finish, start_time))
        self._start_workflow(workflow_type, workflow_id, workflow_args, workflow_kwargs,
                             delete_on_finish=delete_on_finish)

    def has_workflow(self, workflow_id: str) -> bool:
        workflow_id = self._alias_dictionary.get(workflow_id, workflow_id)
        return workflow_id in self._workflows

    async def get_resources(self, workflow_id: str, identity):
        return await self._get_workflow(workflow_id).get_resources(identity)

    def get_resource_stream(self, workflow_id: str, identity):
        return self._get_workflow(workflow_id).get_resource_stream(identity)

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
        """
        Retrieve the result of a workflow, either by:
        - Returning a previously stored result.
        - Awaiting an active workflow's future if it's still running.
        """
        if workflow_id not in self._futures and workflow_id not in self._results:
            raise WorkflowNotFound(f"Workflow '{workflow_id}' does not exist.")

        # If workflow completed successfully, return its stored result
        if workflow_id in self._results:
            result = self._results[workflow_id]
            if delete:
                del self._results[workflow_id]  # Cleanup after retrieval
            return result

        # If future exists, await it to get the result or exception
        future = self._futures[workflow_id]

        try:
            result = await future
            return result

        # Prevent awaiting a cancelled future when client calls get_workflow_result
        except asyncio.CancelledError:
            raise WorkflowCancelled(f"Workflow {workflow_id} was cancelled and has no result.")

        finally:
            # If delete is True, clean up workflow references after the retrieval
            if delete:
                if workflow_id in self._futures:
                    del self._futures[workflow_id]
                if workflow_id in self._results:
                    del self._results[workflow_id]
                if workflow_id in self._workflows:
                    del self._workflows[workflow_id]
                if workflow_id in self._workflow_tasks:
                    del self._workflow_tasks[workflow_id]


def find_workflow_manager() -> WorkflowManager:
    if (manager := workflow_manager.get()) is not None:
        return manager
