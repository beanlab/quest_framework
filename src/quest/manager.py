import asyncio
import signal
from contextvars import ContextVar
from datetime import datetime
from functools import wraps
from typing import Protocol, Callable, TypeVar, Any, TypedDict

from .external import State, IdentityQueue, Queue, Event
from .historian import Historian, _Wrapper, SUSPENDED
from .history import History
from .persistence import BlobStorage
from .serializer import StepSerializer
from .utils import quest_logger, serialize_exception, deserialize_exception


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


class DuplicateWorkflowException(Exception):
    ...


class WorkflowResult(TypedDict):
    workflow_id: str
    workflow_type: str
    start_time: str
    result: Any


class WorkflowData(TypedDict):
    workflow_type: str
    workflow_args: list | tuple
    workflow_kwargs: dict
    delete_on_finish: bool
    start_time: str


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
        self._workflow_data: dict[str, WorkflowData] = {}  # Tracks all workflows
        self._workflows: dict[str, Historian] = {}
        self._workflow_tasks: dict[str, asyncio.Task] = {}
        self._alias_dictionary = {}
        self._serializer: StepSerializer = serializer
        self._results: dict[str, WorkflowResult] = {}

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

        # Rehydrate workflows
        for wid, data in self._workflow_data.items():
            wtype = data["workflow_type"]
            args = data["workflow_args"]
            kwargs = data["workflow_kwargs"]
            delete_on_finish = data["delete_on_finish"]
            self._start_workflow(wtype, wid, args, kwargs, delete_on_finish=delete_on_finish)

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Save whatever state is necessary before exiting"""
        for wid, historian in self._workflows.items():
            await historian.suspend()

        self._storage.write_blob(self._namespace, self._workflow_data)
        self._storage.write_blob(f'{self._namespace}_results', self._results)

        self._workflows.clear()
        self._workflow_tasks.clear()

    def _quest_signal_handler(self, sig, frame):
        quest_logger.debug(f'Caught KeyboardInterrupt: {sig}')
        for wid, historian in self._workflows.items():
            historian.signal_suspend()

    # TODO: Do a key check to see if workflow_id exists. If not return a WorkflowDoesNotExistException
    def _get_workflow(self, workflow_id: str):
        workflow_id = self._alias_dictionary.get(workflow_id, workflow_id)
        return self._workflows[workflow_id]

    def _start_workflow(self,
                        workflow_type: str, workflow_id: str, workflow_args, workflow_kwargs,
                        delete_on_finish: bool = True):

        workflow_function = self._create_workflow(workflow_type)

        workflow_manager.set(self)

        history = self._create_history(workflow_id)
        historian: Historian = Historian(workflow_id, workflow_function, history, serializer=self._serializer)
        self._workflows[workflow_id] = historian

        self._workflow_tasks[workflow_id] = (task := historian.run(*workflow_args, **workflow_kwargs))

        # run _store_result asynchronously in the background
        task.add_done_callback(lambda t: asyncio.create_task(self._store_result(workflow_id, t, delete_on_finish)))

    async def delete_workflow(self, workflow_id: str):
        """
        Cancel and remove a workflow, ensuring it is not rehydrated in the future.
        """
        if workflow_id in self._results:
            del self._results[workflow_id]

        elif workflow_id in self._workflows:
            task = self._workflow_tasks[workflow_id]
            if not task.done():
                task.cancel()
                # cancelling the task will cause _store_result to run, which cleans up

        else:
            raise WorkflowNotFound(f"Workflow '{workflow_id}' not found.")

    async def _store_result(self, workflow_id: str, task: asyncio.Task, delete_on_finish: bool):
        """Store the result or exception of a completed workflow"""
        if (
                (ex := task.exception()) is not None
                and isinstance(ex, asyncio.CancelledError)
                and ex.args and ex.args[0] == SUSPENDED
        ):
            return

        if not delete_on_finish:
            try:
                # Retrieve the workflow result if it completed successfully
                result = task.result()
                serialized_result = await self._serializer.serialize(result)
                result = serialized_result

            except BaseException as e:
                serialized_exception = serialize_exception(e)
                result = serialized_exception

            wdata = self._workflow_data[workflow_id]
            self._results[workflow_id] = WorkflowResult(
                workflow_id=workflow_id,
                workflow_type=wdata['workflow_type'],
                start_time=wdata['start_time'],
                result=result
            )

        # Completed workflow
        del self._workflows[workflow_id]
        del self._workflow_tasks[workflow_id]
        del self._workflow_data[workflow_id]

    def start_workflow(self, workflow_type: str, workflow_id: str, *workflow_args, delete_on_finish: bool = True,
                       **workflow_kwargs):
        """Start the workflow, but do not restart previously canceled ones"""
        start_time = datetime.utcnow().isoformat()

        if workflow_id in self._workflow_tasks:
            raise DuplicateWorkflowException(f'Workflow "{workflow_id}" already exists')

        self._workflow_data[workflow_id] = WorkflowData(
            workflow_type=workflow_type,
            workflow_args=workflow_args,
            workflow_kwargs=workflow_kwargs,
            delete_on_finish=delete_on_finish,
            start_time=start_time
        )
        self._start_workflow(workflow_type, workflow_id, workflow_args, workflow_kwargs,
                             delete_on_finish=delete_on_finish)

    def has_workflow(self, workflow_id: str) -> bool:
        workflow_id = self._alias_dictionary.get(workflow_id, workflow_id)

        return workflow_id in self._workflows or workflow_id in self._results

    async def get_resources(self, workflow_id: str, identity):
        if workflow_id in self._results:
            raise NotImplementedError('todo')  # TODO

        return await self._get_workflow(workflow_id).get_resources(identity)

    def get_resource_stream(self, workflow_id: str, identity):
        if workflow_id in self._results:
            raise NotImplementedError('todo')  # TODO

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
        for wid, data in self._results.items():
            metrics.append({
                "workflow_id": wid,
                "workflow_type": data["workflow_type"],
                "start_time": data["start_time"]
            })
        for wid, data in self._workflow_data.items():
            metrics.append({
                "workflow_id": wid,
                "workflow_type": data["workflow_type"],
                "start_time": data["start_time"]
            })
        return metrics

    async def get_workflow_result(self, workflow_id: str, delete: bool = False):
        if workflow_id in self._workflow_tasks:
            # The workflow is still running, so return the running
            return await self._workflow_tasks[workflow_id]

        elif workflow_id in self._results:
            # The workflow has finished and we saved the result
            serialized_result = self._results[workflow_id]['result']

            if isinstance(serialized_result, dict) and "type" in serialized_result:
                result = deserialize_exception(serialized_result)
            else:
                result = await self._serializer.deserialize(serialized_result)

            future = asyncio.Future()
            if isinstance(result, BaseException):
                future.set_exception(result)
            else:
                future.set_result(result)

            if delete:
                if workflow_id in self._results:
                    del self._results[workflow_id]
                if workflow_id in self._workflows:
                    del self._workflows[workflow_id]
                if workflow_id in self._workflow_tasks:
                    del self._workflow_tasks[workflow_id]

            return await future

        else:
            raise WorkflowNotFound(f"Workflow '{workflow_id}' does not exist.")


def find_workflow_manager() -> WorkflowManager:
    if (manager := workflow_manager.get()) is not None:
        return manager
