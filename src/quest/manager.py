import asyncio
from contextvars import ContextVar
from datetime import datetime
from typing import Protocol, Callable, TypeVar, Any, TypedDict

from .core.history import History
from .core.serializer import StepSerializer
from .core.suspension import is_suspension
from .core.utils import serialize_exception, deserialize_exception, get_current_timestamp
from .core.workflow import Workflow
from .logs import quest_logger
from .persistence import BlobStorage


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
        self._workflows: dict[str, Workflow] = {}
        self._alias_dictionary = {}
        self._serializer: StepSerializer = serializer
        self._results: dict[str, WorkflowResult] = {}

    async def __aenter__(self) -> 'WorkflowManager':
        """Load the workflows and get them running again"""

        if self._storage.has_blob(self._namespace):
            quest_logger.debug('Loading workflows from namespace %s', self._namespace)
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
            await self._start_workflow(wtype, wid, args, kwargs, delete_on_finish=delete_on_finish)

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Save whatever state is necessary before exiting"""
        # Suspend running workflows
        for wid, workflow in list(self._workflows.items()):
            quest_logger.debug('Calling __aexit__ on %s', wid)
            await workflow.__aexit__(exc_type, exc_val, exc_tb)

        self._storage.write_blob(self._namespace, self._workflow_data)
        self._storage.write_blob(f'{self._namespace}_results', self._results)

        self._workflows.clear()

    def __await__(self):
        return asyncio.gather(*self._workflows.values()).__await__()

    async def _start_workflow(self,
                              workflow_type: str, workflow_id: str, workflow_args, workflow_kwargs,
                              delete_on_finish: bool = True):
        quest_logger.debug('Starting workflow %s %s', workflow_type, workflow_id)
        self._workflow_data[workflow_id] = WorkflowData(
            workflow_type=workflow_type,
            workflow_args=workflow_args,
            workflow_kwargs=workflow_kwargs,
            delete_on_finish=delete_on_finish,
            start_time=get_current_timestamp().isoformat()
        )
        workflow_function = self._create_workflow(workflow_type)

        workflow_manager.set(self)

        history = self._create_history(workflow_id)
        workflow = Workflow(
            workflow_id, workflow_function,
            *workflow_args,
            serializer=self._serializer, storage=history,
            **workflow_kwargs
        )
        self._workflows[workflow_id] = await workflow.__aenter__()
        workflow.task.add_done_callback(lambda t: self._schedule_store_result(workflow_id, t, delete_on_finish))

    def _schedule_store_result(self, workflow_id: str, task: asyncio.Task, delete_on_finish: bool):
        try:
            asyncio.create_task(self._store_result(workflow_id, task, delete_on_finish),
                                name=f'{workflow_id}._store_result')
        except RuntimeError:
            pass

    async def _store_result(self, workflow_id: str, task: asyncio.Task, delete_on_finish: bool):
        """Store the result or exception of a completed workflow"""
        quest_logger.debug('Storing result for %s', task.get_name())
        # Should follow the same logic as step_handler.py:_run_step_with_recorded_result
        try:
            result = task.result()
            result = await self._serializer.serialize(result)
            quest_logger.debug('Workflow %s completed %s', workflow_id, result)

        except asyncio.CancelledError as cancel:
            if is_suspension(cancel):
                quest_logger.debug('Workflow %s was suspended', workflow_id)
                return
            result = serialize_exception(cancel)
            quest_logger.exception('Workflow %s failed %s', workflow_id, result)

        except BaseException as e:
            result = serialize_exception(e)
            quest_logger.exception('Workflow %s failed', workflow_id)

        if not delete_on_finish:
            wdata = self._workflow_data[workflow_id]
            self._results[workflow_id] = WorkflowResult(
                workflow_id=workflow_id,
                workflow_type=wdata['workflow_type'],
                start_time=wdata['start_time'],
                result=result
            )

        quest_logger.debug('Deleted workflow %s', workflow_id)
        self._workflows.pop(workflow_id, None)
        self._workflow_data.pop(workflow_id, None)

    async def start_workflow(self, workflow_type: str, workflow_id: str, *workflow_args, delete_on_finish: bool = True,
                             **workflow_kwargs):
        """Start the workflow, but do not restart previously canceled ones"""
        start_time = datetime.utcnow().isoformat()  # TODO fix deprecated utcnow()

        if workflow_id in self._workflows:
            raise DuplicateWorkflowException(f'Workflow "{workflow_id}" already exists')

        self._workflow_data[workflow_id] = WorkflowData(
            workflow_type=workflow_type,
            workflow_args=workflow_args,
            workflow_kwargs=workflow_kwargs,
            delete_on_finish=delete_on_finish,
            start_time=start_time
        )
        await self._start_workflow(workflow_type, workflow_id, workflow_args, workflow_kwargs,
                                   delete_on_finish=delete_on_finish)

    def has_workflow(self, workflow_id: str) -> bool:
        workflow_id = self._alias_dictionary.get(workflow_id, workflow_id)

        return workflow_id in self._workflows or workflow_id in self._results

    def _get_workflow(self, workflow_id: str):
        workflow_id = self._alias_dictionary.get(workflow_id, workflow_id)
        # TODO: Do a key check to see if workflow_id exists. If not return a WorkflowDoesNotExistException
        return self._workflows[workflow_id]

    async def delete_workflow(self, workflow_id: str):
        """
        Cancel and remove a workflow, ensuring it is not rehydrated in the future.
        """
        if workflow_id in self._results:
            del self._results[workflow_id]

        elif workflow_id in self._workflows:
            task = self._workflows[workflow_id].task
            if not task.done():
                task.cancel()
                # cancelling the task will cause _store_result to run, which cleans up
                # TODO - do we need to await the task to ensure it exits?
                # This is an async function, so we could...

        else:
            raise WorkflowNotFound(f"Workflow '{workflow_id}' not found.")

    def resource_stream(self, workflow_id: str, identity):
        if workflow_id in self._results:  # workflow is finished, so it doesn't have resources
            raise NotImplementedError('todo')  # TODO

        return self._get_workflow(workflow_id).resource_stream(identity)

    async def invoke_action(self, workflow_id: str, name: str, identity, action, *args, **kwargs):
        return await self._get_workflow(workflow_id).invoke_action(name, identity, action, *args, **kwargs)

    async def register_alias(self, alias: str, workflow_id: str):
        if alias not in self._alias_dictionary:
            self._alias_dictionary[alias] = workflow_id
        else:
            raise DuplicateAliasException(f'Alias "{alias}" already exists')

    async def deregister_alias(self, alias: str):
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
        if workflow_id in self._workflows:
            # The workflow is still running
            return await self._workflows[workflow_id].task

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

            return await future

        else:
            raise WorkflowNotFound(f"Workflow '{workflow_id}' does not exist.")


def find_workflow_manager() -> WorkflowManager:
    if (manager := workflow_manager.get()) is not None:
        return manager
