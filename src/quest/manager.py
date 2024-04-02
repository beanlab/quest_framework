import asyncio
from typing import Protocol, Callable

from .historian import Historian
from .history import History
from .persistence import BlobStorage


class HistoryFactory(Protocol):
    def __call__(self, workflow_id: str) -> History: ...


class WorkflowFactory(Protocol):
    def __call__(self, workflow_type: str) -> Callable: ...


class WorkflowManager:
    """
    Runs workflow tasks
    It remembers which tasks are still active and resumes them on replay
    """

    def __init__(self, namespace: str, storage: BlobStorage, create_history: HistoryFactory,
                 create_workflow: WorkflowFactory):
        self._namespace = namespace
        self._storage = storage
        self._create_history = create_history
        self._create_workflow = create_workflow
        self._workflow_data = {}
        self._workflows: dict[str, Historian] = {}
        self._workflow_tasks: dict[str, asyncio.Task] = {}

    async def __aenter__(self):
        """Load the workflows and get them running again"""
        print("manager __aenter__ begun")
        if self._storage.has_blob(self._namespace):
            self._workflow_data = self._storage.read_blob(self._namespace)

        for wtype, wid, args, kwargs, background in self._workflow_data.values():
            self._start_workflow(wtype, wid, args, kwargs, delete_on_finish=background)

        print("manager __aenter__ exiting")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Save whatever state is necessary before exiting"""
        print("manager __aexit__ begun")
        self._storage.write_blob(self._namespace, self._workflow_data)
        for wid, historian in self._workflows.items():
            await historian.suspend()
        print("manager __aexit__ exiting")

    def _get_workflow(self, workflow_id: str):
        return self._workflows[workflow_id]  # TODO check for key, throw error

    def _remove_workflow(self, workflow_id: str, context):
        self._workflows.pop(workflow_id)
        self._workflow_tasks.pop(workflow_id)
        del self._workflow_data[workflow_id]

    def _start_workflow(self,
                        workflow_type: str, workflow_id: str, workflow_args, workflow_kwargs,
                        delete_on_finish=False):
        workflow_function = self._create_workflow(workflow_type)

        history = self._create_history(workflow_id)
        historian: Historian = Historian(workflow_id, workflow_function, history)
        self._workflows[workflow_id] = historian

        self._workflow_tasks[workflow_id] = (task := historian.run(*workflow_args, **workflow_kwargs))

        if delete_on_finish:
            task.add_done_callback(lambda t: self._remove_workflow(workflow_id, t))

    def start_workflow(self, workflow_type: str, workflow_id: str, 
                       delete_on_finish: bool,*workflow_args, **workflow_kwargs) -> asyncio.Task:
        
        """Start the workflow.
            Returns the asyncio.Task associated with the workflow.
            This allows the workflow to be awaited and the result retrieved."""
        if workflow_id not in self._workflow_data.keys():
            self._workflow_data[workflow_id] = (workflow_type, workflow_id, workflow_args, workflow_kwargs, delete_on_finish)
            self._start_workflow(workflow_type, workflow_id, workflow_args, workflow_kwargs, delete_on_finish)
        
        return self._workflow_tasks[workflow_id]

    def has_workflow(self, workflow_id: str) -> bool:
        return workflow_id in self._workflows

    def get_workflow(self, workflow_id: str) -> asyncio.Task:
        return self._workflow_tasks[workflow_id]

    async def suspend_workflow(self, workflow_id: str):
        await self._get_workflow(workflow_id).suspend()

    async def get_resources(self, workflow_id: str, identity):
        return await self._get_workflow(workflow_id).get_resources(identity)

    async def send_event(self, workflow_id: str, name: str, identity, action, *args, **kwargs):
        workflow_task = self._workflow_tasks[workflow_id]

        result = False
        if not workflow_task.done():
            result = await self._get_workflow(workflow_id).record_external_event(name, identity, action, *args, **kwargs)
            
        return result
