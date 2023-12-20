import asyncio

from historian import Historian, History


class WorkflowManager:
    """
    Runs workflow tasks
    It remembers which tasks are still active and resumes them on replay
    """

    def __init__(self, open_workflows: History, history_factory, workflow_factory):
        self._history_factory = history_factory
        self._workflow_factory = workflow_factory
        self._workflow_data = open_workflows
        self._workflows: dict[str, Historian] = {}
        self._workflow_tasks: dict[str, asyncio.Task] = {}

    async def __aenter__(self):
        """Load the workflows and get them running again"""
        for wid, wname, args, kwargs in self._workflow_data:
            self.start_workflow(wid, wname, *args, **kwargs)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Save whatever state is necessary before exiting"""
        for wid, historian in self._workflows.items():
            await historian.suspend()

    def _get_workflow(self, workflow_id: str):
        return self._workflows[workflow_id]  # TODO check for key, throw error

    def start_workflow(self, workflow_id: str, workflow_name: str, *workflow_args, **workflow_kwargs):
        """Start the workflow"""
        self._workflow_data.append((workflow_id, workflow_name, workflow_args, workflow_kwargs))

        workflow_function = self._workflow_factory[workflow_name].create()

        history = self._history_factory.create_history(workflow_id)
        historian: Historian = Historian(workflow_id, workflow_function, history)
        self._workflows[workflow_id] = historian

        self._workflow_tasks[workflow_id] = (task := historian.run(*workflow_args, **workflow_kwargs))
        task.add_done_callback(lambda t: self._remove_workflow(workflow_id))

    def _remove_workflow(self, workflow_id: str):
        self._workflows.pop(workflow_id)
        self._workflow_tasks.pop(workflow_id)
        data = next(d for d in self._workflow_data if d[0] == workflow_id)
        self._workflow_data.remove(data)

    async def suspend_workflow(self, workflow_id: str):
        await self._get_workflow(workflow_id).suspend()

    async def get_resources(self, workflow_id: str, identity):
        return await self._get_workflow(workflow_id).get_resources(identity)

    async def send_event(self, workflow_id: str, name: str, identity, action, *args, **kwargs):
        return await self._get_workflow(workflow_id).record_external_event(name, identity, action, *args, **kwargs)
