import logging
from typing import Any, Protocol, TypeVar
from .events import EventManager
from .workflow import Workflow, WorkflowFunction, WorkflowStatus

EM = TypeVar('EM', bound=EventManager)


class DuplicateWorkflowIDException(Exception):
    def __init__(self, workflow_id: str):
        super().__init__(f'Workflow id {workflow_id} already in use')


class WorkflowMetadataSerializer(Protocol):
    """
    Stores and retrieves workflow metadata.
    There should be a unique WorkflowMetadataSerializer for each WorkflowManager
    """

    def save(self, workflow_metadata: dict): ...

    def load(self) -> dict: ...


class EventSerializer(Protocol[EM]):
    def new_event_manager(self, workflow_id: str) -> EM: ...

    def save_events(self, key: str, event_manager: EM): ...

    def load_events(self, key: str) -> EM: ...


class WorkflowSerializer(Protocol):
    """
    The WorkflowSerializer handles the logic needed to save and load complex dependencies
        in the workflow objects.

    For example, if a workflow has stateful dependencies, such as an API client,
        then the WorkflowSerializer is responsible for saving the information necessary
        to recreate the client from data, as well as recreating the workflow from that data.

    The WorkflowManager will save/load all the special workflow data (status, step, etc.)
    """

    def serialize_workflow(self, workflow_id: str, workflow: WorkflowFunction):
        """
        Serializes the data necessary to rehydrate the workflow object.

        :param workflow_id: The ID of the workflow
        :param workflow: The workflow object to be saved.
        """

    def deserialize_workflow(self, workflow_id: str, workflow_manager) -> WorkflowFunction:
        """
        Recreate the workflow object that was associated with the workflow ID with a workflow_manager
        as a parameter.

        :param workflow_id: Unique string identifying the workflow to be recreated
        :param workflow_manager: The workflow manager object that is managing the workflow
        :return: The WorkflowFunction associated with the workflow ID
        """

    def create_new_instance(self, workflow_id: str, workflow_manager) -> WorkflowFunction:
        """
        Create a new instance of the workflow object that was associated with the workflow_id with
        a workflow manager as a parameter.

        :param workflow_id: Unique string identifying the workflow to be created
        :param workflow_manager: The workflow manager object that is managing the workflow
        :return: a new instance of the WorkflowFunction
        """


class WorkflowManager:
    RESUME_WORKFLOW = '__resume_workflow__'

    def __init__(
            self,
            metadata_serializer: WorkflowMetadataSerializer,
            event_serializer: EventSerializer[EM],
            workflow_serializers: dict[str, WorkflowSerializer],
    ):
        self.metadata_serializer = metadata_serializer
        self.event_serializer = event_serializer
        self.workflow_serializers = workflow_serializers

        self.workflows: dict[str, Workflow] = {}
        self.event_managers: dict[str, EventManager] = {}

    async def __aenter__(self):
        workflow_types = self._load_workflow_types()
        await self._load_and_resume_workflows(workflow_types)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.save_workflows()

    def _load_workflow_types(self) -> dict:
        return self.metadata_serializer.load()

    def _workflow_types(self) -> dict[str, str]:
        return {wid: workflow._workflow_type() for wid, workflow in self.workflows.items()}

    def _save_workflows(self):
        """
        Serialize the workflow event managers

        Returns a dict of the workflow IDs to workflow types
        """
        for wid, event_manager in self.event_managers.items():
            self.event_serializer.save_events(wid, event_manager)

        for wid, workflow in self.workflows.items():
            self.workflow_serializers[workflow._workflow_type()].serialize_workflow(wid, workflow._func)

    async def _load_and_resume_workflows(self, workflow_types: dict[str, str]):
        for wid, wtype in workflow_types.items():
            event_manager = self.event_serializer.load_events(wid)
            workflow_func = self.workflow_serializers[wtype].deserialize_workflow(wid, self)
            self.workflows[wid] = (workflow := Workflow(wid, workflow_func, event_manager))
            self.event_managers[wid] = event_manager
            await workflow._async_run()

    def _load_workflows(self, workflow_types: dict[str, str]):
        for wid, wtype in workflow_types.items():
            event_manager = self.event_serializer.load_events(wid)
            workflow_func = self.workflow_serializers[wtype].deserialize_workflow(wid, self)
            self.workflows[wid] = Workflow(wid, workflow_func, event_manager)
            self.event_managers[wid] = event_manager

    async def _resume_workflows(self):
        for workflow in self.workflows.values():
            await workflow._async_run()

    def save_workflows(self):
        self._save_workflows()
        self.metadata_serializer.save(self._workflow_types())

    def load_workflows(self):
        workflow_types = self._load_workflow_types()
        self._load_workflows(workflow_types)

    async def resume_workflows(self):
        await self._resume_workflows()

    def get_workflow(self, workflow_id):
        return self.workflows[workflow_id]

    async def signal_async_workflow(self, workflow_id: str, signal_name: str, payload: Any) -> WorkflowStatus:
        """Sends a promised signal to the indicated workflow asynchronously"""
        workflow = self.get_workflow(workflow_id)
        return await workflow.async_send_signal(signal_name, payload)

    async def exception_signal_async_workflow(self, workflow_id: str, signal_name: str, exception: Exception, *args, **kwargs) -> WorkflowStatus:
        """Sends a not promised signal to the indicated workflow asynchronously"""
        workflow = self.get_workflow(workflow_id)
        return await workflow.async_send_signal_exception(signal_name, exception, *args, **kwargs)

    async def start_async_workflow(self, workflow_id: str, workflow_type: str, *args, **kwargs) -> WorkflowStatus:
        if workflow_id in self.workflows:
            logging.error(f'Workflow ID {workflow_id} already in use')
            raise DuplicateWorkflowIDException(workflow_id)

        event_manager = self.event_serializer.new_event_manager(workflow_id)
        workflow = Workflow(
            workflow_id,
            self.workflow_serializers[workflow_type].create_new_instance(workflow_id, self),
            event_manager=event_manager
        )
        self.event_managers[workflow_id] = event_manager
        self.workflows[workflow_id] = workflow
        return await workflow.async_start(*args, **kwargs)

    def has_workflow(self, wid: str):
        return wid in self.workflows

    def get_current_workflow_status(self, wid: str):
        return self.workflows.get(wid).get_current_status()


if __name__ == '__main__':
    pass
