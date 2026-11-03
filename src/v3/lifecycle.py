from typing import Protocol, TypeVar

from .historian import Historian
from .history import History

WT = TypeVar('WT')


class WorkflowFactory(Protocol[WT]):
    def create_new_workflow(self) -> WT: ...

    def load_workflow(self, workflow_id: str) -> WT: ...

    def save_workflow(self, workflow_id: str, workflow_function: WT): ...


class StatelessWorkflowFactory(WorkflowFactory[WT]):
    def __init__(self, workflow_fuction: WT):
        self._workflow_function = workflow_fuction

    def create_new_workflow(self) -> WT:
        return self._workflow_function

    def load_workflow(self, workflow_id: str) -> WT:
        return self._workflow_function

    def save_workflow(self, workflow_id: str, workflow_function: WT):
        pass


class HistoryFactory(Protocol):
    def __call__(self, workflow_id) -> History: ...


class WorkflowLifecycleManager:
    def __init__(self,
                 workflow_factory: WorkflowFactory,
                 history_factory: HistoryFactory
                 ):
        self._workflow_factory = workflow_factory
        self._history_factory = history_factory

        self._historians: dict[str, Historian] = {}

    async def run_workflow(self, workflow_id: str, *args, **kwargs):
        if workflow_id not in self._historians:
            self._historians[workflow_id] = Historian(
                workflow_id,
                self._workflow_factory.create_new_workflow(),
                self._history_factory(workflow_id)
            )

        return await self._historians[workflow_id].run(*args, **kwargs)

    def has_workflow(self, workflow_id):
        return workflow_id in self._historians

    async def suspend_workflow(self, workflow_id):
        return await self._historians[workflow_id].suspend()

    def get_resources(self, workflow_id, identity):
        return self._historians[workflow_id].get_resources(identity)

    async def signal_workflow(self, workflow_id, resource_name, identity, action, *args, **kwargs):
        return await self._historians[workflow_id] \
            .record_external_event(resource_name, identity, action, *args, **kwargs)
