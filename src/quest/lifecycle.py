from typing import Callable, Protocol, TypeVar, Generic

from src.quest.historian import Historian, History, UniqueEvents

WT = TypeVar('WT')


class WorkflowFactory(Protocol[WT]):
    def create_new_workflow(self) -> WT: ...

    def load_workflow(self, workflow_id: str) -> WT: ...

    def save_workflow(self, workflow_id: str, workflow_function: WT): ...


class HistoryFactory(Protocol):
    def create_history(self, workflow_id) -> History: ...


class UniqueIdsFactory(Protocol):
    def create_unique_ids(self, workflow_id) -> UniqueEvents: ...


class WorkflowLifecycleManager:
    def __init__(self,
                 workflow_factories: dict[str, WorkflowFactory],
                 history_factory: HistoryFactory,
                 unique_ids_factory: UniqueIdsFactory
                 ):
        self._workflow_factories = workflow_factories
        self._history_factory = history_factory
        self._unique_ids_factory = unique_ids_factory

        self._historians = {}

    async def run_workflow(self, workflow_type: str, workflow_id: str, *args, **kwargs):
        self._historians[workflow_id] = (historian := Historian(
            workflow_id,
            self._workflow_factories[workflow_type].create_new_workflow(),
            self._history_factory.create_history(workflow_id),
            self._unique_ids_factory.create_unique_ids(workflow_id)
        ))

        return await historian.run(*args, **kwargs)
