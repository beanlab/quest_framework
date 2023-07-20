from typing import Protocol, TypeVar

from src.quest.historian import Historian, History, UniqueEvents

WT = TypeVar('WT')


class WorkflowFactory(Protocol[WT]):
    def create_new_workflow(self) -> WT: ...

    def load_workflow(self, workflow_id: str) -> WT: ...

    def save_workflow(self, workflow_id: str, workflow_function: WT): ...


class HistoryFactory(Protocol):
    def create_history(self, workflow_id) -> History: ...

    def load_history(self, workflow_id) -> History: ...

    def save_history(self, workflow_id, history: History): ...


class UniqueIdsFactory(Protocol):
    def create_unique_events(self, workflow_id) -> UniqueEvents: ...

    def load_unique_events(self, workflow_id) -> UniqueEvents: ...

    def save_unique_events(self, workflow_id, unique_events: UniqueEvents): ...


class WorkflowLifecycleManager:
    def __init__(self,
                 workflow_factories: dict[str, WorkflowFactory],
                 history_factory: HistoryFactory,
                 unique_ids_factory: UniqueIdsFactory
                 ):
        self._workflow_factories = workflow_factories
        self._history_factory = history_factory
        self._unique_ids_factory = unique_ids_factory

        self._historians: dict[str, Historian] = {}

    async def run_workflow(self, workflow_type: str, workflow_id: str, *args, **kwargs):
        self._historians[workflow_id] = (historian := Historian(
            workflow_id,
            self._workflow_factories[workflow_type].create_new_workflow(),
            self._history_factory.create_history(workflow_id),
            self._unique_ids_factory.create_unique_events(workflow_id)
        ))

        result = await historian.run(*args, **kwargs)

        self._historians.pop(workflow_id)

        return result

    def has_workflow(self, workflow_id):
        return workflow_id in self._historians

    def suspend_workflow(self, workflow_id):
        return self._historians[workflow_id].suspend()

    async def signal_workflow(self, workflow_id, resource_name, identity, action, *args, **kwargs):
        return await self._historians[workflow_id] \
            .record_external_event(resource_name, identity, action, *args, **kwargs)


