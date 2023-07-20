from typing import TypeVar, Callable

from src.quest.lifecycle import WorkflowFactory

WT = TypeVar('WT')


class StatelessWorkflowFactory(WorkflowFactory[WT]):
    def __init__(self, create_workflow_function: Callable[[], WT]):
        self._create_workflow_function = create_workflow_function

    def create_new_workflow(self) -> WT:
        return self._create_workflow_function()

    def load_workflow(self, workflow_id: str) -> WT:
        return self._create_workflow_function()

    def save_workflow(self, workflow_id: str, workflow_function: WT):
        pass


