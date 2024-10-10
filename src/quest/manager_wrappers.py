from .manager import find_workflow_manager
from .historian import find_historian


class Alias:
    def __init__(self, alias, manager, workflow_id):
        self._alias = alias
        self._manager = manager
        self._workflow_id = workflow_id

    def __aenter__(self):
        self._manager.register(self._alias, self._workflow_id)

    def __aexit__(self, exc_type, exc_val, exc_tb):
        self._manager.deregister(self._alias)


def alias(alias: str) -> Alias:
    # TODO: TESTING
    manager = find_workflow_manager()
    manager_test = manager['workflow_manager']
    workflow_id = find_historian().workflow_id
    return Alias(alias, manager, workflow_id)
