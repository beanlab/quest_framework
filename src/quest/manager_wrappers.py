from manager import find_workflow_manager
from historian import find_historian

class Alias:
    def __init__(self, alias):
        self._alias = alias
        self._manager = find_workflow_manager()
        self._workflow_id = find_historian().workflow_id

    def __aenter__(self):
        self._manager.register(self._alias, self._workflow_id)

    def __aexit__(self, exc_type, exc_val, exc_tb):
        self._manager.deregister(self._alias)