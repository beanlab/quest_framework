from .manager import find_workflow_manager
from .historian import find_historian


class Alias:
    def __init__(self, alias, manager, workflow_id):
        self._alias = alias
        self._manager = manager
        self._workflow_id = workflow_id

    async def __aenter__(self):
        await self._manager._register_alias(self._alias, self._workflow_id)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._manager._deregister_alias(self._alias)


def alias(alias: str) -> Alias:
    manager = find_workflow_manager()
    workflow_id = find_historian().workflow_id
    return Alias(alias, manager, workflow_id)
