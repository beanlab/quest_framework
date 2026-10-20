from quest.refactor.core.historian import Historian


class StateContext:
    def __init__(self, identity: str | None, name: str, value):
        self._historian: Historian = find_historian()
        self._identity = identity
        self._name = name
        self._value = value


    async def __aenter__(self):
        return await self._historian.add_state(self._identity, self._name, self._value)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self._historian.remove_state(self._identity, self._name)

