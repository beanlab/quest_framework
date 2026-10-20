from typing import Any


class StateHandler:
    def __init__(self):
        self._state: dict[str | None, Any] = {None: {}}

    def set_state(self, identity: str, name: str, value):
        if identity not in self._state:
            self._state[identity] = {}
        self._state[identity][name] = value

    def get_state(self, identity, name=None):
        # Return both public and identity values, with identity taking precedent
        if name is not None:
            return  self._state[identity][name]
        return {**self._state[None], **self._state[identity]}

    def delete_state(self, identity: str, name: str):
        del self._state[identity][name]
