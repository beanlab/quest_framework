from typing import Any


class ActionHandler:
    def __init__(self):
        self._actions: dict[str | None, Any] = {None: {}}

    def add_action(self, identity: str, name: str, func):
        if identity not in self._actions:
            self._actions[identity] = {}
        self._actions[identity][name] = func

    def remove_action(self, identity: str, name: str):
        del self._actions[identity][name]
        if not self._actions[identity]:
            del self._actions[identity]

    def get_action(self, identity: str, name: str):
        return self._actions[identity][name]