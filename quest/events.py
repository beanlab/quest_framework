from typing import Protocol, Union, TypedDict, Optional, TypeVar, Generator


class ToJson(Protocol):
    def to_json(self) -> dict: ...


Serializable = Union[dict, ToJson]


class UniqueEvent:
    def __init__(self, name: str, counter=-1):
        self.name = name
        self.counter = counter

    def reset(self):
        self.counter = -1

    def __next__(self):
        self.counter += 1
        return f'{self.name}_{self.counter}'

    def to_json(self):
        return {
            "name": self.name,
            "counter": self.counter
        }


class EventException(TypedDict):
    name: str
    args: tuple
    kwargs: dict


class Event(TypedDict):
    timestamp: str
    payload: Optional[Serializable]
    exception: Optional[EventException]


ET = TypeVar('ET')


class EventManager(Protocol):
    def __getitem__(self, key: str) -> ET: ...

    def __setitem__(self, key: str, value: ET): ...

    def __delitem__(self, key): ...

    def __contains__(self, key: str) -> bool: ...

    def items(self) -> Generator: ...

    def all_items(self) -> dict: ...


class InMemoryEventManager(EventManager):
    def __init__(self,
                 workflow_id: str,
                 initial_state: dict[str, Event] = None,
                 initial_counters: dict[str, UniqueEvent] = None
                 ):
        self._workflow_id = workflow_id
        self._state = initial_state or {}
        self._counters = initial_counters or {}

    def __getitem__(self, key: str) -> Event:
        return self._state[key]

    def __setitem__(self, key: str, value: Event):
        self._state[key] = value

    def __delitem__(self, key):
        del self._state[key]

    def __contains__(self, key: str) -> bool:
        return key in self._state

    def items(self):
        yield from self._state.items()


if __name__ == '__main__':
    pass
