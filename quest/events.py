from typing import Protocol, Union, TypedDict, Optional


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


class EventManager(Protocol):
    def __getitem__(self, key: str) -> Event: ...

    def __setitem__(self, key: str, value: Event): ...

    def __contains__(self, key: str) -> bool: ...

    def counter(self, event_name) -> UniqueEvent: ...


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

    def __contains__(self, key: str) -> bool:
        return key in self._state

    def counter(self, event_name) -> UniqueEvent:
        if event_name not in self._counters:
            self._counters[event_name] = UniqueEvent(event_name)
        return self._counters[event_name]


if __name__ == '__main__':
    pass
