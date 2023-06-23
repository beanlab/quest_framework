from typing import Protocol, Union, TypedDict, Optional, TypeVar, Generator


class ToJson(Protocol):
    def to_json(self) -> dict: ...


Serializable = Union[dict, ToJson]


class UniqueEvent:
    def __init__(self, name: str, counter=-1, replay=True):
        self.name = name
        self.counter = counter
        self.replay = replay

    def reset(self):
        if self.replay:
            self.counter = -1

    def __next__(self):
        self.counter += 1
        return f'{self.name}_{self.counter}'

    def to_json(self):
        return {
            "name": self.name,
            "counter": self.counter,
            "replay": self.replay
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


class EventManager(Protocol[ET]):
    def __getitem__(self, key: str) -> ET: ...

    def __setitem__(self, key: str, value: ET): ...

    def __delitem__(self, key): ...

    def __contains__(self, key: str) -> bool: ...

    def items(self) -> Generator: ...


class InMemoryEventManager(EventManager):
    def __init__(self,
                 workflow_id: str,
                 initial_state: dict[str, Event] = None
                 ):
        self.workflow_id = workflow_id
        self.state = initial_state or {}

    def __getitem__(self, key: str) -> Event:
        return self.state[key]

    def __setitem__(self, key: str, value: Event):
        self.state[key] = value

    def __delitem__(self, key):
        del self.state[key]

    def __contains__(self, key: str) -> bool:
        return key in self.state

    def items(self):
        yield from self.state.items()


if __name__ == '__main__':
    pass
