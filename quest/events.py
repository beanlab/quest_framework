import json
import os
from typing import Protocol, Union, TypedDict


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


class Event(TypedDict):
    timestamp: str
    payload: Serializable


class EventManager(Protocol):
    def __getitem__(self, key: str) -> Event: ...

    def __setitem__(self, key: str, value: Event): ...

    def __contains__(self, key: str) -> bool: ...

    def counter(self, event_name) -> UniqueEvent: ...


class InMemoryEventManager(EventManager):
    def __init__(self, initial_state: dict[str, Event] = None):
        self._state = initial_state or {}
        self._counters = {}

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


class LocalEventManager(EventManager):
    def __init__(self, filename):
        self._filename = filename
        if os.path.exists(filename):
            with open(filename) as file:
                tmp = json.load(file)
                self._state = tmp['events']
                self._counters = {k: UniqueEvent(**v) for k, v in tmp['counters'].items()}

        else:
            self._state = {}
            self._counters = {}

    def __getitem__(self, key: str) -> Event:
        return self._state[key]

    def __setitem__(self, key: str, value: Event):
        self._state[key] = value
        self._save()

    def __contains__(self, key: str) -> bool:
        return key in self._state

    def counter(self, event_name) -> UniqueEvent:
        if event_name not in self._counters:
            self._counters[event_name] = UniqueEvent(event_name)
        return self._counters[event_name]

    def _save(self):
        with open(self._filename, 'w') as file:
            json.dump({
                "events": self._state,
                "counters": {k: v.to_json() for k, v in self._counters.items()}
            }, file)
