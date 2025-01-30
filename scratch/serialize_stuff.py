import asyncio
from dataclasses import dataclass
from typing import Callable, Protocol, TypeVar, Any, TypedDict

from quest import step, Historian


T = TypeVar('T')

class StepSerializer(Protocol[T]):
    async def serialize(self, item: Any) -> T: ...

    async def deserialize(self, record: T) -> Any: ...


class NoopSeriliazer(StepSerializer):
    async def serialize(self, item: Any) -> Any:
        return item

    async def deserialize(self, record: Any) -> Any:
        return record



class SerializedRecord(TypedDict):
    _ms_type: str

class MasterSerializer(StepSerializer):
    def __init__(self, type_map: dict):
        self._type_map = type_map

    async def serialize(self, item: Any) -> SerializedRecord:
        the_type = type(item)
        if the_type in self._type_map:
            ...
        else:
            ...

    async def deserialize(self, record: SerializedRecord) -> Any:
        return record


@dataclass
class Stuff:
    name: str
    number: int

    def make_statement(self):
        return (self.name + ' ') * self.number


async def make_stuff(name) -> Stuff:
    return Stuff(name, 7)


@step
async def run_with_serializing(func: Callable, *args):
    stuff = await func(*args)
    return 'Stuff', stuff.wid, stuff.number


async def make_stuff_wrapper(name) -> Stuff:
    factory_name, *args = await run_with_serializing(make_stuff, name)
    Stuff = globals()[factory_name]
    return Stuff(*args)


pause = asyncio.Event()


async def workflow():
    stuff = await make_stuff('foo')
    await pause.wait()
    print(stuff.make_statement())


async def main():
    historian = Historian('demo', workflow, [])
    wtask = historian.run()
    await asyncio.sleep(0.1)
    await historian.suspend()

    pause.set()

    await historian.run()


def create_historian(wid: str, func: Callable, type_serializers: dict=None):
    history = []

    if type_serializers is None:
        step_serializer = NoopSeriliazer()
    else:
        step_serializer = MasterSerializer(type_serializers)

    return Historian(
        wid,
        func,
        history,
        step_serializer
    )



def elsewhere():
    type_sers = {
        Stuff: lambda stuff: (Stuff, (stuff.wid, stuff.number), {})
    }
    hist = create_historian('foo', workflow, type_sers)

if __name__ == '__main__':
    asyncio.run(main())
