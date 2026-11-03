import asyncio
from dataclasses import dataclass
import pytest
from typing import TypeVar

from quest.historian import Historian
from quest.serializer import MasterSerializer, TypeSerializer
from quest.wrappers import step

T = TypeVar('T')


# Define custom class
@dataclass
class Stuff:
    name: str
    number: int

    def make_statement(self):
        return (self.name + ' ') * self.number


# TypeSerializer for Stuff
class StuffSerializer(TypeSerializer[Stuff]):
    async def serialize(self, obj: Stuff) -> tuple[tuple, dict]:
        return (obj.name, obj.number), {}

    async def deserialize(self, *args, **kwargs) -> Stuff:
        return Stuff(*args, **kwargs)


# Create MasterSerializer with StuffSerializer
type_serializers = {Stuff: StuffSerializer()}
serializer = MasterSerializer(type_serializers)


@step
async def create_stuff(name: str, number: int) -> Stuff:
    return Stuff(name, number)


pause_event = asyncio.Event()


async def workflow():
    stuff = await create_stuff('hello', 2)
    assert stuff.make_statement() == 'hello hello '
    await pause_event.wait()
    return stuff.make_statement()


@pytest.mark.asyncio
async def test_master_serializer():
    history = []
    # Create historian with custom serializer
    historian = Historian('test_workflow', workflow, history, serializer=serializer)

    workflow_task = historian.run()

    await asyncio.sleep(0.1)

    await historian.suspend()
    pause_event.set()
    workflow_task = historian.run()

    result = await workflow_task

    assert result == 'hello hello '
